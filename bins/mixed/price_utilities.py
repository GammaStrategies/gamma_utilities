from datetime import datetime
import sys
import logging
import time

from ratelimit.exception import RateLimitException
from ..cache import cache_utilities
from ..apis import thegraph_utilities, coingecko_utilities
from ..apis.geckoterminal_helper import geckoterminal_price_helper

from ..configuration import (
    CONFIGURATION,
    DEX_POOLS_PRICE_PATHS,
    TOKEN_ADDRESS_CONVERSION,
    USDC_TOKEN_ADDRESSES,
)
from ..database.common.db_collections_common import database_global
from ..formulas.dex_formulas import sqrtPriceX96_to_price_float
from ..general import file_utilities
from ..general.enums import Chain, Protocol, databaseSource, text_to_chain
from ..w3.builders import build_protocol_pool


LOG_NAME = "price"


class price_scraper:
    token_addresses_not_thegraph = ["0xf4c8e32eadec4bfe97e0f595add0f4450a863a11"]

    def __init__(
        self,
        cache: bool = True,
        cache_filename: str = "",
        coingecko: bool = True,
        onchain: bool = True,
        geckoterminal: bool = True,
        geckoterminal_sleepNretry: bool = False,
        thegraph: bool = True,
        source_order: list[databaseSource] | None = None,
    ):
        cache_folderName = CONFIGURATION["cache"]["save_path"]

        # init cache
        self.cache = (
            cache_utilities.price_cache(cache_filename, cache_folderName)
            if cache
            else None
        )

        self.coingecko = coingecko
        self.geckoterminal = geckoterminal
        self.geckoterminal_sleepNretry = geckoterminal_sleepNretry
        self.thegraph = thegraph
        self.onchain = onchain

        self.source_order = source_order

        # create price helpers
        self.init_apis(cache, cache_folderName)

    ## CONFIG ##
    def init_apis(self, cache: bool, cache_savePath: str):
        self.thegraph_connectors = {
            "uniswapv3": thegraph_utilities.uniswapv3_scraper(
                cache=cache, cache_savePath=cache_savePath
            ),
            "quickswapv3": thegraph_utilities.quickswap_scraper(
                cache=cache, cache_savePath=cache_savePath
            ),
            "zyberswapv3": thegraph_utilities.zyberswap_scraper(
                cache=cache, cache_savePath=cache_savePath
            ),
            "thena": thegraph_utilities.thena_scraper(
                cache=cache, cache_savePath=cache_savePath
            ),
        }

        # blocks->tiumestamp thegraph
        self.thegraph_block_connector = thegraph_utilities.blocks_scraper(
            cache=cache, cache_savePath=cache_savePath
        )
        # price coingecko
        self.coingecko_price_connector = coingecko_utilities.coingecko_price_helper(
            retries=1, request_timeout=5
        )

        self.geckoterminal_price_connector = geckoterminal_price_helper(
            retries=2, request_timeout=10, sleepNretry=self.geckoterminal_sleepNretry
        )

    def create_source_order(self) -> list[databaseSource]:
        """create the price scraper order"""
        result = []
        if self.cache:
            result.append(databaseSource.CACHE)
        if self.geckoterminal:
            result.append(databaseSource.GECKOTERMINAL)
        if self.onchain:
            result.append(databaseSource.ONCHAIN)
        if self.thegraph:
            result.append(databaseSource.THEGRAPH)
        if self.coingecko:
            result.append(databaseSource.COINGECKO)
        if len(result) == 0:
            raise Exception("No price sources selected")
        return result

    ## PUBLIC ##
    def get_price(
        self,
        network: str,
        token_id: str,
        block: int = 0,
        of: str = "USD",
        source_order: list | None = None,
    ) -> tuple[float, databaseSource]:
        """
        return: price_usd_token, source
        """

        # result var
        _price = None
        _source = None

        # make address lower case
        token_id = token_id.lower()

        # HARDCODED: change token address if manually specified in configuration
        try:
            chain = text_to_chain(network)
            if token_id in TOKEN_ADDRESS_CONVERSION.get(chain, {}):
                # change token address
                logging.getLogger(__name__).debug(
                    f"  {network} token address {token_id} has changed to {TOKEN_ADDRESS_CONVERSION[chain][token_id]} as set in configuration to gather price of"
                )
                token_id = TOKEN_ADDRESS_CONVERSION[network][token_id]
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error while trying to evaluate a change of token address while getting price {e}"
            )

        # follow the source order
        for source in source_order or self.source_order or self.create_source_order():
            if source == databaseSource.CACHE:
                _price, _source = self._get_price_from_cache(
                    network, token_id, block, of
                )
            elif (
                source == databaseSource.GECKOTERMINAL
                and network in self.geckoterminal_price_connector.networks
            ):
                _price, _source = self._get_price_from_geckoterminal(
                    network, token_id, block, of
                )
            elif (
                source == databaseSource.COINGECKO
                and network in self.coingecko_price_connector.networks
            ):
                _price, _source = self._get_price_from_coingecko(
                    network, token_id, block, of
                )
            elif source == databaseSource.ONCHAIN:
                _price, _source = self._get_price_from_onchain_data(
                    network, token_id, block
                )

            elif source == databaseSource.THEGRAPH:
                _price, _source = self._get_price_from_thegraph(
                    network, token_id, block
                )

            # if price found, exit for loop
            if _price not in [None, 0]:
                break

        # SAVE CACHE
        if _price not in [None, 0]:
            logging.getLogger(LOG_NAME).debug(
                f" {network}'s token {token_id} price at block {block} was found: {_price}"
            )

            if self.cache != None:
                # save price to cache and disk
                logging.getLogger(LOG_NAME).debug(
                    f" {network}'s token {token_id} price at block {block} was saved to cache"
                )

                self.cache.add_data(
                    chain_id=network,
                    address=token_id,
                    block=block,
                    key=of,
                    data=_price,
                    save2file=True,
                )
        else:
            # not found
            logging.getLogger(LOG_NAME).warning(
                f" {network}'s token {token_id} price at block {block} not found"
            )

        return _price, _source

    def _get_price_from_cache(
        self, network, token_id, block: int = 0, of: str = "USD"
    ) -> tuple[float, databaseSource]:
        _source = databaseSource.CACHE

        # try return price from cached values
        try:
            _price = self.cache.get_data(
                chain_id=network, address=token_id, block=block, key=of
            )
        except Exception:
            _price = None

        return _price, _source

    def _get_price_from_thegraph(
        self,
        network: str,
        token_id: str,
        block: int,
    ) -> tuple[float, databaseSource]:
        #
        _source = databaseSource.THEGRAPH
        _price = 0
        # get a list of thegraph_connectors
        thegraph_connectors = self._get_connector_candidates(network=network)

        for dex, connector in thegraph_connectors.items():
            logging.getLogger(LOG_NAME).debug(
                f" Trying to get {network}'s token {token_id} price at block {block} from {dex} subgraph"
            )

            try:
                _where_query = f""" id: "{token_id}" """

                if block != 0:
                    # get price at block
                    _block_query = f""" number: {block}"""
                    _data = connector.get_all_results(
                        network=network,
                        query_name="tokens",
                        where=_where_query,
                        block=_block_query,
                    )
                else:
                    # get current block price
                    _data = connector.get_all_results(
                        network=network, query_name="tokens", where=_where_query
                    )

                # process query
                try:
                    # get the first item in data list
                    _data = _data[0]

                    token_symbol = _data["symbol"]
                    # decide what to use to get to price ( value or volume )
                    if (
                        float(_data["totalValueLockedUSD"]) > 0
                        and float(_data["totalValueLocked"]) > 0
                    ):
                        # get unit usd price from value locked
                        _price = float(_data["totalValueLockedUSD"]) / float(
                            _data["totalValueLocked"]
                        )
                    elif (
                        "volume" in _data
                        and float(_data["volume"]) > 0
                        and "volumeUSD" in _data
                        and float(_data["volumeUSD"]) > 0
                    ):
                        # get unit usd price from volume
                        _price = float(_data["volumeUSD"]) / float(_data["volume"])
                    else:
                        # no way
                        _price = 0

                    # TODO: decide on certain circumstances (DAI USDC...)
                    # if _price == 0:
                    #     _price = self._price_special_case(address=token_id, network=network)
                except ZeroDivisionError:
                    # one or all prices are zero. Cant continue
                    # return zeros
                    logging.getLogger(LOG_NAME).warning(
                        f"one or all price variables of {network}'s token {token_symbol} (address {token_id}) at block {block} from {dex} subgraph are ZERO. Can't get price.  --> data: {_data}"
                    )

                    _price = 0
                except (KeyError, TypeError) as err:
                    # errors': [{'message': 'indexing_error'}]
                    if "errors" in _data:
                        for error in _data["errors"]:
                            logging.getLogger(LOG_NAME).error(
                                f"Unexpected error while getting price of {network}'s token address {token_id} at block {block} from {dex} subgraph   data:{_data}      .error: {error}"
                            )

                    else:
                        logging.getLogger(LOG_NAME).exception(
                            f"Unexpected error while getting price of {network}'s token address {token_id} at block {block} from {dex} subgraph   data:{_data}      .error: {sys.exc_info()[0]}"
                        )

                    _price = 0
                except IndexError as err:
                    logging.getLogger(LOG_NAME).error(
                        f"No data returned while getting price of {network}'s token address {token_id} at block {block} from {dex} subgraph   data:{_data}      .error: {sys.exc_info()[0]}"
                    )

                    _price = 0
                except Exception:
                    logging.getLogger(LOG_NAME).exception(
                        f"Unexpected error while getting price of {network}'s token address {token_id} at block {block} from {dex} subgraph   data:{_data}      .error: {sys.exc_info()[0]}"
                    )

                    _price = 0

            except Exception as e:
                logging.getLogger(LOG_NAME).debug(
                    f" Could not get {network}'s token {token_id} price at block {block} from thegraph. error-> {e}"
                )

        # return result
        return _price, _source

    def _get_price_from_coingecko(
        self, network: str, token_id: str, block: int, of: str
    ) -> tuple[float, databaseSource]:
        _source = databaseSource.COINGECKO
        _price = 0

        try:
            if of != "USD":
                raise NotImplementedError(
                    f" Cannot find {of} price method to be gathered from"
                )

            if block != 0:
                # convert block to timestamp
                if timestamp := self._convert_block_to_timestamp(
                    network=network, block=block
                ):
                    # get price at block
                    _price = self.coingecko_price_connector.get_price_historic(
                        network, token_id, timestamp
                    )
            else:
                # get current block price
                _price = self.coingecko_price_connector.get_price(
                    network, token_id, "usd"
                )
        except Exception as e:
            logging.getLogger(LOG_NAME).debug(
                f" Could not get {network}'s token {token_id} price at block {block} from coingecko. error-> {e}"
            )
        #
        if _price and isinstance(_price, dict):
            if token_id.lower() in _price:
                _price = _price[token_id.lower()]["usd"]
            else:
                _price = 0

        return _price, _source

    def _get_price_from_geckoterminal(
        self, network: str, token_id: str, block: int, of: str
    ) -> tuple[float, databaseSource]:
        _price = 0
        _source = databaseSource.GECKOTERMINAL

        try:
            if of != "USD":
                raise NotImplementedError(
                    f" Cannot find {of} price method to be gathered from"
                )

            if block != 0:
                # convert block to timestamp
                if timestamp := self._convert_block_to_timestamp(
                    network=network, block=block
                ):
                    try:
                        # get price at block
                        _price = self.geckoterminal_price_connector.get_price_historic(
                            network=network,
                            token_address=token_id,
                            before_timestamp=timestamp,
                        )
                    except RateLimitException as err:
                        logging.getLogger(__name__).debug(
                            f" geckoterminal auto rate limit fired"
                        )

                    # if no historical price was found but timestamp is 5 minute close to current time, get current price
                    if _price in [0, None] and (time.time() - timestamp) <= (5 * 60):
                        logging.getLogger(__name__).warning(
                            f" Price at block {block} not found using ohlcvs geckoterminal. Because timestamp date {datetime.fromtimestamp(timestamp)} is close to current time, try getting current price"
                        )
                        _price = self.geckoterminal_price_connector.get_price_now(
                            network=network, token_address=token_id
                        )

            else:
                # get current block price
                _price = self.geckoterminal_price_connector.get_price_now(
                    network=network, token_address=token_id
                )
        except Exception as e:
            logging.getLogger(LOG_NAME).debug(
                f" Could not get {network}'s token {token_id} price at block {block} from geckoterminal. error-> {e}"
            )
        #
        return _price, _source

    def _get_price_from_onchain_data(
        self, network: str, token_id: str, block: int
    ) -> tuple[float, databaseSource]:
        _price = 0
        _source = databaseSource.ONCHAIN
        try:
            # convert network string in chain enum
            chain = text_to_chain(network)
            # get price from onchain
            onchain_price_helper = usdc_price_scraper()
            _price = onchain_price_helper.get_price(
                chain=chain, token_address=token_id, block=block
            )
        except Exception as e:
            logging.getLogger(LOG_NAME).exception(
                f"Error while getting onchain price {e}"
            )

        return _price, _source

    # HELPERS
    def _convert_block_to_timestamp(self, network: str, block: int) -> int:
        # try database
        try:
            # create global database manager
            global_db = database_global(
                mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
            )
            return global_db.get_items_from_database(
                collection_name="blocks", find={"block": block}
            )[0]["timestamp"]
        except IndexError:
            logging.getLogger(__name__).error(
                f" {network}'s block {block} not found in database"
            )
        except Exception as e:
            logging.getLogger(LOG_NAME).exception(
                f"Error while getting block {block} timestamp from database. Error: {e}"
            )

        # try thegraph
        try:
            if network in self.thegraph_block_connector._URLS.keys():
                if block_data := self.thegraph_block_connector.get_all_results(
                    network=network,
                    query_name="blocks",
                    where=f""" number: "{block}" """,
                ):
                    block_data = block_data[0]

                    logging.getLogger(__name__).error(
                        f"     --> {network}'s block {block} found in subgraph"
                    )

                    return block_data["timestamp"]
            else:
                logging.getLogger(__name__).debug(
                    f" No {network} thegraph block connector found. Can't get block {block} timestamp from thegraph"
                )
        except Exception as e:
            logging.getLogger(LOG_NAME).exception(
                f"Error while getting block {block} timestamp from thegraph. Error: {e}"
            )
            return 0

        # try web3
        try:
            # create an erc20 dummy token
            from bins.w3.protocols.general import erc20

            dummy = erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
            if block_data := dummy._getBlockData(block=block):
                logging.getLogger(__name__).error(
                    f"     --> {network}'s block {block} found placing web3 calls"
                )
                return block_data["timestamp"]

        except Exception as e:
            logging.getLogger(LOG_NAME).exception(
                f"Error while getting block {block} timestamp from web3. Error: {e}"
            )
            return 0

    def _get_connector_candidates(self, network: str) -> dict:
        """get thegraph connectors with data for the specified network

        Args:
            network (str):

        Returns:
            dict: { <connector name> : <connector>}
        """
        return {
            name: connector
            for name, connector in self.thegraph_connectors.items()
            if network in connector.networks
        }


class usdc_price_scraper:
    def __init__(self):
        pass

    def get_price(
        self, chain: Chain, token_address: str, block: int | None = None
    ) -> float | None:
        try:
            # check if token is USDC
            if token_address.lower() in USDC_TOKEN_ADDRESSES.get(chain, []):
                return 1

            # try get path from file
            price = self._get_price_using_file_paths(
                chain=chain, token_address=token_address, block=block
            )
            if price is None:
                # try get price from var
                price = self._get_price_using_var_paths(
                    chain=chain, token_address=token_address, block=block
                )

            return price
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error while getting onchain price for token {token_address} on chain {chain}. Error: {e}"
            )
            return None

    def _get_price_using_var_paths(
        self, chain: Chain, token_address: str, block: int | None = None
    ) -> float | None:
        """get price of token_address in USDC using the paths defined in DEX_POOLS_PRICE_PATHS"""
        try:
            # check if token is USDC
            if token_address.lower() in USDC_TOKEN_ADDRESSES.get(chain, []):
                return 1

            # check if path to token is known
            if token_address in DEX_POOLS_PRICE_PATHS.get(chain, " "):
                price = 1
                # follow the path to get USDC price of token address
                for dex_pool_config, i in DEX_POOLS_PRICE_PATHS[chain][token_address]:
                    # select the right protocol
                    dex_pool = build_protocol_pool(
                        chain=chain,
                        protocol=dex_pool_config["protocol"],
                        pool_address=dex_pool_config["address"].lower(),
                        block=block,
                    )

                    # get price
                    token_in_base = sqrtPriceX96_to_price_float(
                        sqrtPriceX96=dex_pool.sqrtPriceX96,
                        token0_decimals=dex_pool.token0.decimals,
                        token1_decimals=dex_pool.token1.decimals,
                    )
                    if i == 0:
                        token_in_base = 1 / token_in_base

                    price *= token_in_base

                return price
            else:
                logging.getLogger(__name__).debug(
                    f" token {token_address} not found in DEX_POOLS_PRICE_PATHS. Cant get onchain price"
                )
                return None
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error while getting onchain price for token {token_address} on chain {chain}. Error: {e}"
            )
            return None

    def _get_price_using_file_paths(
        self, chain: Chain, token_address: str, block: int | None = None
    ) -> float | None:
        """Try get price using the precomputed json file with paths to tokens"""
        try:
            # check if token is USDC
            if token_address.lower() in USDC_TOKEN_ADDRESSES.get(chain, []):
                return 1

            # check if path to token is known
            # load json file
            if price_paths := file_utilities.load_json(
                filename="token_paths", folder_path="data"
            ):
                if token_address in price_paths.get(chain, " "):
                    price = 1
                    # follow the path to get USDC price of token address
                    for operation in price_paths[chain][token_address]:
                        # select the right protocol
                        dex_pool = build_protocol_pool(
                            chain=chain,
                            protocol=operation["protocol"],
                            pool_address=operation["address"],
                            block=block,
                        )

                        if sqrtPriceX96 := dex_pool.sqrtPriceX96:
                            # get price
                            token_in_base = sqrtPriceX96_to_price_float(
                                sqrtPriceX96=sqrtPriceX96,
                                token0_decimals=dex_pool.token0.decimals,
                                token1_decimals=dex_pool.token1.decimals,
                            )
                            # token0, token1 = whois_token(
                            #     token_addressA=operation["token_from"],
                            #     token_addressB=operation["token_to"],
                            # )
                            if (
                                dex_pool.token1.address.lower()
                                != operation["token_to"].lower()
                            ):
                                # reverse price
                                token_in_base = 1 / token_in_base

                            price *= token_in_base
                        else:
                            logging.getLogger(__name__).debug(
                                f" token {token_address} in pool {operation['address']} has sqrtPriceX96 to {sqrtPriceX96} at block {block}. Price is really zero."
                            )
                            return 0

                    return price
                else:
                    logging.getLogger(__name__).debug(
                        f" token {token_address} not found in price paths file. Cant get onchain price"
                    )
                    return None
            else:
                logging.getLogger(__name__).debug(
                    f" price paths file not found. Cant get onchain price"
                )
                return None
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error while getting onchain price for token {token_address} on chain {chain}. Error: {e}"
            )
            return None
