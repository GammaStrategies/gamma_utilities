import contextlib
import sys
import logging
from bins.cache import cache_utilities
from bins.apis import thegraph_utilities, coingecko_utilities
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global

LOG_NAME = "price"


class price_scraper:
    def __init__(self, cache: bool = True, cache_filename: str = ""):

        cache_folderName = CONFIGURATION["cache"]["save_path"]

        # init cache
        self.cache = (
            cache_utilities.price_cache(cache_filename, cache_folderName)
            if cache
            else None
        )

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

    ## PUBLIC ##
    def get_price(
        self, network: str, token_id: str, block: int = 0, of: str = "USD"
    ) -> float:
        """
        return: price_usd_token
        """

        # result var
        _price = None

        # make address lower case
        token_id = token_id.lower()

        # try return price from cached values
        try:
            _price = self.cache.get_data(
                chain_id=network, address=token_id, block=block, key=of
            )
        except Exception:
            _price = None

        if _price is None or _price == 0:
            # get a list of thegraph_connectors
            thegraph_connectors = self._get_connector_candidates(network=network)

            for dex, connector in thegraph_connectors.items():

                logging.getLogger(LOG_NAME).debug(
                    f" Trying to get {network}'s token {token_id} price at block {block} from {dex} subgraph"
                )
                with contextlib.suppress(Exception):
                    _price = self._get_price_from_thegraph(
                        thegraph_connector=connector,
                        dex=dex,
                        network=network,
                        token_id=token_id,
                        block=block,
                        of=of,
                    )

                    if _price and _price != 0:
                        # exit for loop
                        break

        # coingecko
        if (
            _price is None
            or _price == 0
            and network in self.coingecko_price_connector.networks
        ):
            # GET FROM COINGECKO
            logging.getLogger(LOG_NAME).debug(
                f" Trying to get {network}'s token {token_id} price at block {block} from coingecko"
            )

            try:
                _price = self._get_price_from_coingecko(network, token_id, block, of)
            except Exception:
                logging.getLogger(LOG_NAME).debug(
                    f" Could not get {network}'s token {token_id} price at block {block} from coingecko."
                )

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

        # return result
        return _price

    def _get_price_from_thegraph(
        self,
        thegraph_connector,
        dex: str,
        network: str,
        token_id: str,
        block: int,
        of: str,
    ) -> float:

        if of != "USD":
            raise NotImplementedError(
                f" Cannot find {of} price method to be gathered from"
            )

        _where_query = f""" id: "{token_id}" """

        if block != 0:
            # get price at block
            _block_query = f""" number: {block}"""
            _data = thegraph_connector.get_all_results(
                network=network,
                query_name="tokens",
                where=_where_query,
                block=_block_query,
            )
        else:
            # get current block price
            _data = thegraph_connector.get_all_results(
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

        # return result
        return _price

    def _get_price_from_coingecko(
        self, network: str, token_id: str, block: int, of: str
    ) -> float:

        _price = 0
        if of != "USD":
            raise NotImplementedError(
                f" Cannot find {of} price method to be gathered from"
            )

        if block != 0:
            # convert block to timestamp
            timestamp = self._convert_block_to_timestamp(network=network, block=block)
            if timestamp != 0:
                # get price at block
                _price = self.coingecko_price_connector.get_price_historic(
                    network, token_id, timestamp
                )
        else:
            # get current block price
            _price = self.coingecko_price_connector.get_price(network, token_id, "usd")

        #
        return _price

    # HELPERS
    def _convert_block_to_timestamp(self, network: str, block: int) -> int:

        # try database
        with contextlib.suppress(Exception):
            # create global database manager
            global_db = database_global(
                mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
            )
            return global_db.get_items_from_database(
                collection_name="block", find={"block": block}
            )[0]["timestamp"]
        # try thegraph
        try:
            block_data = self.thegraph_block_connector.get_all_results(
                network=network, query_name="blocks", where=f""" number: "{block}" """
            )[0]

            return block_data["timestamp"]
        except Exception:
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
