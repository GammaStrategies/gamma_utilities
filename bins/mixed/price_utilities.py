import sys
import logging
from bins.cache import cache_utilities
from bins.apis import thegraph_utilities, coingecko_utilities
from bins.configuration import CONFIGURATION

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
        # price thegraph
        self.thegraph_univ3_connector = thegraph_utilities.uniswapv3_scraper(
            cache=cache, cache_savePath=cache_savePath
        )
        # blocks->tiumestamp thegraph
        self.thegraph_block_connector = thegraph_utilities.blocks_scraper(
            cache=cache, cache_savePath=cache_savePath
        )
        # price coingecko
        self.coingecko_price_connector = coingecko_utilities.coingecko_price_helper()

    ## PUBLIC ##
    def get_price(
        self, network: str, token_id: str, block: int = 0, of: str = "USD"
    ) -> float:
        """
        return: price_usd_token
        """

        # make address lower case
        token_id = token_id.lower()

        # try return price from cached values
        try:
            _price = self.cache.get_data(
                chain_id=network, address=token_id, block=block, key=of
            )
        except:
            _price = None

        if _price == None or _price == 0:
            # GET FROM UNIV3 THEGRAPH
            logging.getLogger(LOG_NAME).debug(
                " Trying to get {}'s token {} price at block {} from uniswapv3 subgraph".format(
                    network, token_id, block
                )
            )
            try:
                _price = self._get_price_from_univ3_thegraph(
                    network, token_id, block, of
                )
            except:
                logging.getLogger(LOG_NAME).debug(
                    " Could not get {}'s token {} price at block {} from uniswapv3 subgraph.".format(
                        network, token_id, block
                    )
                )
        if _price == None or _price == 0:
            # GET FROM COINGECKO
            logging.getLogger(LOG_NAME).debug(
                " Trying to get {}'s token {} price at block {} from coingecko".format(
                    network, token_id, block
                )
            )
            try:
                _price = self._get_price_from_coingecko(network, token_id, block, of)
            except:
                logging.getLogger(LOG_NAME).debug(
                    " Could not get {}'s token {} price at block {} from coingecko.".format(
                        network, token_id, block
                    )
                )

        # SAVE CACHE
        if _price != None and _price != 0:
            logging.getLogger(LOG_NAME).debug(
                " {}'s token {} price at block {} was found: {}".format(
                    network, token_id, block, _price
                )
            )
            if self.cache != None:
                # save price to cache and disk
                logging.getLogger(LOG_NAME).debug(
                    " {}'s token {} price at block {} was saved to cache".format(
                        network, token_id, block
                    )
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
                " {}'s token {} price at block {} not found".format(
                    network, token_id, block
                )
            )

        # return result
        return _price

    # PRIV
    def _get_price_from_univ3_thegraph(
        self, network: str, token_id: str, block: int, of: str
    ) -> float:

        if of == "USD":

            _where_query = """ id: "{}" """.format(token_id)

            if block != 0:
                # get price at block
                _block_query = """ number: {}""".format(block)
                _data = self.thegraph_univ3_connector.get_all_results(
                    network=network,
                    query_name="tokens",
                    where=_where_query,
                    block=_block_query,
                )
            else:
                # get current block price
                _data = self.thegraph_univ3_connector.get_all_results(
                    network=network, query_name="tokens", where=_where_query
                )

        else:
            raise NotImplementedError(
                " Cannot find {} price method to be gathered from".format(of)
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
                "one or all price variables of {}'s token {} (address {}) at block {} from uniswap subgraph are ZERO. Can't get price.  --> data: {}".format(
                    network, token_symbol, token_id, block, _data
                )
            )
            _price = 0
        except (KeyError, TypeError) as err:
            # errors': [{'message': 'indexing_error'}]
            if "errors" in _data:
                for error in _data["errors"]:
                    logging.getLogger(LOG_NAME).error(
                        "Unexpected error while getting price of {}'s token address {} at block {} from uniswap subgraph   data:{}      .error: {}".format(
                            network, token_id, block, _data, error
                        )
                    )
            else:
                logging.getLogger(LOG_NAME).exception(
                    "Unexpected error while getting price of {}'s token address {} at block {} from uniswap subgraph   data:{}      .error: {}".format(
                        network, token_id, block, _data, sys.exc_info()[0]
                    )
                )
            _price = 0
        except:
            logging.getLogger(LOG_NAME).exception(
                "Unexpected error while getting price of {}'s token address {} at block {} from uniswap subgraph   data:{}      .error: {}".format(
                    network, token_id, block, _data, sys.exc_info()[0]
                )
            )
            _price = 0

        # return result
        return _price

    def _get_price_from_coingecko(
        self, network: str, token_id: str, block: int, of: str
    ) -> float:

        _price = 0
        if of == "USD":
            if block != 0:
                # convert block to timestamp
                timestamp = self._convert_block_to_timestamp(
                    network=network, block=block
                )
                if timestamp != 0:
                    # get price at block
                    _price = self.coingecko_price_connector.get_price_historic(
                        network, token_id, timestamp
                    )
            else:
                # get current block price
                _price = self.coingecko_price_connector.get_price(
                    network, token_id, "usd"
                )

        else:
            raise NotImplementedError(
                " Cannot find {} price method to be gathered from".format(of)
            )

        #
        return _price

    # HELPERS
    # TODO: use database to query blocks
    def _convert_block_to_timestamp(self, network: str, block: int) -> int:
        try:
            block_data = self.thegraph_block_connector.get_all_results(
                network=network,
                query_name="blocks",
                where=""" number: "{}" """.format(block),
            )[0]
            return block_data["timestamp"]
        except:
            return 0
