import json
from pycoingecko import CoinGeckoAPI
from pycoingecko.utils import func_args_preprocessing
import datetime as dt
import logging

import requests
from bins.cache.cache_utilities import CACHE_LOCK, file_backend

from bins.configuration import CONFIGURATION


class coingecko_cache(file_backend):
    def _init_cache(self):
        self._cache = self._load_cache_file() or {}
        self.max_size = 1000  # 1MB in kbytes

    def add_data(self, url: str, data, save2file=False) -> bool:
        """
        Args:
           data (dict): data to cache
        Returns:
           bool: success or fail
        """

        # avoid saving None values
        if data is None:
            logging.getLogger(__name__).debug(
                f" None value not saved to coingecko's cache"
            )
            return False

        # do parent add_data process ( control size...)
        super().add_data(data=data)

        with CACHE_LOCK:
            # NETWORK and BLOCK must be present when caching
            if not url in self._cache:
                self._cache[url] = data
            else:
                logging.getLogger(__name__).warning(
                    f" {url} already in cache. Updating data"
                )
                self._cache[url] = data

        if save2file:
            # save file to disk
            self._save_tofile()

        return True

    def get_data(self, url: str):
        """Retrieves data from cache

        Returns:
           dict: Can return None if not found
        """
        # use it for key in cache
        if result := self._cache.get(url, None):
            # add cached flag
            result["cached"] = True
            return result
        return None


class coingecko_apiMod(CoinGeckoAPI):
    def __init__(self, api_key: str = "", retries=5):
        # init custom persistent cache
        self.initialize_persistent_cache()
        # init parent
        super().__init__(api_key, retries)

    def __request(self, url):
        try:
            response = self.session.get(url, timeout=self.request_timeout)
        except requests.exceptions.RequestException:
            raise

        try:
            response.raise_for_status()
            content = json.loads(response.content.decode("utf-8"))
            return content
        except Exception as e:
            # check if json (with error message) is returned
            try:
                content = json.loads(response.content.decode("utf-8"))
                raise ValueError(content)
            # if no json
            except json.decoder.JSONDecodeError:
                pass

            raise

    def __api_url_params(self, api_url, params, api_url_has_params=False):
        # if using pro version of CoinGecko, inject key in every call
        if self.api_key:
            params["x_cg_pro_api_key"] = self.api_key

        if params:
            # if api_url contains already params and there is already a '?' avoid
            # adding second '?' (api_url += '&' if '?' in api_url else '?'); causes
            # issues with request parametes (usually for endpoints with required
            # arguments passed as parameters)
            api_url += "&" if api_url_has_params else "?"
            for key, value in params.items():
                if type(value) == bool:
                    value = str(value).lower()

                api_url += "{0}={1}&".format(key, value)
            api_url = api_url[:-1]
        return api_url

    @func_args_preprocessing
    def get_coin_market_chart_range_from_contract_address_by_id(
        self, id, contract_address, vs_currency, from_timestamp, to_timestamp, **kwargs
    ):
        """Get historical market data include price, market cap, and 24h volume within a range of timestamp (granularity auto) from a contract address"""

        api_url_base = "{0}coins/{1}/contract/{2}/market_chart/range?vs_currency={3}&from={4}&to={5}".format(
            self.api_base_url,
            id,
            contract_address,
            vs_currency,
            from_timestamp,
            to_timestamp,
        )
        api_url = self.__api_url_params(api_url_base, kwargs, api_url_has_params=True)

        # search in cache
        response = self.cache.get_data(api_url_base)

        if response == None:
            # get from coingecko
            try:
                response = self.__request(api_url)

            except requests.HTTPError as e:
                for err in e.args:
                    if "too many requests" in err.lower() or "429" in err:
                        # "Too Many Requests"
                        logging.getLogger(__name__).debug(
                            f" Too many requests made to coingecko."
                        )
                        # do not add to cache
                        return None
                    if "not found" in err.lower() or "404" in err:
                        # "Not Found"
                        logging.getLogger(__name__).debug(
                            f" Token not found at coingecko."
                        )
                        # add to cache
                        response = {"prices": [[]], "error": err}
            except ValueError as e:
                for err in e.args:
                    # try to react from the code
                    if error_message := err.get("error", None):
                        if error_message.lower() == "coin not found":
                            logging.getLogger(__name__).warning(
                                f" Token {contract_address} not found at coingecko. Error: {err}"
                            )
                            # add to cache
                            response = {"prices": [[]], "error": err}
                        else:
                            # "Your app has exceeded its concurrent requests capacity. If you have retries enabled, you can safely ignore this message
                            logging.getLogger(__name__).exception(
                                f" [1]Unknown coingecko ValueError:    -> {err}"
                            )
                            # do not add to cache
                            return None
                    if error_code := err.get("status", {}).get("error_code", None):
                        if error_code == 429:
                            logging.getLogger(__name__).debug(
                                f" Too many requests made to coingecko."
                            )
                            # do not add to cache
                            return None
                        elif error_code == 404:
                            logging.getLogger(__name__).debug(
                                f" Token not found at coingecko."
                            )
                            # add to cache
                            response = {"prices": [[]], "error": err}
                        else:
                            logging.getLogger(__name__).exception(
                                f" [2.1]Unknown coingecko error_code:    -> {err}"
                            )
                            # do not add to cache
                            return None
                    if error_str := err.get("error", None):
                        if error_str.lower() == "coin not found":
                            logging.getLogger(__name__).warning(
                                f" Token {contract_address} not found at coingecko. Error: {err}"
                            )
                            # add to cache
                            response = {"prices": [[]], "error": err}
                        else:
                            logging.getLogger(__name__).exception(
                                f" [2.2]Unknown coingecko error ValueError:    -> {err} -> contract {contract_address}"
                            )
                            # do not add to cache
                            return None
                    else:
                        logging.getLogger(__name__).exception(
                            f" [2]Unknown coingecko ValueError:  -> {err}  -> contract {contract_address}"
                        )
                        # do not add to cache
                        return None

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Exception at coingecko's get_coin_market_chart_range_from_contract_address_by_id. Error: {e}"
                )
                # do not add to cache
                return None

            # save response to cache
            self.cache.add_data(url=api_url_base, data=response, save2file=True)

        return response

    # control cache size
    def initialize_persistent_cache(self):
        self.cache = coingecko_cache(
            filename="coingecko_cache",
            folder_name=CONFIGURATION.get("cache", {}).get("save_path", None)
            or "data/cache",
        )


class coingecko_price_helper:
    """Coingecko price cache"""

    def __init__(self, retries: int = 1, request_timeout=10):
        # todo: coded coingecko's network id conversion
        # https://api.coingecko.com/api/v3/asset_platforms
        self.COINGECKO_netids = {
            # enums.Chain.ETHEREUM: "ethereum",
            # enums.Chain.OPTIMISM: "optimistic-ethereum",
            # ...
            "ethereum": "ethereum",
            "optimism": "optimistic-ethereum",
            "polygon": "polygon-pos",
            "polygon_zkevm": "polygon-zkevm",
            "arbitrum": "arbitrum-one",  # "arbitrum-nova" is targeted for gaming etc ...
            "celo": "celo",
            "binance": "binance-smart-chain",
            "moonbeam": "moonbeam",
            "base": "base",
            "avalanche": "avalanche",
            "fantom": "fantom",
            "linea": "linea",
            "mantle": "mantle",
            "rollux": "rollux",
            "opbnb": "opbnb",
        }

        self.retries = retries
        self.request_timeout = request_timeout

    @property
    def networks(self) -> list[str]:
        """available networks

        Returns:
            list: of networks
        """
        return list(self.COINGECKO_netids.keys())

    ## PUBLIC ##
    def get_price(
        self,
        network: str,
        contract_address: str,
        vs_currency: str = "usd",
    ) -> float:
        """Get current token price"""

        # get price from coingecko
        cg = coingecko_apiMod(
            api_key=CONFIGURATION.get("sources", {}).get("coingeko_api_key", ""),
            retries=self.retries,
        )
        # modify cgecko's default timeout
        cg.request_timeout = self.request_timeout

        try:
            # { "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {
            #     "usd": 20656,
            #     "eur": 20449,
            #     "eth": 12.115137,
            #     "btc": 1.000103
            #         }
            #  }
            return cg.get_token_price(
                id=self.COINGECKO_netids[network],
                vs_currencies=vs_currency,
                contract_addresses=contract_address,
            )

        except ValueError as e:
            logging.getLogger(__name__).error(
                f" ValueError at coingecko's price gathering of {contract_address}        error-> {e}"
            )

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Exception at coingecko's price gathering of {contract_address}        error-> {e}"
            )

        return 0

    def get_price_historic(
        self, network: str, contract_address: str, timestamp: int, vs_currency="usd"
    ) -> float:  # sourcery skip: remove-unnecessary-cast
        """historic prices

        Args:
           network (str): _description_
           contract_address (str): _description_
           timestamp (int): _description_
           vs_currency (str, optional): _description_. Defaults to "usd".

        Returns:
           float: price (zero when not found)
        """
        # check that timestamp is formated correctly
        if isinstance(timestamp, float):
            timestamp = int(timestamp)
        elif isinstance(timestamp, str):
            if "." in timestamp:
                # timestamp has a non accepted by coingeko format "3333333.0"
                timestamp = int(timestamp.split(".")[0])

        # get price from coingecko
        cg = coingecko_apiMod(
            api_key=CONFIGURATION.get("sources", {}).get("coingeko_api_key", ""),
            retries=self.retries,
        )
        # modify cgecko's default timeout
        cg.request_timeout = self.request_timeout
        # define a timeframe to query
        to_timestamp = int(
            dt.datetime.timestamp(
                dt.datetime.fromtimestamp(timestamp) + dt.timedelta(hours=(24 * 20))
            )
        )
        # query coinGecko
        try:
            _data = cg.get_coin_market_chart_range_from_contract_address_by_id(
                id=self.COINGECKO_netids[network],
                vs_currency=vs_currency,
                contract_address=contract_address,
                from_timestamp=timestamp,
                to_timestamp=to_timestamp,
            )
        except ValueError as err:
            if err.args:
                # try get error code: can be "error": "coin not found"
                if isinstance(err.args[0], dict):
                    code = err.args[0].get("status", {}).get("error_code", 0)
                    if not code:
                        code = err.args[0].get("error_code", 0)

                if code == 429:
                    logging.getLogger(__name__).warning(
                        f"Too many requests error while getting price  of {contract_address} at {network} from coinGecko       .error: {err}"
                    )
                    return 0
                elif code == 404:
                    logging.getLogger(__name__).warning(
                        f"Price not found for contract {contract_address} at {network}  for timestamp {timestamp}"
                    )
                    return 0
                elif code == 0 and "error" in err.args[0]:
                    logging.getLogger(__name__).warning(
                        f" {err.args[0]['error']} error while getting price  of {contract_address} at {network} from coinGecko."
                    )
                else:
                    logging.getLogger(__name__).exception(
                        f"Unexpected value error while getting price  of {contract_address} at {network} from coinGecko       .error: {err}"
                    )

                _data = {"prices": [[]], "error": err.args[0]}
        except Exception as err:
            logging.getLogger(__name__).exception(
                f"Unexpected error while getting price  of {contract_address} at {network} from coinGecko       .error: {err}"
            )

            _data = {"prices": [[]], "error": "dontknow"}

        # check if result has actually a price in it
        try:
            if _data["prices"][0]:
                return _data["prices"][0][1]

            # price not found
            logging.getLogger(__name__).debug(
                f" Price not found for contract {contract_address} at {network}  for timestamp {timestamp}"
            )

            # TODO: should we try to increase timeframe window??
            return 0
        except IndexError:
            # cannot find price return as zero
            logging.getLogger(__name__).debug(
                f" Price not found for contract {contract_address}  at {network}  for timestamp {timestamp}"
            )

            return 0

    def get_prices(
        self, network: str, contract_addresses: list, vs_currencies: list = None
    ) -> dict:
        """get multiple prices at once (current)

        Args:
           network (str): _description_
           contract_addresses (list): _description_
           vs_currencies (list, optional): _description_. Defaults to ["usd"].

        Returns:
           dict:   {"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {
                       "usd": 20656,
                       "eur": 20449,
                       "eth": 12.115137,
                       "btc": 1.000103
                       },
                    "0x222222...":
                   }
        """

        if vs_currencies is None:
            vs_currencies = ["usd"]
        result = {}
        # get price from coingecko
        cg = coingecko_apiMod(
            api_key=CONFIGURATION.get("sources", {}).get("coingeko_api_key", ""),
            retries=self.retries,
        )
        # modify cgecko's default timeout
        cg.request_timeout = self.request_timeout

        # split contract_addresses in batches so URI too long errors do not popup
        n = 50
        # using list comprehension
        for i in range(0, len(contract_addresses), n):
            # { "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {
            #     "usd": 20656,
            #     "eur": 20449,
            #     "eth": 12.115137,
            #     "btc": 1.000103
            #         }
            #  }
            result |= cg.get_token_price(
                id=self.COINGECKO_netids[network],
                vs_currencies=vs_currencies,
                contract_addresses=contract_addresses[i : i + n],
            )

        return result
