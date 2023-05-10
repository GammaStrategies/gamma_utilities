import sys
from pycoingecko import CoinGeckoAPI
import datetime as dt
import logging

from bins.general import net_utilities


class coingecko_price_helper:
    """Coingecko price cache"""

    def __init__(self, retries: int = 1, request_timeout=10):
        # todo: coded coingecko's network id conversion
        self.COINGECKO_netids = {
            "polygon": "polygon-pos",
            "ethereum": "ethereum",
            "optimism": "optimistic-ethereum",
            "arbitrum": "arbitrum-one",  # "arbitrum-nova" is targeted for gaming and donowhat ...
            "celo": "celo",
            "binance": "binance-smart-chain",
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
        self, network: str, contract_address: str, vs_currency: str = "usd"
    ) -> float:
        """Get current token price"""

        # get price from coingecko
        cg = CoinGeckoAPI(retries=self.retries)
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

        except Exception:
            logging.getLogger(__name__).exception(
                f" Exception at coingecko's price gathering of {contract_address}        error-> {sys.exc_info()[0]}"
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
        cg = CoinGeckoAPI(retries=self.retries)
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
            if "error" in err.args[0]:
                _data = {"prices": [[]], "error": err.args[0]["error"]}
            else:
                logging.getLogger(__name__).exception(
                    f"Unexpected error while getting price  of {contract_address} at {network} from coinGecko       .error: {sys.exc_info()[0]}"
                )

                _data = {"prices": [[]], "error": err.args[0]}
        except Exception:
            logging.getLogger(__name__).exception(
                f"Unexpected error while getting price  of {contract_address} at {network} from coinGecko       .error: {sys.exc_info()[0]}"
            )

            _data = {"prices": [[]], "error": "dontknow"}

        # check if result has actually a price in it
        try:
            if _data["prices"][0]:
                return _data["prices"][0][1]

            # price not found
            logging.getLogger(__name__).debug(
                f" Price not found for contract {contract_address} at {self.COINGECKO_netids[network]}  for timestamp {timestamp}"
            )

            # TODO: should we try to increase timeframe window??
            return 0
        except IndexError:
            # cannot find price return as zero
            logging.getLogger(__name__).debug(
                f" Price not found for contract {contract_address}  at {self.COINGECKO_netids[network]}  for timestamp {timestamp}"
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
        cg = CoinGeckoAPI(retries=self.retries)
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


class geckoterminal_price_helper:
    """https://api.geckoterminal.com/docs/index.html"""

    _main_url = "https://api.geckoterminal.com/api/v2/"

    def __init__(self, retries: int = 1, request_timeout=10):
        # todo: coded coingecko's network id conversion
        self.netids = {
            "polygon": "polygon_pos",
            "ethereum": "eth",
            "optimism": "optimism",
            "arbitrum": "arbitrum",  # "arbitrum-nova" is targeted for gaming and donowhat ...
            "celo": "celo",
            "binance": "bsc",
            "avalanche": "avax",
            "polygon_zkevm": "polygon-zkevm",
        }

        self.retries = retries
        self.request_timeout = request_timeout

        self.__RATE_LIMIT = net_utilities.rate_limit(rate_max_sec=0.5)  #  rate limiter

    @property
    def networks(self) -> list[str]:
        """available networks

        Returns:
            list: of networks
        """
        return list(self.netids.keys())

    def get_price_historic(
        self, network: str, token_address: str, before_timestamp: int
    ) -> float | None:
        # find a pool in gecko terminal that has the same token address
        if pools_data := self.get_pools_token_data(
            network=network, token_address=token_address
        ):
            for pool_data in pools_data["data"]:
                try:
                    pool_address = pool_data["id"].split("_")[1]
                    # check if token address is base or quote
                    if base_or_quote := self.get_base_or_quote(
                        token_address=token_address, pool_data=pool_data
                    ):
                        # get pool ohlv data
                        if ohlcsv_data := self.get_ohlcvs(
                            network=network,
                            pool_address=pool_address,
                            timeframe="minute",
                            aggregate=1,
                            before_timestamp=before_timestamp,
                            limit=1,
                            token=base_or_quote.replace("_token", ""),
                        ):
                            (
                                _timestamp,
                                _open,
                                _high,
                                _low,
                                _close,
                                _volume,
                            ) = ohlcsv_data["data"]["attributes"]["ohlcv_list"][0]
                            return _close

                except Exception as e:
                    logging.getLogger(__name__).error(
                        f"Error while getting pool address from {pool_data['id']}: {e}"
                    )
        return None

    def get_price_now(self, network: str, token_address: str) -> float:
        # find price searching for pools
        if price := self.get_price_from_pools(
            network=network, token_address=token_address
        ):
            logging.getLogger(__name__).debug(
                f"Price found for {token_address} in pools: {price}"
            )
            return price

    # find data

    def get_price_from_pools(self, network: str, token_address: str) -> float | None:
        # find price searching for pools
        if pools_token_data := self.get_pools_token_data(
            network=network, token_address=token_address
        ):
            # search for the token in the pools:  identify token as base or quote and retrieve its price usd from attributes
            try:
                for pool_data in pools_token_data["data"]:
                    if base_or_quote := self.get_base_or_quote(
                        token_address=token_address, pool_data=pool_data
                    ):
                        return float(
                            pool_data["attributes"][f"{base_or_quote}_price_usd"]
                        )

            except Exception as e:
                logging.getLogger(__name__).error(
                    f"Error while searching for token {token_address} in pools data: {e}"
                )

        return None

    def _find_pools(
        self, network: str, token0_address: str, token1_address: str
    ) -> list[dict]:
        result = []
        # find price searching for pools
        if pools_token_data := self.get_pools_token_data(
            network=network, token_address=token0_address
        ):
            # search for the token in the pools:  identify token as base or quote and retrieve its price usd from attributes
            try:
                for pool_data in pools_token_data["data"]:
                    if (
                        pool_data["relationships"]["base_token"]["data"]["id"]
                        .split("_")[1]
                        .lower()
                        == token0_address.lower()
                    ) and (
                        pool_data["relationships"]["quote"]["data"]["id"]
                        .split("_")[1]
                        .lower()
                        == token1_address.lower()
                    ):
                        # this is the pool we seek
                        result.append(pool_data)

            except Exception as e:
                logging.getLogger(__name__).error(
                    f"Error while searching for pools {token0_address} / {token1_address} in pools data: {e}"
                )

        return result

    # get data from geckoterminal's endpoints

    def get_pools_token_data(self, network: str, token_address: str) -> dict:
        """get the top 20 pools data for a token

        Args:
            network (str): network name
            token_address (str): token address

        Returns:
            dict: "data": [
                        {
                        "id": "string",
                        "type": "string",
                        "attributes": {
                            "name": "string",
                            "address": "string",
                            "token_price_usd": "string",
                            "base_token_price_usd": "string",
                            "quote_token_price_usd": "string",
                            "base_token_price_native_currency": "string",
                            "quote_token_price_native_currency": "string",
                            "pool_created_at": "string",
                            "reserve_in_usd": "string"
                        },
                        "relationships": {
                            "data": {
                            "id": "string",
                            "type": "string"
                            }
                        }
                        }
                    ]
        """

        url = f"{self.build_networks_url(network)}/tokens/{token_address}/pools"
        return self._request_data(url=url)

    def get_ohlcvs(
        self,
        network: str,
        pool_address: str,
        timeframe: str = "day",
        aggregate: int = 1,
        before_timestamp: int | None = None,
        limit: int = 100,
        currency: str = "usd",
        token: str = "base",
    ):
        """get ohlcv data for a pool"""

        # validate arguments
        if not currency in ["usd", "token"]:
            raise ValueError(f"currency must be 'usd' or 'token'")
        if not timeframe in ["day", "hour", "minute"]:
            raise ValueError(f"timeframe must be 'day', 'hour', 'minute'")
        if limit > 1000:
            raise ValueError(f"limit must be less or equal than 1000")
        if token not in ["base", "quote"]:
            raise ValueError(f"token must be 'base' or 'quote'")

        url = f"{self.build_networks_url(network)}/pools/{pool_address}/ohlcv/{timeframe}?{self.build_url_arguments(aggregate=aggregate, before_timestamp=before_timestamp, limit=limit, currency=currency, token=token)}"
        return self._request_data(url=url)

    # HELPERs
    def _request_data(self, url):
        self.__RATE_LIMIT.continue_when_safe()
        return net_utilities.get_request(url)

    def build_networks_url(self, network: str) -> str:
        return f"{self._main_url}networks/{self.netids[network]}"

    def build_url_arguments(self, **kargs) -> str:
        result = ""
        for k, v in kargs.items():
            # only add if not None
            if v:
                separator = "&" if result != "" else ""
                result += f"{separator}{k}={v}"
        return result

    def get_base_or_quote(self, token_address: str, pool_data: dict) -> str | None:
        """return if token_address is base or quote token in pool_data

        Args:
            token_address (str):
            pool_data (dict):   {
                                "data": [
                                    {
                                    "id": "string",
                                    "type": "string",
                                    "attributes": {
                                        "name": "string",
                                        "address": "string",
                                        "token_price_usd": "string",
                                        "base_token_price_usd": "string",
                                        "quote_token_price_usd": "string",
                                        "base_token_price_native_currency": "string",
                                        "quote_token_price_native_currency": "string",
                                        "pool_created_at": "string",
                                        "reserve_in_usd": "string"
                                    },
                                    "relationships": {
                                        "data": {
                                        "id": "string",
                                        "type": "string"
                                        }
                                    }
                                    }
                                ]
                                }


        Returns:
            str | None:  base_token, quote_token or None
        """
        if (
            pool_data["relationships"]["base_token"]["data"]["id"].split("_")[1].lower()
            == token_address.lower()
        ):
            return "base_token"
        elif (
            pool_data["relationships"]["quote"]["data"]["id"].split("_")[1].lower()
            == token_address.lower()
        ):
            return "quote_token"
        else:
            return None
