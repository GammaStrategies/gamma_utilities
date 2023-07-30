import logging
import time
import re
from ..cache.cache_utilities import pool_token_cache

from ..general import net_utilities

from ratelimit import RateLimitException, limits, sleep_and_retry


POOL_TOKEN_CACHE = pool_token_cache(time_limit=3600)


class geckoterminal_price_helper:
    """https://api.geckoterminal.com/docs/index.html"""

    _main_url = "https://api.geckoterminal.com/api/v2/"

    def __init__(self, retries: int = 1, request_timeout=10, sleepNretry: bool = False):
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
            "moonbeam": "moonbeam",
            "fantom": "fantom",
        }

        self.retries = retries
        self.request_timeout = request_timeout
        self.sleepNretry = sleepNretry

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
        try:
            # find a pool in gecko terminal that has the same token address
            if pools_data := self.get_pools_token_data(
                network=network, token_address=token_address, use_cache=True
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
                                if len(
                                    ohlcsv_data.get("data", {})
                                    .get("attributes", {})
                                    .get("ohlcv_list", None)
                                ):
                                    (
                                        _timestamp,
                                        _open,
                                        _high,
                                        _low,
                                        _close,
                                        _volume,
                                    ) = ohlcsv_data["data"]["attributes"]["ohlcv_list"][
                                        0
                                    ]
                                    return _close
                                else:
                                    logging.getLogger(__name__).debug(
                                        f" no ohlcv data was returned by geckoterminal -> {ohlcsv_data} for pool {pool_data['id']}"
                                    )
                                    return None

                    except Exception as e:
                        logging.getLogger(__name__).exception(
                            f"Error while getting pool address from {pool_data['id']}: {e}"
                        )
        except RateLimitException as e:
            logging.getLogger(__name__).debug(
                f"Rate limit fired while getting price historic for {token_address}: {e}"
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error while getting price historic for {token_address}: {e}"
            )
        return None

    def get_price_now(self, network: str, token_address: str) -> float | None:
        try:
            # find price searching for pools
            if price := self.get_price_from_pools(
                network=network, token_address=token_address
            ):
                logging.getLogger(__name__).debug(
                    f"Price found for {token_address} in pools: {price}"
                )
                return price
        except RateLimitException as e:
            logging.getLogger(__name__).debug(
                f"Rate limit fired while getting price now for {token_address}: {e}"
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error while getting price now for {token_address}: {e}"
            )
        return None

    # find data

    def get_price_from_pools(self, network: str, token_address: str) -> float | None:
        # find price searching for pools
        if pools_token_data := self.get_pools_token_data(
            network=network, token_address=token_address, use_cache=False
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
            network=network, token_address=token0_address, use_cache=True
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

    def get_pools_token_data(
        self, network: str, token_address: str, use_cache: bool = False
    ) -> dict:
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

        if (
            not use_cache
            or POOL_TOKEN_CACHE.get_data(key=network, subkey=token_address) is None
        ):
            url = f"{self.build_networks_url(network)}/tokens/{token_address}/pools"
            if data := request_data(
                url=url, timeout=self.request_timeout, sleepnretry=self.sleepNretry
            ):
                POOL_TOKEN_CACHE.set_data(key=network, subkey=token_address, data=data)

            else:
                return None

        # return result
        return POOL_TOKEN_CACHE.get_data(key=network, subkey=token_address)

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
        return request_data(
            url=url, timeout=self.request_timeout, sleepnretry=self.sleepNretry
        )

    def search(self, text_to_search: str) -> dict:
        """search address in gecko terminal realm

        Args:
            text_to_search (str):

        Returns:
            dict:  example:  {
                "data": {
                "id": "636a2746-3f7b-4703-b3ce-b8928181b092",
                "type": "search",
                "attributes": {
                    "networks": [],
                    "dexes": [],
                    "pools": [
                        {
                            "type": "pool",
                            "address": "0xcf5cebf7ddbda02e9691c2da19459a5f54fbf733",
                            "api_address": "0xcf5cebf7ddbda02e9691c2da19459a5f54fbf733",
                            "price_in_usd": "30.7113421481082",
                            "reserve_in_usd": "16.94336007789644224019234628811",
                            "from_volume_in_usd": "0.0",
                            "to_volume_in_usd": "0.0",
                            "price_percent_change": "0%",
                            "network": {
                                "name": "CELO",
                                "image_url": "https://assets.geckoterminal.com/0f0vvsahd5ycw2h3zgiv2pdppm7b",
                                "identifier": "celo"
                            },
                            "dex": {
                                "name": "Uniswap V3 (Celo)",
                                "identifier": "uniswap_v3_celo",
                                "image_url": "https://assets.geckoterminal.com/l61ksh3lzzp06j7q10ro2xd6bpdi"
                            },
                            "tokens": [
                                {
                                    "name": "Verra Mangrove Basket Token",
                                    "symbol": "VMBT",
                                    "image_url": "missing.png",
                                    "is_base_token": true
                                },
                                {
                                    "name": "Celo native asset",
                                    "symbol": "CELO",
                                    "image_url": "https://assets.coingecko.com/coins/images/11090/small/InjXBNx9_400x400.jpg?1674707499",
                                    "is_base_token": false
                                }
                            ]
                        }
                    ],
                    "pairs": []
                }
            }
        }
        """
        # https://app.geckoterminal.com/api/p1/search?query=
        url = f"{self._main_url}search?query={text_to_search}"
        return request_data(
            url=url, timeout=self.request_timeout, sleepnretry=self.sleepNretry
        )

    # HELPERs

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
                quote_token = token0
                base_token = token1
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
        # regex address
        regx_txt = "(?P<address>0x[a-zA-Z0-9]{40})"  # "(?P<address>0x.[^_]{39})"

        # check if this is base token
        for match in re.findall(
            regx_txt, pool_data["relationships"]["base_token"]["data"]["id"]
        ):
            if match.lower() == token_address.lower():
                return "base_token"

        # check if this is quote token
        for match in re.findall(
            regx_txt, pool_data["relationships"]["quote_token"]["data"]["id"]
        ):
            if match.lower() == token_address.lower():
                return "quote_token"

        # nothing found
        return None

        # if (
        #     pool_data["relationships"]["base_token"]["data"]["id"].split("_")[1].lower()
        #     == token_address.lower()
        # ):
        #     return "base_token"
        # elif (
        #     pool_data["relationships"]["quote_token"]["data"]["id"]
        #     .split("_")[1]
        #     .lower()
        #     == token_address.lower()
        # ):
        #     return "quote_token"
        # else:
        #     return None


def request_data(url, timeout, sleepnretry: bool = False):
    if sleepnretry:
        return request_data_sleepnretry(url=url, timeout=timeout)
    else:
        return request_data_limited(url=url, timeout=timeout)


@limits(calls=1, period=3, raise_on_limit=False)
def request_data_limited(url, timeout):
    if data := net_utilities.get_request(url=url, timeout_secs=timeout):
        if "data" in data:
            return data
        else:
            # {'status': '429', 'title': 'Rate Limited', 'debug': 'free limit'}
            if "status" in data and data["status"] == "429":
                logging.getLogger(__name__).warning(
                    f" Rate Limited by geckoterminal. retrying in 1 sec..."
                )
            else:
                logging.getLogger(__name__).warning(
                    f"no data returned by geckoterminal {url} -> {data}. retrying in 1 sec..."
                )
            time.sleep(1)
    return None


@sleep_and_retry
@limits(calls=1, period=3, raise_on_limit=True)
def request_data_sleepnretry(url, timeout):
    if data := net_utilities.get_request(url=url, timeout_secs=timeout):
        if "data" in data:
            return data
        else:
            # {'status': '429', 'title': 'Rate Limited', 'debug': 'free limit'}
            if "status" in data and data["status"] == "429":
                logging.getLogger(__name__).warning(
                    f" Rate Limited by geckoterminal. retrying in 1 sec..."
                )
            else:
                logging.getLogger(__name__).warning(
                    f"no data returned by geckoterminal -> {data}. retrying in 1 sec..."
                )
            time.sleep(1)
    return None
