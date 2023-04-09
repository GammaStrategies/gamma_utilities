import sys
from pycoingecko import CoinGeckoAPI
import datetime as dt
import logging


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
