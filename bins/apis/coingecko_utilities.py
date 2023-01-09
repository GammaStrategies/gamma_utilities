
from pycoingecko import CoinGeckoAPI
import datetime as dt


class coingecko_price_helper:
    """Coingecko price cache"""

    def __init__(self):
        # todo: coded coingecko's network id conversion
        self.COINGECKO_netids = {
        "polygon": "polygon-pos",
        "ethereum": "ethereum",
        "optimism": "optimistic-ethereum",
        "arbitrum": "arbitrum-one",  # "arbitrum-nova" is targeted for gaming and donowhat ...
        "celo": "celo",
        }
  
   ## PUBLIC ##
    def get_price(self, network:str, contract_address:str, vs_currency:str="usd") -> float:
        """Get current token price"""

        # get price from coingecko
        cg = CoinGeckoAPI()

        try:
            _price = cg.get_token_price(
                id=self.COINGECKO_netids[network],
                vs_currencies=vs_currency,
                contract_addresses=contract_address,
            )
            # { "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {
            #     "usd": 20656,
            #     "eur": 20449,
            #     "eth": 12.115137,
            #     "btc": 1.000103
            #         }
            #  }
            return _price

        except:
            logging.getLogger(__name__).exception(
                " Exception at coingecko's price gathering of {}        error-> {}".format(
                    contract_address, sys.exc_info()[0]
                )
            )
  
        return 0

    def get_price_historic(self, network: str, contract_address: str, timestamp: int, vs_currency="usd")->float:
        """ historic prices

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
        cg = CoinGeckoAPI()
        # modify cgecko's default timeout
        cg.request_timeout = 30
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
            if err.args[0] == {"error": "Could not find coin with the given id"}:
                _data = {"prices": [list()], "error": "not found"}
            else:
                logging.getLogger(__name__).exception(
                "Unexpected error while getting price  of {} at {} from coinGecko       .error: {}".format(
                    contract_address, network, sys.exc_info()[0]))
                _data = {"prices": [list()], "error": "dontknow"}
        except:
            logging.getLogger(__name__).exception(
                "Unexpected error while getting price  of {} at {} from coinGecko       .error: {}".format(
                    contract_address, network, sys.exc_info()[0]
                )
            )
            _data = {"prices": [list()], "error": "dontknow"}

        # check if result has actually a price in it
        try:
            if len(_data["prices"][0]) > 0:    
                return _data["prices"][0][1]
            else:
                # price not found
                logging.getLogger(__name__).debug(
                    " Price not found for contract {} at {}  for timestamp {}".format(
                        contract_address, self.COINGECKO_netids[network], timestamp
                    ))
                # TODO: should we try to increase timeframe window??
                return 0
        except IndexError:
            # cannot find price return as zero
            logging.getLogger(__name__).debug(
                " Price not found for contract {}  at {}  for timestamp {}".format(
                    contract_address, self.COINGECKO_netids[network], timestamp
                ))
            return 0
     
    def get_prices(self, network:str, contract_addresses:list, vs_currencies:list=["usd"])->dict:
        """ get multiple prices at once (current)

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

        result  = dict()
        # get price from coingecko
        cg = CoinGeckoAPI()

        # split contract_addresses in batches so URI too long errors do not popup
        n = 50
        # using list comprehension
        for i in range(0, len(contract_addresses), n):
            lst = contract_addresses[i : i + n]

            prices = cg.get_token_price(
                id=self.COINGECKO_netids[network],
                vs_currencies=vs_currencies,
                contract_addresses=lst,
            )
            # { "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {
            #     "usd": 20656,
            #     "eur": 20449,
            #     "eth": 12.115137,
            #     "btc": 1.000103
            #         }
            #  }
            result.update(prices)
        
        
        return result


