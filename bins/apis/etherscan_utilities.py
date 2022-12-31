
import logging
from bins.general import net_utilities


class etherscan_helper:
    
    _api_keys = dict() 
    _urls = {"ethereum":"https://api.etherscan.io",
             "polygon": "https://api.polygonscan.com",
             "optimism": "https://api-optimistic.etherscan.io",
             "arbitrum":"https://api.arbiscan.io",
             "celo":"https://api.celoscan.io"
        }

    def __init__(self, api_keys:dict):
        """  Etherscan minimal API wrapper
         Args:
            api_keys (dict): {<network>:<APIKEY>} or {"etherscan":<key> , "polygonscan":key...}
         """        

        self._api_keys = self.__setup_apiKeys(api_keys)
        
        self.__RATE_LIMIT = net_utilities.rate_limit(rate_max_sec=5) #  rate limiter

        # api network keys must be present in any case
        for k in self._urls.keys():
            if not k in self._api_keys.keys():
                self._api_keys[k] = ""

   # SETUP
    def __setup_apiKeys(self, apiKeys:dict):
        """ arrange api keys in an easier way to handle 
         Args:
            tokens (_type_): as stated in config.yaml file
         """
        if "etherscan" in apiKeys.keys():
            # needs to be processed
            result = dict()
            for k,v in apiKeys.items():
                if k.lower() == "etherscan":
                    result["ethereum"] = v
                    result["optimism"] = v
                    result["arbitrum"] = v
                    result["celo"] = v
                elif k.lower() == "polygonscan":
                    result["polygon"] = v
        else:
            # no need to process
            result = apiKeys

        return result

   # PUBLIC
    def get_contract_supply(self, network:str, contract_address:str)->int:
       
        url = "{}/api?{}&apiKey={}".format(
                self._urls[network.lower()], 
                self.build_url_arguments( 
                    module="stats", action="tokensupply", contractaddress=contract_address),
                self._api_keys[network.lower()]
                )
        
        # rate control
        self.__RATE_LIMIT.continue_when_safe()

        _data = net_utilities.get_request(url)  #  {"status":"1","message":"OK-Missing/Invalid API Key, rate limit of 1/5sec applied","result":"21265524714464"}
        
        if _data["status"] == "1":
            return int(_data["result"])
        else:
            # todo: log message
            logging.getLogger(__name__).error(" Unexpected error while querying url {}    . error message: {}".format(
                    url, _data["message"]))
            return 0

    def get_contract_transactions(self, network:str, contract_address: str) -> list:
        result = list()
        page = 1 # define pagination var
        offset = 10000 # items to be presented with on each query
        
        # loop till no more results are retrieved
        while True:
            
            try:
                url = "{}/api?{}&apiKey={}".format(
                        self._urls[network.lower()], 
                        self.build_url_arguments( 
                            module="account", action="tokentx", contractaddress=contract_address,
                            startblock=0,endblock=99999999,page=page,offset=offset, sort="asc"),
                        self._api_keys[network.lower()]
                        )
                
                # rate control
                self.__RATE_LIMIT.continue_when_safe()

                # get data
                _data = net_utilities.get_request(url)  #  {"status":"1","message":"OK-Missing/Invalid API Key, rate limit of 1/5sec applied","result":....}
                
                if _data["status"] == "1":
                    # query when thru ok
                    if len(_data["result"]) > 0:
                        # Add data to result
                        result += _data["result"]

                        if len(_data["result"]) < offset:
                            # there is no more data to be scraped
                            break
                        else:
                            # add pagination var
                            page +=1
                    else:
                        # no data
                        break
                else:
                    logging.getLogger(__name__).debug(" {} for {} in {}  . error message: {}".format( _data["message"], contract_address, network))
                    break

            except:
                # do not continue
                logging.getLogger(__name__).error(" Unexpected error while querying url {}    . error message: {}".format(
                    url, _data["message"]))
                break

        # return result
        return result

    def get_block_by_timestamp(self, network:str, timestamp:int)->int:
        url = "{}/api?{}&apiKey={}".format(
                self._urls[network.lower()], 
                self.build_url_arguments( 
                    module="block", action="getblocknobytime", closest="before", timestamp=timestamp),
                self._api_keys[network.lower()]
                )
        
        # rate control
        self.__RATE_LIMIT.continue_when_safe()

        _data = net_utilities.get_request(url)  #  {"status":"1","message":"OK-Missing/Invalid API Key, rate limit of 1/5sec applied","result":"21265524714464"}
        
        if _data["status"] == "1":
            return int(_data["result"])
        else:
            # todo: log message
            logging.getLogger(__name__).error(" Unexpected error while querying url {}    . error message: {}".format(
                    url, _data["message"]))
            return 0

   # HELPERs
    def build_url_arguments(self, **kargs)->str:
        result = ""
        for k,v in kargs.items():
            separator = "&" if result != "" else ""
            result += "{}{}={}".format(separator,k,v)
        return result

