import logging
from ..general import net_utilities


class etherscan_helper:
    _urls = {
        "ethereum": "https://api.etherscan.io",
        "polygon": "https://api.polygonscan.com",
        "optimism": "https://api-optimistic.etherscan.io",
        "arbitrum": "https://api.arbiscan.io",
        "celo": "https://api.celoscan.io",
        "polygon_zkevm": "https://api-zkevm.polygonscan.com/",
        "binance": "https://api.bscscan.com",
        "moonbeam": "https://api-moonbeam.moonscan.io",
        "fantomscan": "https://api.ftmscan.com",
    }
    _key_network_matches = {
        "etherscan": "ethereum",
        "polygonscan": "polygon",
        "arbiscan": "arbitrum",
        "optimisticetherscan": "optimism",
        "bscscan": "binance",
        "zkevmpolygonscan": "polygon_zkevm",
        "moonbeam": "moonbeam",
        "fantomscan": "fantomscan",
    }

    def __init__(self, api_keys: dict):
        """Etherscan minimal API wrapper
        Args:
           api_keys (dict): {<network>:<APIKEY>} or {"etherscan":<key> , "polygonscan":key...}
        """

        self._api_keys = self.__setup_apiKeys(api_keys)

        self.__RATE_LIMIT = net_utilities.rate_limit(rate_max_sec=5)  #  rate limiter

        # api network keys must be present in any case
        for k in self._urls.keys():
            if k not in self._api_keys.keys():
                self._api_keys[k] = ""

    # SETUP
    def __setup_apiKeys(self, apiKeys: dict):
        """arrange api keys in an easier way to handle
        Args:
           tokens (_type_): as stated in config.yaml file
        """
        result = {}
        for k, v in apiKeys.items():
            if k.lower() in self._key_network_matches.keys():
                result[self._key_network_matches[k.lower()]] = v

        return result

    # PUBLIC
    def get_contract_supply(self, network: str, contract_address: str) -> int:
        url = "{}/api?{}&apiKey={}".format(
            self._urls[network.lower()],
            self.build_url_arguments(
                module="stats", action="tokensupply", contractaddress=contract_address
            ),
            self._api_keys[network.lower()],
        )

        return self._request_data(url)

    def get_contract_transactions(self, network: str, contract_address: str) -> list:
        result = []
        page = 1  # define pagination var
        offset = 10000  # items to be presented with on each query

        # loop till no more results are retrieved
        while True:
            try:
                url = "{}/api?{}&apiKey={}".format(
                    self._urls[network.lower()],
                    self.build_url_arguments(
                        module="account",
                        action="tokentx",
                        contractaddress=contract_address,
                        startblock=0,
                        endblock=99999999,
                        page=page,
                        offset=offset,
                        sort="asc",
                    ),
                    self._api_keys[network.lower()],
                )

                # rate control
                self.__RATE_LIMIT.continue_when_safe()

                # get data
                _data = net_utilities.get_request(
                    url
                )  #  {"status":"1","message":"OK-Missing/Invalid API Key, rate limit of 1/5sec applied","result":....}

                if _data["status"] == "1":
                    # query when thru ok
                    if _data["result"]:
                        # Add data to result
                        result += _data["result"]

                        if len(_data["result"]) < offset:
                            # there is no more data to be scraped
                            break
                        else:
                            # add pagination var
                            page += 1
                    else:
                        # no data
                        break
                else:
                    logging.getLogger(__name__).debug(
                        " {} for {} in {}  . error message: {}".format(
                            _data["message"], contract_address, network
                        )
                    )
                    break

            except Exception:
                # do not continue
                logging.getLogger(__name__).error(
                    f' Unexpected error while querying url {url}    . error message: {_data["message"]}'
                )

                break

        # return result
        return result

    def get_block_by_timestamp(self, network: str, timestamp: int) -> int:
        url = "{}/api?{}&apiKey={}".format(
            self._urls[network.lower()],
            self.build_url_arguments(
                module="block",
                action="getblocknobytime",
                closest="before",
                timestamp=timestamp,
            ),
            self._api_keys[network.lower()],
        )

        return self._request_data(url)

    def get_contract_creation(
        self, network: str, contract_addresses: list[str]
    ) -> list:
        """_summary_

        Args:
            network (str): _description_
            contract_addresses (list[str]): _description_

        Returns:
            list: [{ contractAddress: "0xb1a0e5fee652348a206d935985ae1e8a9182a245",
                    contractCreator: "0x71e7d05be74ff748c45402c06a941c822d756dc5",
                    txHash: "0x4d7e24a6dab8ba46440a8df3cfca8a4e8225fa2d5daf312c21f0647000d6ce42"
                    }]
        """
        result = []
        page = 1  # define pagination var
        offset = 10000  # items to be presented with on each query
        # loop till no more results are retrieved
        while True:
            try:
                # format contract address to match api requirements
                contract_addresses_string = ",".join(contract_addresses)

                url = "{}/api?{}&apiKey={}".format(
                    self._urls[network.lower()],
                    self.build_url_arguments(
                        module="contract",
                        action="getcontractcreation",
                        contractaddresses=contract_addresses_string,
                        page=page,
                        offset=offset,
                        sort="asc",
                    ),
                    self._api_keys[network.lower()],
                )

                # rate control
                self.__RATE_LIMIT.continue_when_safe()

                # get data
                _data = net_utilities.get_request(
                    url
                )  #  {"status":"1","message":"OK-Missing/Invalid API Key, rate limit of 1/5sec applied","result":....}

                if _data["status"] == "1":
                    # query when thru ok
                    if _data["result"]:
                        # Add data to result
                        result += _data["result"]

                        if len(_data["result"]) < offset:
                            # there is no more data to be scraped
                            break
                        else:
                            # add pagination var
                            page += 1
                    else:
                        # no data
                        break
                else:
                    logging.getLogger(__name__).debug(
                        " {} for {} in {}  . error message: {}".format(
                            _data["message"], contract_addresses, network
                        )
                    )
                    break

            except Exception as e:
                # do not continue
                logging.getLogger(__name__).error(
                    f' Unexpected error while querying url {url}    . error message: {_data["message"] if _data else e}'
                )

                break

        # return result
        return result

    def _request_data(self, url):
        self.__RATE_LIMIT.continue_when_safe()
        _data = net_utilities.get_request(url)
        if _data["status"] == "1":
            return int(_data["result"])

        logging.getLogger(__name__).error(
            f" Unexpected error while querying url {url}    . error message: {_data}"
        )

        return 0

    # HELPERs
    def build_url_arguments(self, **kargs) -> str:
        result = ""
        for k, v in kargs.items():
            separator = "&" if result != "" else ""
            result += f"{separator}{k}={v}"
        return result
