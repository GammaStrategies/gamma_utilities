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
        "base": "https://api.basescan.org",
        "linea": "https://api.lineascan.build",
        "avalanche": "https://api.snowtrace.io",
        "gnosis": "https://api.gnosisscan.io",
        "blast": "https://api.blastscan.io",
    }
    # config key : network
    _key_network_matches = {
        "etherscan": "ethereum",
        "polygonscan": "polygon",
        "arbiscan": "arbitrum",
        "optimisticetherscan": "optimism",
        "bscscan": "binance",
        "zkevmpolygonscan": "polygon_zkevm",
        "moonbeam": "moonbeam",
        "fantomscan": "fantom",
        "basescan": "base",
        "lineascan": "linea",
        "snowtrace": "avalanche",
        "celoscan": "celo",
        "gnosisscan": "gnosis",
        "blastscan": "blast",
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
    def get_contract_supply(self, network: str, contract_address: str) -> int | None:
        if self._check_network_available(network=network) is False:
            return None

        url = "{}/api?{}&apiKey={}".format(
            self._urls[network.lower()],
            self.build_url_arguments(
                module="stats", action="tokensupply", contractaddress=contract_address
            ),
            self._api_keys[network.lower()],
        )

        return self._request_data(url)

    def get_contract_transactions(self, network: str, contract_address: str) -> list:
        if self._check_network_available(network=network) is False:
            return []
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

    def get_wallet_normal_transactions(self, network: str, wallet_address: str) -> list:
        """

        Args:
            network (str): _description_
            wallet_address (str): _description_

        Returns:
            list: _description_
        """
        if self._check_network_available(network=network) is False:
            return []
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
                        action="txlist",
                        contractaddress=wallet_address,
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
                            _data["message"], wallet_address, network
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

    def get_wallet_erc20_transactions(self, network: str, wallet_address: str) -> list:
        """Get all erc20 transactions for a given address

        Args:
            network (str):
            wallet_address (str):

        Returns:
            list: [{
                'to':'0x8c823c1489dcf2af7ded0eccdbf81ff993e1435b'
                'value':'103732130441301140023'
                'tokenName':'CASH'
                'tokenSymbol':'CASH'
                'tokenDecimal':'18'
                'transactionIndex':'70'
                'gas':'524778'
                'gasPrice':'91125092256'
                'gasUsed':'335144'
                'cumulativeGasUsed':'11434816'
                'input':'deprecated'
                'confirmations':'2287898'
            }, ...]
        """
        if self._check_network_available(network=network) is False:
            return []

        result = []
        page = 1  # define pagination var
        offset = 5000  # items to be presented with on each query

        # loop till no more results are retrieved
        while True:
            try:
                url = "{}/api?{}&apiKey={}".format(
                    self._urls[network.lower()],
                    self.build_url_arguments(
                        module="account",
                        action="tokentx",
                        address=wallet_address,
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
                            _data["message"], wallet_address, network
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

    def get_block_by_timestamp(self, network: str, timestamp: int) -> int | None:
        if self._check_network_available(network=network) is False:
            return None

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

        if self._check_network_available(network=network) is False:
            return []

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

    def get_contract_abi(self, network: str, contract_address: str) -> str | None:
        """Returns the Contract Application Binary Interface ( ABI ) of a verified smart contract

        Args:
            network (str): _description_
            contract_address (str): _description_

        Returns:
            str | None: _description_
        """
        if self._check_network_available(network=network) is False:
            return []

        result = []
        # loop till no more results are retrieved
        try:
            url = "{}/api?{}&apiKey={}".format(
                self._urls[network.lower()],
                self.build_url_arguments(
                    module="contract",
                    action="getabi",
                    address=contract_address,
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
                else:
                    # no data
                    pass
            else:
                logging.getLogger(__name__).debug(
                    " {} for {} in {}  . error message: {}".format(
                        _data["message"], contract_address, network
                    )
                )

        except Exception as e:
            # do not continue
            logging.getLogger(__name__).error(
                f' Unexpected error while querying url {url}    . error message: {_data["message"] if _data else e}'
            )

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

    def _check_network_available(self, network: str) -> bool:
        if network.lower() in self._urls.keys():
            return True
        else:
            logging.getLogger(__name__).debug(
                f" Network {network} not available in etherscan helper"
            )
            return False
