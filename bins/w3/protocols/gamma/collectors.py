import logging
import sys

from eth_abi import abi
from hexbytes import HexBytes

from web3 import Web3
from web3.middleware import geth_poa_middleware, simple_cache_middleware

from ....configuration import CONFIGURATION, rpcUrl_list
from ..general import erc20, bep20
from .hypervisor import (
    gamma_hypervisor,
    gamma_hypervisor_bep20,
)


class data_collector:
    """Scrapes the chain once to gather
    all configured topics from the contracts addresses supplied (hypervisor list)
    main func being <get_all_operations>

    IMPORTANT: data has no decimal conversion
    """

    # SETUP
    def __init__(
        self,
        topics: dict,
        topics_data_decoders: dict,
        network: str,
    ):
        self.network = network

        self._progress_callback = None  # log purp
        # univ3_pool helpers simulating contract functionality just to be able to use the tokenX decimal part
        self._token_helpers = dict()
        # all data retrieved will be saved here. { <contract_address>: {<topic>: <topic defined content> } }
        self._data = dict()

        # setup Web3
        # self.setup_w3(network=network)

        # set topics vars
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # define helper
        self._web3_helper = (
            bep20(address="0x0000000000000000000000000000000000000000", network=network)
            if network == "binance"
            else erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
        )

    def setup_topics(self, topics: dict, topics_data_decoders: dict):
        if not topics is None and len(topics.keys()) > 0:
            # set topics
            self._topics = topics
            # create a reversed topic list to be used to process topics
            self._topics_reversed = {v: k for k, v in self._topics.items()}

        if not topics_data_decoders is None and len(topics_data_decoders.keys()) > 0:
            # set data decoders
            self._topics_data_decoders = topics_data_decoders

    # PROPS
    @property
    def progress_callback(self):
        return self._progress_callback

    @progress_callback.setter
    def progress_callback(self, value):
        self._progress_callback = value
        self._web3_helper._progress_callback = value

    def operations_generator(
        self,
        block_ini: int,
        block_end: int,
        contracts: list,
        topics: dict = {},
        topics_data_decoders: dict = {},
        max_blocks: int = 5000,
    ) -> list[dict]:
        """operation item generator

        Args:
            block_ini (int): _description_
            block_end (int): _description_
            contracts (list): _description_
            topics (dict, optional): _description_. Defaults to {}.
            topics_data_decoders (dict, optional): _description_. Defaults to {}.
            max_blocks (int, optional): _description_. Defaults to 5000.

        Returns:
            dict: includes topic operation like deposits, withdraws, transfers...

        Yields:
            Iterator[dict]:
        """
        # set topics vars ( if set )
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # get a list of events
        filter_chunks = self._web3_helper.create_eventFilter_chunks(
            eventfilter={
                "fromBlock": block_ini,
                "toBlock": block_end,
                "address": contracts,
                "topics": [[v for k, v in self._topics.items()]],
            },
            max_blocks=max_blocks,
        )

        for filter in filter_chunks:
            if entries := self._web3_helper.get_all_entries(
                filter=filter, rpcKey_names=["private"]
            ):
                chunk_result = []
                for event in entries:
                    # get topic name found
                    topic = self._topics_reversed[event.topics[0].hex()]
                    # first topic is topic id
                    custom_abi_data = self._topics_data_decoders[topic]
                    # decode
                    data = abi.decode(custom_abi_data, HexBytes(event.data))

                    # show progress
                    if self._progress_callback:
                        self._progress_callback(
                            text="processing {} at block:{}".format(
                                topic, event.blockNumber
                            ),
                            remaining=block_end - event.blockNumber,
                            total=block_end - block_ini,
                        )

                    # convert data
                    result_item = self._convert_topic(topic, event, data)
                    # add topic to result item
                    result_item["topic"] = "{}".format(topic.split("_")[1])
                    result_item["logIndex"] = event.logIndex

                    chunk_result.append(result_item)

                yield chunk_result

    # HELPERS
    def _convert_topic(self, topic: str, event, data) -> dict:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address

        itm["timestamp"] = ""
        itm["decimals_token0"] = ""
        itm["decimals_token1"] = ""
        itm["decimals_contract"] = ""

        # specific vars
        if topic in ["gamma_deposit", "gamma_withdraw"]:
            itm["sender"] = event.topics[1][-20:].hex()
            itm["to"] = event.topics[2][-20:].hex()
            itm["shares"] = str(data[0])
            itm["qtty_token0"] = str(data[1])
            itm["qtty_token1"] = str(data[2])

        elif topic == "gamma_rebalance":
            # rename topic to fee
            # topic = "gamma_fee"
            itm["tick"] = data[0]
            itm["totalAmount0"] = str(data[1])
            itm["totalAmount1"] = str(data[2])
            itm["qtty_token0"] = str(data[3])
            itm["qtty_token1"] = str(data[4])

        elif topic == "gamma_zeroBurn":
            itm["fee"] = data[0]
            itm["qtty_token0"] = str(data[1])
            itm["qtty_token1"] = str(data[2])

        elif topic in ["gamma_transfer", "arrakis_transfer"]:
            itm["src"] = event.topics[1][-20:].hex()
            itm["dst"] = event.topics[2][-20:].hex()
            itm["qtty"] = str(data[0])

        elif topic in ["arrakis_deposit", "arrakis_withdraw"]:
            itm["sender"] = data[0] if topic == "arrakis_deposit" else event.address
            itm["to"] = data[0] if topic == "arrakis_withdraw" else event.address
            itm["qtty_token0"] = str(data[2])  # amount0
            itm["qtty_token1"] = str(data[3])  # amount1
            itm["shares"] = str(data[1])  # mintAmount

        elif topic == "arrakis_fee":
            itm["qtty_token0"] = str(data[0])
            itm["qtty_token1"] = str(data[1])

        elif topic == "arrakis_rebalance":
            itm["lowerTick"] = str(data[0])
            itm["upperTick"] = str(data[1])
            # data[2] #liquidityBefore
            # data[2] #liquidityAfter
        elif topic in ["gamma_approval"]:
            itm["value"] = str(data[0])

        elif topic in ["gamma_setFee"]:
            itm["fee"] = data[0]

        elif topic == "uniswapv3_collect":
            itm["recipient"] = data[0]
            itm["amount0"] = str(data[1])
            itm["amount1"] = str(data[2])
        else:
            logging.getLogger(__name__).warning(
                f" Can't find topic [{topic}] converter. Discarding  event [{event}]  with data [{data}] "
            )

        return itm


def create_data_collector(network: str) -> data_collector:
    """Create a data collector class

    Args:
       network (str):

    Returns:
       data_collector:
    """
    result = data_collector(
        topics={
            "gamma_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # event_signature_hash = web3.keccak(text="transfer(uint32...)").hex()
            "gamma_rebalance": "0xbc4c20ad04f161d631d9ce94d27659391196415aa3c42f6a71c62e905ece782d",
            "gamma_deposit": "0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6",
            "gamma_withdraw": "0xebff2602b3f468259e1e99f613fed6691f3a6526effe6ef3e768ba7ae7a36c4f",
            "gamma_approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
            "gamma_setFee": "0x91f2ade82ab0e77bb6823899e6daddc07e3da0e3ad998577e7c09c2f38943c43",
            "gamma_zeroBurn": "0x4606b8a47eb284e8e80929101ece6ab5fe8d4f8735acc56bd0c92ca872f2cfe7",
        },
        topics_data_decoders={
            "gamma_transfer": ["uint256"],
            "gamma_rebalance": [
                "int24",
                "uint256",
                "uint256",
                "uint256",
                "uint256",
                "uint256",
            ],
            "gamma_deposit": ["uint256", "uint256", "uint256"],
            "gamma_withdraw": ["uint256", "uint256", "uint256"],
            "gamma_approval": ["uint256"],
            "gamma_setFee": ["uint8"],
            "gamma_zeroBurn": [
                "uint8",  # fee
                "uint256",  # fees0
                "uint256",  # fees1
            ],
        },
        network=network,
    )
    return result


############################################################################################################


class data_collector_OLD:
    """Scrapes the chain once to gather
    all configured topics from the contracts addresses supplied (hypervisor list)
    main func being <get_all_operations>

    IMPORTANT: data has no decimal conversion
    """

    # SETUP
    def __init__(
        self,
        topics: dict,
        topics_data_decoders: dict,
        network: str,
    ):
        self.network = network

        self._progress_callback = None  # log purp
        # univ3_pool helpers simulating contract functionality just to be able to use the tokenX decimal part
        self._token_helpers = dict()
        # all data retrieved will be saved here. { <contract_address>: {<topic>: <topic defined content> } }
        self._data = dict()

        # setup Web3
        self.setup_w3(network=network)

        # set topics vars
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # define helper
        self._web3_helper = (
            bep20(address="0x0000000000000000000000000000000000000000", network=network)
            if network == "binance"
            else erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
        )

    def setup_topics(self, topics: dict, topics_data_decoders: dict):
        if not topics is None and len(topics.keys()) > 0:
            # set topics
            self._topics = topics
            # create a reversed topic list to be used to process topics
            self._topics_reversed = {v: k for k, v in self._topics.items()}

        if not topics_data_decoders is None and len(topics_data_decoders.keys()) > 0:
            # set data decoders
            self._topics_data_decoders = topics_data_decoders

    def setup_w3(self, network: str):
        # create Web3 helper

        rpcProvider = rpcUrl_list(network=network, rpcKey_names=["private"])[0]

        self._w3 = Web3(
            Web3.HTTPProvider(
                rpcProvider,
                request_kwargs={"timeout": 120},
            )
        )
        # add simple cache module
        self._w3.middleware_onion.add(simple_cache_middleware)

        # add middleware as needed
        if network != "ethereum":
            self._w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # PROPS
    @property
    def progress_callback(self):
        return self._progress_callback

    @progress_callback.setter
    def progress_callback(self, value):
        self._progress_callback = value
        self._web3_helper._progress_callback = value

    # PUBLIC
    # TODO:  remove or change
    def fill_all_operations_data(
        self,
        block_ini: int,
        block_end: int,
        contracts: list,
        topics: dict = {},
        topics_data_decoders: dict = {},
        max_blocks: int = 5000,
    ):
        """Retrieve all topic data from the hypervisors list
           All content is saved in _data var

        Args:
           block_ini (int):  from this block (inclusive)
           block_end (int):  to this block (inclusive)
           contracts (list):  list of contract addresses to look for
        """

        # clear data content to save result to
        if len(self._data.keys()) > 0:
            self._data = dict()

        # set topics vars ( if set )
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # get all possible events
        for event in self._web3_helper.get_chunked_events(
            eventfilter={
                "fromBlock": block_ini,
                "toBlock": block_end,
                "address": contracts,
                "topics": [[v for k, v in self._topics.items()]],
            },
            max_blocks=max_blocks,
        ):
            # get topic name found
            topic = self._topics_reversed[event.topics[0].hex()]
            # first topic is topic id
            custom_abi_data = self._topics_data_decoders[topic]
            # decode
            data = abi.decode(custom_abi_data, HexBytes(event.data))
            # save topic data to cache
            self._save_topic(topic, event, data)

            # show progress
            if self._progress_callback:
                self._progress_callback(
                    text="processing {} at block:{}".format(topic, event.blockNumber),
                    remaining=block_end - event.blockNumber,
                    total=block_end - block_ini,
                )

    def operations_generator(
        self,
        block_ini: int,
        block_end: int,
        contracts: list,
        topics: dict = {},
        topics_data_decoders: dict = {},
        max_blocks: int = 5000,
    ) -> dict:
        """operation item generator

        Args:
            block_ini (int): _description_
            block_end (int): _description_
            contracts (list): _description_
            topics (dict, optional): _description_. Defaults to {}.
            topics_data_decoders (dict, optional): _description_. Defaults to {}.
            max_blocks (int, optional): _description_. Defaults to 5000.

        Returns:
            dict: includes topic operation like deposits, withdraws, transfers...

        Yields:
            Iterator[dict]:
        """
        # set topics vars ( if set )
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)
        # get all possible events using private rpc providers
        for event in self._web3_helper.get_chunked_events(
            eventfilter={
                "fromBlock": block_ini,
                "toBlock": block_end,
                "address": contracts,
                "topics": [[v for k, v in self._topics.items()]],
            },
            max_blocks=max_blocks,
            rpcKey_names=["private"],
        ):
            # get topic name found
            topic = self._topics_reversed[event.topics[0].hex()]
            # first topic is topic id
            custom_abi_data = self._topics_data_decoders[topic]
            # decode
            data = abi.decode(custom_abi_data, HexBytes(event.data))

            # show progress
            if self._progress_callback:
                self._progress_callback(
                    text="processing {} at block:{}".format(topic, event.blockNumber),
                    remaining=block_end - event.blockNumber,
                    total=block_end - block_ini,
                )

            # convert data
            result = self._convert_topic(topic, event, data)
            # add topic to result item
            result["topic"] = "{}".format(topic.split("_")[1])
            result["logIndex"] = event.logIndex

            yield result

    # HELPERS
    # TODO:  remove or change
    def _save_topic(self, topic: str, event, data):
        # init result
        itm = self._convert_topic(topic=topic, event=event, data=data)
        # force fee topic
        if topic == "gamma_rebalance":
            topic = "gamma_fee"

        # contract ( itm["address"])
        if not itm["address"].lower() in self._data.keys():
            self._data[itm["address"].lower()] = dict()

        # create topic in contract if not exists
        topic_key = "{}s".format(topic.split("_")[1])
        if not topic_key in self._data[itm["address"].lower()]:
            self._data[itm["address"].lower()][topic_key] = list()

        # append topic to contract
        self._data[itm["address"].lower()][topic_key].append(itm)

        # SPECIAL CASEs
        # gamma fees / rebalances
        if topic == "gamma_fee":
            # rename to rebalance
            topic = "gamma_rebalance"
            # gamma fees and rebalances are in the same place
            # create topic in contract if not existnt
            topic_key = "{}s".format(topic.split("_")[1])
            if not topic_key in self._data[itm["address"].lower()]:
                self._data[itm["address"].lower()][topic_key] = list()
            self._data[itm["address"].lower()][topic_key].append(
                {
                    "transactionHash": event.transactionHash.hex(),
                    "blockHash": event.blockHash.hex(),
                    "blockNumber": event.blockNumber,
                    "address": event.address,
                    "lowerTick": None,
                    "upperTick": None,
                }
            )

    def _convert_topic(self, topic: str, event, data) -> dict:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address
        itm["timestamp"] = self._web3_helper._getBlockData(itm["blockNumber"]).timestamp

        # create a cached decimal dict
        if not itm["address"].lower() in self._token_helpers:
            try:
                tmp = (
                    gamma_hypervisor_bep20(address=itm["address"], network=self.network)
                    if self.network == "binance"
                    else gamma_hypervisor(address=itm["address"], network=self.network)
                )

                # decimals should be inmutable
                self._token_helpers[itm["address"].lower()] = {
                    "address_token0": tmp.token0.address,
                    "address_token1": tmp.token1.address,
                    "decimals_token0": tmp.token0.decimals,
                    "decimals_token1": tmp.token1.decimals,
                    "decimals_contract": tmp.decimals,
                }
            except Exception:
                logging.getLogger(__name__).error(
                    " Unexpected error caching topic ({}) related info from hyp: {}    .transaction hash: {}    -> error: {}".format(
                        topic, itm["address"], itm["transactionHash"], sys.exc_info()[0]
                    )
                )

        # set decimal vars for later use
        decimals_token0 = self._token_helpers[itm["address"].lower()]["decimals_token0"]
        decimals_token1 = self._token_helpers[itm["address"].lower()]["decimals_token1"]
        decimals_contract = self._token_helpers[itm["address"].lower()][
            "decimals_contract"
        ]

        itm["decimals_token0"] = decimals_token0
        itm["decimals_token1"] = decimals_token1
        itm["decimals_contract"] = decimals_contract

        # specific vars
        if topic in ["gamma_deposit", "gamma_withdraw"]:
            itm["sender"] = event.topics[1][-20:].hex()
            itm["to"] = event.topics[2][-20:].hex()
            itm["shares"] = str(data[0])
            itm["qtty_token0"] = str(data[1])
            itm["qtty_token1"] = str(data[2])

        elif topic == "gamma_rebalance":
            # rename topic to fee
            # topic = "gamma_fee"
            itm["tick"] = data[0]
            itm["totalAmount0"] = str(data[1])
            itm["totalAmount1"] = str(data[2])
            itm["qtty_token0"] = str(data[3])
            itm["qtty_token1"] = str(data[4])

        elif topic == "gamma_zeroBurn":
            itm["fee"] = data[0]
            itm["qtty_token0"] = str(data[1])
            itm["qtty_token1"] = str(data[2])

        elif topic in ["gamma_transfer", "arrakis_transfer"]:
            itm["src"] = event.topics[1][-20:].hex()
            itm["dst"] = event.topics[2][-20:].hex()
            itm["qtty"] = str(data[0])

        elif topic in ["arrakis_deposit", "arrakis_withdraw"]:
            itm["sender"] = data[0] if topic == "arrakis_deposit" else event.address
            itm["to"] = data[0] if topic == "arrakis_withdraw" else event.address
            itm["qtty_token0"] = str(data[2])  # amount0
            itm["qtty_token1"] = str(data[3])  # amount1
            itm["shares"] = str(data[1])  # mintAmount

        elif topic == "arrakis_fee":
            itm["qtty_token0"] = str(data[0])
            itm["qtty_token1"] = str(data[1])

        elif topic == "arrakis_rebalance":
            itm["lowerTick"] = str(data[0])
            itm["upperTick"] = str(data[1])
            # data[2] #liquidityBefore
            # data[2] #liquidityAfter
        elif topic in ["gamma_approval"]:
            itm["value"] = str(data[0])

        elif topic in ["gamma_setFee"]:
            itm["fee"] = data[0]

        elif topic == "uniswapv3_collect":
            itm["recipient"] = data[0]
            itm["amount0"] = str(data[1])
            itm["amount1"] = str(data[2])
        else:
            logging.getLogger(__name__).warning(
                f" Can't find topic [{topic}] converter. Discarding  event [{event}]  with data [{data}] "
            )

        return itm


class data_collector_alternative:
    """Scrapes the chain once to gather
    all configured topics from the contracts addresses supplied (hypervisor list)
    main func being <get_all_operations>

    IMPORTANT: data has no decimal conversion
    """

    # SETUP
    def __init__(
        self,
        topics: dict,
        topics_data_decoders: dict,
        network: str,
    ):
        self.network = network

        self._progress_callback = None  # log purp
        # univ3_pool helpers simulating contract functionality just to be able to use the tokenX decimal part
        self._token_helpers = dict()
        # all data retrieved will be saved here. { <contract_address>: {<topic>: <topic defined content> } }
        self._data = dict()

        # setup Web3
        # self.setup_w3(network=network)

        # set topics vars
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # define helper
        self._web3_helper = (
            bep20(address="0x0000000000000000000000000000000000000000", network=network)
            if network == "binance"
            else erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
        )

    def setup_topics(self, topics: dict, topics_data_decoders: dict):
        if not topics is None and len(topics.keys()) > 0:
            # set topics
            self._topics = topics
            # create a reversed topic list to be used to process topics
            self._topics_reversed = {v: k for k, v in self._topics.items()}

        if not topics_data_decoders is None and len(topics_data_decoders.keys()) > 0:
            # set data decoders
            self._topics_data_decoders = topics_data_decoders

    # PROPS
    @property
    def progress_callback(self):
        return self._progress_callback

    @progress_callback.setter
    def progress_callback(self, value):
        self._progress_callback = value
        self._web3_helper._progress_callback = value

    def operations_generator(
        self,
        block_ini: int,
        block_end: int,
        contracts: list,
        topics: dict = {},
        topics_data_decoders: dict = {},
        max_blocks: int = 5000,
    ) -> list[dict]:
        """operation item generator

        Args:
            block_ini (int): _description_
            block_end (int): _description_
            contracts (list): _description_
            topics (dict, optional): _description_. Defaults to {}.
            topics_data_decoders (dict, optional): _description_. Defaults to {}.
            max_blocks (int, optional): _description_. Defaults to 5000.

        Returns:
            dict: includes topic operation like deposits, withdraws, transfers...

        Yields:
            Iterator[dict]:
        """
        # set topics vars ( if set )
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # get a list of events
        filter_chunks = self._web3_helper.create_eventFilter_chunks(
            eventfilter={
                "fromBlock": block_ini,
                "toBlock": block_end,
                "address": contracts,
                "topics": [[v for k, v in self._topics.items()]],
            },
            max_blocks=max_blocks,
        )

        for filter in filter_chunks:
            if entries := self._web3_helper.get_all_entries(
                filter=filter, rpcKey_names=["private"]
            ):
                chunk_result = []
                for event in entries:
                    # get topic name found
                    topic = self._topics_reversed[event.topics[0].hex()]
                    # first topic is topic id
                    custom_abi_data = self._topics_data_decoders[topic]
                    # decode
                    data = abi.decode(custom_abi_data, HexBytes(event.data))

                    # show progress
                    if self._progress_callback:
                        self._progress_callback(
                            text="processing {} at block:{}".format(
                                topic, event.blockNumber
                            ),
                            remaining=block_end - event.blockNumber,
                            total=block_end - block_ini,
                        )

                    # convert data
                    result_item = self._convert_topic(topic, event, data)
                    # add topic to result item
                    result_item["topic"] = "{}".format(topic.split("_")[1])
                    result_item["logIndex"] = event.logIndex

                    chunk_result.append(result_item)

                yield chunk_result

    # HELPERS
    def _convert_topic(self, topic: str, event, data) -> dict:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address
        itm["timestamp"] = self._web3_helper.timestampFromBlockNumber(
            int(itm["blockNumber"])
        )

        # create a cached decimal dict
        if not itm["address"].lower() in self._token_helpers:
            try:
                tmp = (
                    gamma_hypervisor_bep20(address=itm["address"], network=self.network)
                    if self.network == "binance"
                    else gamma_hypervisor(address=itm["address"], network=self.network)
                )

                # decimals should be inmutable
                self._token_helpers[itm["address"].lower()] = {
                    "address_token0": tmp.token0.address,
                    "address_token1": tmp.token1.address,
                    "decimals_token0": tmp.token0.decimals,
                    "decimals_token1": tmp.token1.decimals,
                    "decimals_contract": tmp.decimals,
                }
            except Exception:
                logging.getLogger(__name__).error(
                    " Unexpected error caching topic ({}) related info from hyp: {}    .transaction hash: {}    -> error: {}".format(
                        topic, itm["address"], itm["transactionHash"], sys.exc_info()[0]
                    )
                )

        # set decimal vars for later use
        decimals_token0 = self._token_helpers[itm["address"].lower()]["decimals_token0"]
        decimals_token1 = self._token_helpers[itm["address"].lower()]["decimals_token1"]
        decimals_contract = self._token_helpers[itm["address"].lower()][
            "decimals_contract"
        ]

        itm["decimals_token0"] = decimals_token0
        itm["decimals_token1"] = decimals_token1
        itm["decimals_contract"] = decimals_contract

        # specific vars
        if topic in ["gamma_deposit", "gamma_withdraw"]:
            itm["sender"] = event.topics[1][-20:].hex()
            itm["to"] = event.topics[2][-20:].hex()
            itm["shares"] = str(data[0])
            itm["qtty_token0"] = str(data[1])
            itm["qtty_token1"] = str(data[2])

        elif topic == "gamma_rebalance":
            # rename topic to fee
            # topic = "gamma_fee"
            itm["tick"] = data[0]
            itm["totalAmount0"] = str(data[1])
            itm["totalAmount1"] = str(data[2])
            itm["qtty_token0"] = str(data[3])
            itm["qtty_token1"] = str(data[4])

        elif topic == "gamma_zeroBurn":
            itm["fee"] = data[0]
            itm["qtty_token0"] = str(data[1])
            itm["qtty_token1"] = str(data[2])

        elif topic in ["gamma_transfer", "arrakis_transfer"]:
            itm["src"] = event.topics[1][-20:].hex()
            itm["dst"] = event.topics[2][-20:].hex()
            itm["qtty"] = str(data[0])

        elif topic in ["arrakis_deposit", "arrakis_withdraw"]:
            itm["sender"] = data[0] if topic == "arrakis_deposit" else event.address
            itm["to"] = data[0] if topic == "arrakis_withdraw" else event.address
            itm["qtty_token0"] = str(data[2])  # amount0
            itm["qtty_token1"] = str(data[3])  # amount1
            itm["shares"] = str(data[1])  # mintAmount

        elif topic == "arrakis_fee":
            itm["qtty_token0"] = str(data[0])
            itm["qtty_token1"] = str(data[1])

        elif topic == "arrakis_rebalance":
            itm["lowerTick"] = str(data[0])
            itm["upperTick"] = str(data[1])
            # data[2] #liquidityBefore
            # data[2] #liquidityAfter
        elif topic in ["gamma_approval"]:
            itm["value"] = str(data[0])

        elif topic in ["gamma_setFee"]:
            itm["fee"] = data[0]

        elif topic == "uniswapv3_collect":
            itm["recipient"] = data[0]
            itm["amount0"] = str(data[1])
            itm["amount1"] = str(data[2])
        else:
            logging.getLogger(__name__).warning(
                f" Can't find topic [{topic}] converter. Discarding  event [{event}]  with data [{data}] "
            )

        return itm


def create_data_collector_alternative(network: str) -> data_collector_alternative:
    """Create a data collector class

    Args:
       network (str):

    Returns:
       data_collector:
    """
    result = data_collector_alternative(
        topics={
            "gamma_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # event_signature_hash = web3.keccak(text="transfer(uint32...)").hex()
            "gamma_rebalance": "0xbc4c20ad04f161d631d9ce94d27659391196415aa3c42f6a71c62e905ece782d",
            "gamma_deposit": "0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6",
            "gamma_withdraw": "0xebff2602b3f468259e1e99f613fed6691f3a6526effe6ef3e768ba7ae7a36c4f",
            "gamma_approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
            "gamma_setFee": "0x91f2ade82ab0e77bb6823899e6daddc07e3da0e3ad998577e7c09c2f38943c43",
            "gamma_zeroBurn": "0x4606b8a47eb284e8e80929101ece6ab5fe8d4f8735acc56bd0c92ca872f2cfe7",
        },
        topics_data_decoders={
            "gamma_transfer": ["uint256"],
            "gamma_rebalance": [
                "int24",
                "uint256",
                "uint256",
                "uint256",
                "uint256",
                "uint256",
            ],
            "gamma_deposit": ["uint256", "uint256", "uint256"],
            "gamma_withdraw": ["uint256", "uint256", "uint256"],
            "gamma_approval": ["uint256"],
            "gamma_setFee": ["uint8"],
            "gamma_zeroBurn": [
                "uint8",  # fee
                "uint256",  # fees0
                "uint256",  # fees1
            ],
        },
        network=network,
    )
    return result
