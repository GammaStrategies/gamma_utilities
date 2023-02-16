import logging
import sys

from eth_abi import abi
from hexbytes import HexBytes
from decimal import Decimal

from web3 import Web3
from web3.middleware import geth_poa_middleware

from bins.configuration import CONFIGURATION


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
        self.setup_w3(network=network)

        # set topics vars
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # define helper
        self._web3_helper = erc20(
            address="0x0000000000000000000000000000000000000000", network=network
        )

    def setup_topics(self, topics: dict, topics_data_decoders: dict):

        if not topics == None and len(topics.keys()) > 0:
            # set topics
            self._topics = topics
            # create a reversed topic list to be used to process topics
            self._topics_reversed = {v: k for k, v in self._topics.items()}

        if not topics_data_decoders == None and len(topics_data_decoders.keys()) > 0:
            # set data decoders
            self._topics_data_decoders = topics_data_decoders

    def setup_w3(self, network: str):
        # create Web3 helper
        self._w3 = Web3(
            Web3.HTTPProvider(
                CONFIGURATION["sources"]["web3Providers"][network],
                request_kwargs={"timeout": 120},
            )
        )
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
        itm["timestamp"] = self._w3.eth.get_block(itm["blockNumber"]).timestamp

        # create a cached decimal dict
        if not itm["address"].lower() in self._token_helpers:
            try:
                tmp = gamma_hypervisor(address=itm["address"], network=self.network)
                # decimals should be inmutable
                self._token_helpers[itm["address"].lower()] = {
                    "address_token0": tmp.token0.address,
                    "address_token1": tmp.token1.address,
                    "decimals_token0": tmp.token0.decimals,
                    "decimals_token1": tmp.token1.decimals,
                    "decimals_contract": tmp.decimals,
                }
            except:
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
