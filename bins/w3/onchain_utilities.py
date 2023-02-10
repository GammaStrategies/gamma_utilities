import logging
import sys
import math
import datetime as dt
from eth_abi import abi
from hexbytes import HexBytes
from decimal import Decimal

from web3 import Web3, exceptions
from web3.contract import ContractEvent
from web3.middleware import geth_poa_middleware

from bins.configuration import HYPERVISOR_REGISTRIES, CONFIGURATION
from bins.general import file_utilities, net_utilities
from bins.cache import cache_utilities
from bins.database.common import db_operations_models

from bins.formulas import univ3_formulas

# GENERAL
class web3wrap:

    _abi_filename = ""  # json file name without the extension
    _abi_path = ""  # like data/abi/gamma
    _abi = ""

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        # set init vars
        self._address = Web3.toChecksumAddress(address)
        self._network = network
        # progress
        self._progress_callback = None

        # set optionals
        self.setup_abi(abi_filename=abi_filename, abi_path=abi_path)

        # setup Web3
        self.setup_w3(network=network)

        # setup contract to query
        self.setup_contract(address=self._address, abi=self._abi)
        # setup cache helper
        self.setup_cache()

        # set block
        self._block = self._w3.eth.get_block("latest").number if block == 0 else block

    def setup_abi(self, abi_filename: str, abi_path: str):
        # set optionals
        if abi_filename != "":
            self._abi_filename = abi_filename
        if abi_path != "":
            self._abi_path = abi_path
        # load abi
        self._abi = file_utilities.load_json(
            filename=self._abi_filename, folder_path=self._abi_path
        )

    def setup_w3(self, network: str):
        # create Web3 helper
        self._w3 = Web3(
            Web3.HTTPProvider(
                CONFIGURATION["sources"]["web3Providers"][network],
                request_kwargs={"timeout": 60},
            )
        )
        # add middleware as needed
        if network != "ethereum":
            self._w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    def setup_contract(self, address: str, abi: str):
        # set contract
        self._contract = self._w3.eth.contract(address=address, abi=abi)

    def setup_cache(self):
        # define network
        self._chain_id = self.w3.eth.chain_id
        # made up a descriptive cahce file name
        cache_filename = "{}_{}".format(self._chain_id, self.address.lower())
        # create cache helper
        self._cache = cache_utilities.standard_property_cache(
            filename=cache_filename, folder_name="data/cache/onchain", reset=False
        )

    # CUSTOM PROPERTIES
    @property
    def address(self) -> str:
        return self._address

    @property
    def w3(self) -> Web3:
        return self._w3

    @property
    def contract(self):
        return self._contract

    @property
    def block(self) -> int:
        """ """
        return self._block

    @block.setter
    def block(self, value: int):
        self._block = value

    # HELPERS
    def average_blockTime(self, blocksaway: int = 500) -> dt.datetime.timestamp:
        """Average time of block creation

        Args:
           blocksaway (int, optional): blocks used compute average. Defaults to 500.

        Returns:
           dt.datetime.timestamp: average time per block
        """
        result = 0
        # no decimals allowed
        blocksaway = math.floor(blocksaway)
        #
        if blocksaway > 0:
            block_curr = self._w3.eth.get_block("latest")
            block_past = self._w3.eth.get_block(block_curr.number - blocksaway)
            result = (block_curr.timestamp - block_past.timestamp) / blocksaway
        return result

    def blockNumberFromTimestamp(
        self,
        timestamp: dt.datetime.timestamp,
        inexact_mode="before",
        eq_timestamp_position="first",
    ) -> int:
        """Will
           At least 15 queries are needed to come close to a timestamp block number

        Args:
           timestamp (dt.datetime.timestamp): _description_
           inexact_mode (str): "before" or "after" -> if found closest to timestapm, choose a block before of after objective
           eq_timestamp_position (str): first or last position to choose when a timestamp corresponds to multiple blocks ( so choose the first or the last one of those blocks)

        Returns:
           int: blocknumber
        """

        if int(timestamp) == 0:
            raise ValueError("Timestamp cannot be zero!")

        # check min timestamp
        min_block = self._w3.eth.get_block(1)
        if min_block.timestamp > timestamp:
            return 1

        queries_cost = 0
        found_exact = False

        block_curr = self._w3.eth.get_block("latest")
        first_step = math.ceil(block_curr.number * 0.85)

        # make sure we have positive block result
        while (block_curr.number + first_step) <= 0:
            first_step -= 1
        # calc blocks to go up/down closer to goal
        block_past = self._w3.eth.get_block(block_curr.number - (first_step))
        blocks_x_timestamp = (
            abs(block_curr.timestamp - block_past.timestamp) / first_step
        )

        block_step = (block_curr.timestamp - timestamp) / blocks_x_timestamp
        block_step_sign = -1

        _startime = dt.datetime.utcnow()

        while block_curr.timestamp != timestamp:

            queries_cost += 1

            # make sure we have positive block result
            while (block_curr.number + (block_step * block_step_sign)) <= 0:
                if queries_cost == 1:
                    # first time here, set lower block steps
                    block_step /= 2
                else:
                    # change sign and lower steps
                    block_step_sign *= -1
                    block_step /= 2

            # go to block
            try:
                block_curr = self._w3.eth.get_block(
                    math.floor(block_curr.number + (block_step * block_step_sign))
                )
            except exceptions.BlockNotFound:
                # diminish step
                block_step /= 2
                continue

            blocks_x_timestamp = (
                (
                    abs(block_curr.timestamp - block_past.timestamp)
                    / abs(block_curr.number - block_past.number)
                )
                if abs(block_curr.number - block_past.number) != 0
                else 0
            )
            if blocks_x_timestamp != 0:
                block_step = math.ceil(
                    abs(block_curr.timestamp - timestamp) / blocks_x_timestamp
                )

            if block_curr.timestamp < timestamp:
                # block should be higher than current
                block_step_sign = 1
            elif block_curr.timestamp > timestamp:
                # block should be lower than current
                block_step_sign = -1
            else:
                # got it
                found_exact = True
                # exit loop
                break

            # set block past
            block_past = block_curr

            # 15sec while loop safe exit (an eternity to find the block)
            if (dt.datetime.utcnow() - _startime).total_seconds() > (15):
                if inexact_mode == "before":
                    # select block smaller than objective
                    while block_curr.timestamp > timestamp:
                        block_curr = self._w3.eth.get_block(block_curr.number - 1)
                elif inexact_mode == "after":
                    # select block greater than objective
                    while block_curr.timestamp < timestamp:
                        block_curr = self._w3.eth.get_block(block_curr.number + 1)
                else:
                    raise ValueError(
                        " Inexact method chosen is not valid:->  {}".format(
                            inexact_mode
                        )
                    )
                # exit loop
                break

        # define result
        result = block_curr.number

        # get blocks with same timestamp
        sametimestampBlocks = self.get_sameTimestampBlocks(block_curr, queries_cost)
        if len(sametimestampBlocks) > 0:
            if eq_timestamp_position == "first":
                result = sametimestampBlocks[0]
            elif eq_timestamp_position == "last":
                result = sametimestampBlocks[-1]

        # log result
        if found_exact:
            logging.getLogger(__name__).debug(
                " Took {} on-chain queries to find block number {} of timestamp {}".format(
                    queries_cost, block_curr.number, timestamp
                )
            )
        else:
            logging.getLogger(__name__).warning(
                " Could not find the exact block number from timestamp -> took {} on-chain queries to find block number {} ({}) closest to timestamp {}  -> original-found difference {}".format(
                    queries_cost,
                    block_curr.number,
                    block_curr.timestamp,
                    timestamp,
                    timestamp - block_curr.timestamp,
                )
            )

        # return closest block found
        return result

    def get_sameTimestampBlocks(self, block, queries_cost: int):

        result = list()
        # try go backwards till different timestamp is found
        curr_block = block
        while curr_block.timestamp == block.timestamp:
            if not curr_block.number == block.number:
                result.append(curr_block.number)
            curr_block = self._w3.eth.get_block(curr_block.number - 1)
            queries_cost += 1
        # try go forward till different timestamp is found
        curr_block = block
        while curr_block.timestamp == block.timestamp:
            if not curr_block.number == block.number:
                result.append(curr_block.number)
            curr_block = self._w3.eth.get_block(curr_block.number + 1)
            queries_cost += 1

        return sorted(result)

    def create_eventFilter_chunks(self, eventfilter: dict, max_blocks=1000) -> list:
        """create a list of event filters
           to be able not to timeout servers

        Args:
           eventfilter (dict):  {'fromBlock': ,
                                   'toBlock': block,
                                   'address': [self._address],
                                   'topics': [self._topics[operation]],
                                   }

        Returns:
           list: of the same
        """
        result = list()
        tmp_filter = {k: v for k, v in eventfilter.items()}
        toBlock = eventfilter["toBlock"]
        fromBlock = eventfilter["fromBlock"]
        blocksXfilter = math.ceil((toBlock - fromBlock) / max_blocks)

        current_fromBlock = tmp_filter["fromBlock"]
        current_toBlock = current_fromBlock + max_blocks
        for i in range(blocksXfilter):

            # mod filter blocks
            tmp_filter["toBlock"] = current_toBlock
            tmp_filter["fromBlock"] = current_fromBlock

            # append filter
            result.append({k: v for k, v in tmp_filter.items()})

            # exit if done...
            if current_toBlock == toBlock:
                break

            # increment chunk
            current_fromBlock = current_toBlock + 1
            current_toBlock = current_fromBlock + max_blocks
            if current_toBlock > toBlock:
                current_toBlock = toBlock

        # return result
        return result

    def get_chunked_events(self, eventfilter, max_blocks=2000):
        # get a list of filters with different block chunks
        for _filter in self.create_eventFilter_chunks(
            eventfilter=eventfilter, max_blocks=max_blocks
        ):
            entries = self._w3.eth.filter(_filter).get_all_entries()

            # progress if no data found
            if self._progress_callback and len(entries) == 0:
                self._progress_callback(
                    text="no matches from blocks {} to {}".format(
                        _filter["fromBlock"], _filter["toBlock"]
                    ),
                    remaining=eventfilter["toBlock"] - _filter["toBlock"],
                    total=eventfilter["toBlock"] - eventfilter["fromBlock"],
                )

            # filter blockchain data
            for event in entries:
                yield event

    def as_dict(self) -> dict:
        result = dict()
        result["block"] = self.block
        # lower case address to be able to be directly compared
        result["address"] = self.address.lower()

        return result


class erc20(web3wrap):

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = "erc20" if abi_filename == "" else abi_filename
        self._abi_path = "data/abi" if abi_path == "" else abi_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # PROPERTIES
    @property
    def decimals(self) -> int:
        return self._contract.functions.decimals().call()

    def balanceOf(self, address: str) -> float:
        return self._contract.functions.balanceOf(Web3.toChecksumAddress(address)).call(
            block_identifier=self.block
        )

    @property
    def totalSupply(self) -> float:
        return self._contract.functions.totalSupply().call(
            block_identifier=self.block
        ) / (10**self.decimals)

    @property
    def symbol(self) -> str:
        # MKR special: ( has a too large for python int )
        if self.address == "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2":
            return "MKR"
        return self._contract.functions.symbol().call()

    def allowance(self, owner: str, spender: str) -> int:
        return self._contract.functions.allowance(
            Web3.toChecksumAddress(owner), Web3.toChecksumAddress(spender)
        ).call(block_identifier=self.block)

    def as_dict(self) -> dict:
        result = super().as_dict()

        result["decimals"] = self.decimals
        result["totalSupply"] = self.totalSupply
        result["symbol"] = self.symbol

        return result


class erc20_cached(erc20):

    SAVE2FILE = True

    # PROPERTIES
    @property
    def decimals(self) -> int:
        prop_name = "decimals"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def totalSupply(self) -> float:
        prop_name = "totalSupply"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def symbol(self) -> str:
        prop_name = "symbol"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


# COLLECTORs
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
    def _save_topic(self, topic: str, event, data):

        # init result
        itm = self._convert_topic(topic=topic, event=event, data=data)
        # force fee topic
        if topic == "gamma_rebalance":
            topic = "gamma_fee"

        # contract ( itm["address"])
        if not itm["address"].lower() in self._data.keys():
            self._data[itm["address"].lower()] = dict()

        # create topic in contract if not existnt
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

        return itm

    def _convert_topic_decimal(self, topic: str, event, data) -> dict:

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
                    " Unexpected error caching topic ({}) related info from hyp: {}    .transaction hash: {}  -> error: {}".format(
                        topic, itm["address"], itm["transactionHash"], sys.exc_info()[0]
                    )
                )

        # set decimal vars for later use
        decimals_token0 = self._token_helpers[itm["address"].lower()]["decimals_token0"]
        decimals_token1 = self._token_helpers[itm["address"].lower()]["decimals_token1"]
        decimals_contract = self._token_helpers[itm["address"].lower()][
            "decimals_contract"
        ]

        # specific vars
        if topic in ["gamma_deposit", "gamma_withdraw"]:

            itm["sender"] = event.topics[1][-20:].hex()
            itm["to"] = event.topics[2][-20:].hex()
            itm["shares"] = Decimal(data[0]) / Decimal(10**decimals_contract)
            itm["qtty_token0"] = Decimal(data[1]) / Decimal(10**decimals_token0)
            itm["qtty_token1"] = Decimal(data[2]) / Decimal(10**decimals_token1)

        elif topic == "gamma_rebalance":
            itm["tick"] = data[0]
            itm["totalAmount0"] = Decimal(data[1]) / Decimal(10**decimals_token0)
            itm["totalAmount1"] = Decimal(data[2]) / Decimal(10**decimals_token1)
            itm["qtty_token0"] = Decimal(data[3]) / Decimal(10**decimals_token0)
            itm["qtty_token1"] = Decimal(data[4]) / Decimal(10**decimals_token1)

        elif topic == "gamma_zeroBurn":

            itm["fee"] = data[0]
            itm["qtty_token0"] = Decimal(data[1]) / Decimal(10**decimals_token0)
            itm["qtty_token1"] = Decimal(data[2]) / Decimal(10**decimals_token1)

        elif topic in ["gamma_transfer", "arrakis_transfer"]:

            itm["src"] = event.topics[1][-20:].hex()
            itm["dst"] = event.topics[2][-20:].hex()
            itm["qtty"] = Decimal(data[0]) / Decimal(10**decimals_contract)

        elif topic in ["arrakis_deposit", "arrakis_withdraw"]:

            itm["sender"] = data[0] if topic == "arrakis_deposit" else event.address
            itm["to"] = data[0] if topic == "arrakis_withdraw" else event.address
            itm["qtty_token0"] = Decimal(data[2]) / Decimal(
                10**decimals_token0
            )  # amount0
            itm["qtty_token1"] = Decimal(data[3]) / Decimal(
                10**decimals_token1
            )  # amount1
            itm["shares"] = Decimal(data[1]) / Decimal(
                10**decimals_contract
            )  # mintAmount

        elif topic == "arrakis_fee":

            itm["qtty_token0"] = Decimal(data[0]) / Decimal(10**decimals_token0)
            itm["qtty_token1"] = Decimal(data[1]) / Decimal(10**decimals_token1)

        elif topic == "arrakis_rebalance":

            itm["lowerTick"] = Decimal(data[0])
            itm["upperTick"] = Decimal(data[1])
            # data[2] #liquidityBefore
            # data[2] #liquidityAfter
        elif topic in ["gamma_approval"]:
            itm["value"] = Decimal(data[0]) / Decimal(10**decimals_contract)

        elif topic in ["gamma_setFee"]:
            itm["fee"] = Decimal(data[0])

        elif topic == "uniswapv3_collect":
            itm["recipient"] = data[0]
            itm["amount0"] = Decimal(data[1]) / Decimal(10**decimals_token0)  #
            itm["amount1"] = Decimal(data[2]) / Decimal(10**decimals_token1)  #

        return itm

    def _convert_topic_original(self, topic: str, event, data) -> dict:

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
                    " Unexpected error caching topic ({}) related info from hyp: {}    .transaction hash: {}  -> error: {}".format(
                        topic, itm["address"], itm["transactionHash"], sys.exc_info()[0]
                    )
                )

        # set decimal vars for later use
        decimals_token0 = self._token_helpers[itm["address"].lower()]["decimals_token0"]
        decimals_token1 = self._token_helpers[itm["address"].lower()]["decimals_token1"]
        decimals_contract = self._token_helpers[itm["address"].lower()][
            "decimals_contract"
        ]

        # specific vars
        if topic in ["gamma_deposit", "gamma_withdraw"]:

            itm["sender"] = event.topics[1][-20:].hex()
            itm["to"] = event.topics[2][-20:].hex()
            itm["shares"] = int(data[0]) / (10**decimals_contract)
            itm["qtty_token0"] = int(data[1]) / (10**decimals_token0)
            itm["qtty_token1"] = int(data[2]) / (10**decimals_token1)

        elif topic == "gamma_rebalance":
            # rename topic to fee
            # topic = "gamma_fee"
            itm["tick"] = data[0]
            itm["totalAmount0"] = int(data[1]) / (10**decimals_token0)
            itm["totalAmount1"] = int(data[2]) / (10**decimals_token1)
            itm["qtty_token0"] = int(data[3]) / (10**decimals_token0)
            itm["qtty_token1"] = int(data[4]) / (10**decimals_token1)

        elif topic == "gamma_zeroBurn":

            itm["fee"] = data[0]
            itm["qtty_token0"] = int(data[1]) / (10**decimals_token0)
            itm["qtty_token1"] = int(data[2]) / (10**decimals_token1)

        elif topic in ["gamma_transfer", "arrakis_transfer"]:

            itm["src"] = event.topics[1][-20:].hex()
            itm["dst"] = event.topics[2][-20:].hex()
            itm["qtty"] = int(data[0]) / (10**decimals_contract)

        elif topic in ["arrakis_deposit", "arrakis_withdraw"]:

            itm["sender"] = data[0] if topic == "arrakis_deposit" else event.address
            itm["to"] = data[0] if topic == "arrakis_withdraw" else event.address
            itm["qtty_token0"] = int(data[2]) / (10**decimals_token0)  # amount0
            itm["qtty_token1"] = int(data[3]) / (10**decimals_token1)  # amount1
            itm["shares"] = int(data[1]) / (10**decimals_contract)  # mintAmount

        elif topic == "arrakis_fee":

            itm["qtty_token0"] = int(data[0]) / (10**decimals_token0)
            itm["qtty_token1"] = int(data[1]) / (10**decimals_token1)

        elif topic == "arrakis_rebalance":

            itm["lowerTick"] = int(data[0])
            itm["upperTick"] = int(data[1])
            # data[2] #liquidityBefore
            # data[2] #liquidityAfter
        elif topic in ["gamma_approval"]:
            itm["value"] = int(data[0]) / (10**decimals_contract)

        elif topic in ["gamma_setFee"]:
            itm["fee"] = int(data[0])

        elif topic == "uniswapv3_collect":
            itm["recipient"] = data[0]
            itm["amount0"] = int(data[1]) / (10**decimals_token0)  #
            itm["amount1"] = int(data[2]) / (10**decimals_token1)  #

        return itm


# DEXes
class univ3_pool(web3wrap):

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = "univ3_pool" if abi_filename == "" else abi_filename
        self._abi_path = "data/abi/uniswap/v3" if abi_path == "" else abi_path

        self._token0: erc20 = None
        self._token1: erc20 = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # PROPERTIES
    @property
    def factory(self) -> str:
        return self._contract.functions.factory().call(block_identifier=self.block)

    @property
    def fee(self) -> int:
        """The pool's fee in hundredths of a bip, i.e. 1e-6"""
        return self._contract.functions.fee().call(block_identifier=self.block)

    @property
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
        return self._contract.functions.feeGrowthGlobal0X128().call(
            block_identifier=self.block
        )

    @property
    def feeGrowthGlobal1X128(self) -> int:
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
        return self._contract.functions.feeGrowthGlobal1X128().call(
            block_identifier=self.block
        )

    @property
    def liquidity(self) -> int:
        return self._contract.functions.liquidity().call(block_identifier=self.block)

    @property
    def maxLiquidityPerTick(self) -> int:
        return self._contract.functions.maxLiquidityPerTick().call(
            block_identifier=self.block
        )

    def observations(self, input: int):
        return self._contract.functions.observations(input).call(
            block_identifier=self.block
        )

    def observe(self, secondsAgo: int):
        """observe _summary_

        Args:
           secondsAgo (int): _description_

        Returns:
           _type_: tickCumulatives   int56[] :  12731930095582
                   secondsPerLiquidityCumulativeX128s   uint160[] :  242821134689165142944235398318169

        """
        return self._contract.functions.observe(secondsAgo).call(
            block_identifier=self.block
        )

    def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        result = self._contract.functions.positions(position_key).call(
            block_identifier=self.block
        )
        return {
            "liquidity": result[0],
            "feeGrowthInside0LastX128": result[1],
            "feeGrowthInside1LastX128": result[2],
            "tokensOwed0": result[3],
            "tokensOwed1": result[4],
        }

    @property
    def protocolFees(self):
        """The amounts of token0 and token1 that are owed to the protocol

        Returns:
           _type_: token0   uint128 :  0
                   token1   uint128 :  0
        """
        return self._contract.functions.protocolFees().call(block_identifier=self.block)

    @property
    def slot0(self) -> dict:
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  0
                   unlocked   bool :  true
        """
        tmp = self._contract.functions.slot0().call(block_identifier=self.block)
        result = {
            "sqrtPriceX96": tmp[0],
            "tick": tmp[1],
            "observationIndex": tmp[2],
            "observationCardinality": tmp[3],
            "observationCardinalityNext": tmp[4],
            "feeProtocol": tmp[5],
            "unlocked": tmp[6],
        }
        return result

    def snapshotCumulativeInside(self, tickLower: int, tickUpper: int):
        return self._contract.functions.snapshotCumulativeInside(
            tickLower, tickUpper
        ).call(block_identifier=self.block)

    def tickBitmap(self, input: int) -> int:
        return self._contract.functions.tickBitmap(input).call(
            block_identifier=self.block
        )

    @property
    def tickSpacing(self) -> int:
        return self._contract.functions.tickSpacing().call(block_identifier=self.block)

    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0
                       liquidityNet   int128 :  0
                       feeGrowthOutside0X128   uint256 :  0
                       feeGrowthOutside1X128   uint256 :  0
                       tickCumulativeOutside   int56 :  0
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0
                       secondsOutside   uint32 :  0
                       initialized   bool :  false
        """
        result = self._contract.functions.ticks(tick).call(block_identifier=self.block)
        return {
            "liquidityGross": result[0],
            "liquidityNet": result[1],
            "feeGrowthOutside0X128": result[2],
            "feeGrowthOutside1X128": result[3],
            "tickCumulativeOutside": result[4],
            "secondsPerLiquidityOutsideX128": result[5],
            "secondsOutside": result[6],
            "initialized": result[7],
        }

    @property
    def token0(self) -> erc20:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 == None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        """The second of the two tokens of the pool, sorted by address_

        Returns:
           erc20:
        """
        if self._token1 == None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    # write function without state change ( not wrkin)
    def collect(
        self, recipient, tickLower, tickUpper, amount0Requested, amount1Requested, owner
    ):
        return self._contract.functions.collect(
            recipient, tickLower, tickUpper, amount0Requested, amount1Requested
        ).call({"from": owner})

    # CUSTOM PROPERTIES
    @property
    def block(self) -> int:
        return self._block

    @block.setter
    def block(self, value: int):
        # set block
        self._block = value
        self.token0.block = value
        self.token1.block = value

    # CUSTOM FUNCTIONS
    def position(self, ownerAddress: str, tickLower: int, tickUpper: int) -> dict:
        """

        Returns:
           dict:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        return self.positions(
            univ3_formulas.get_positionKey(
                ownerAddress=ownerAddress,
                tickLower=tickLower,
                tickUpper=tickUpper,
            )
        )

    def get_qtty_depoloyed(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Args:
           ownerAddress (str):
           tickUpper (int):
           tickLower (int):
           inDecimal (bool): return result in a decimal format?

        Returns:
           dict: {
                   "qtty_token0":0,        (float) # quantity of token 0 deployed in dex
                   "qtty_token1":0,        (float) # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,   (float) # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,   (float) # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """

        result = {
            "qtty_token0": 0,  # quantity of token 0 deployed in dex
            "qtty_token1": 0,  # quantity of token 1 deployed in dex
            "fees_owed_token0": 0,  # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
            "fees_owed_token1": 0,  # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )
        # get slot data
        slot0 = self.slot0

        # get current tick from slot
        tickCurrent = slot0["tick"]
        sqrtRatioX96 = slot0["sqrtPriceX96"]
        sqrtRatioAX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickLower)
        sqrtRatioBX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickUpper)
        # calc quantity from liquidity
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.LiquidityAmounts.getAmountsForLiquidity(
            sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, pos["liquidity"]
        )

        # add owed tokens
        result["fees_owed_token0"] = pos["tokensOwed0"]
        result["fees_owed_token1"] = pos["tokensOwed1"]

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            decimals_token0 = self.token0.decimals
            decimals_token1 = self.token1.decimals

            result["qtty_token0"] /= 10**decimals_token0
            result["qtty_token1"] /= 10**decimals_token1
            result["fees_owed_token0"] /= 10**decimals_token0
            result["fees_owed_token1"] /= 10**decimals_token1

        # return result
        return result

    def get_fees_uncollected(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Args:
            ownerAddress (str):
            tickUpper (int):
            tickLower (int):
            inDecimal (bool): return result in a decimal format?

        Returns:
            dict: {
                    "qtty_token0":0,   (float)     # quantity of uncollected token 0
                    "qtty_token1":0,   (float)     # quantity of uncollected token 1
                }
        """

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # get ticks
        tickCurrent = self.slot0["tick"]
        ticks_lower = self.ticks(tickLower)
        ticks_upper = self.ticks(tickUpper)

        # calc token0 uncollected fees
        # feeGrowthOutside0X128_lower = ticks_lower["feeGrowthOutside0X128"]
        # feeGrowthOutside0X128_upper = ticks_upper["feeGrowthOutside0X128"]
        # feeGrowthInside0LastX128 = pos["feeGrowthInside0LastX128"]
        # # add to result
        # result["qtty_token0"] = univ3_formulas.get_uncollected_fees(
        #     feeGrowthGlobal=self.feeGrowthGlobal0X128,
        #     feeGrowthOutsideLower=feeGrowthOutside0X128_lower,
        #     feeGrowthOutsideUpper=feeGrowthOutside0X128_upper,
        #     feeGrowthInsideLast=feeGrowthInside0LastX128,
        #     tickCurrent=tickCurrent,
        #     liquidity=pos["liquidity"],
        #     tickLower=tickLower,
        #     tickUpper=tickUpper,
        # )

        # calc token1 uncollected fees
        # feeGrowthOutside1X128_lower = ticks_lower["feeGrowthOutside1X128"]
        # feeGrowthOutside1X128_upper = ticks_upper["feeGrowthOutside1X128"]
        # feeGrowthInside1LastX128 = pos["feeGrowthInside1LastX128"]
        # #       add fee1 to result
        # result["qtty_token1"] = univ3_formulas.get_uncollected_fees(
        #     feeGrowthGlobal=self.feeGrowthGlobal1X128,
        #     feeGrowthOutsideLower=feeGrowthOutside1X128_lower,
        #     feeGrowthOutsideUpper=feeGrowthOutside1X128_upper,
        #     feeGrowthInsideLast=feeGrowthInside1LastX128,
        #     tickCurrent=tickCurrent,
        #     liquidity=pos["liquidity"],
        #     tickLower=tickLower,
        #     tickUpper=tickUpper,
        # )

        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.get_uncollected_fees_vGammawire(
            fee_growth_global_0=self.feeGrowthGlobal0X128,
            fee_growth_global_1=self.feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower,
            tick_upper=tickUpper,
            fee_growth_outside_0_lower=ticks_lower["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=ticks_lower["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=ticks_upper["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=ticks_upper["feeGrowthOutside1X128"],
            liquidity=pos["liquidity"],
            fee_growth_inside_last_0=pos["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pos["feeGrowthInside1LastX128"],
        )

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            decimals_token0 = self.token0.decimals
            decimals_token1 = self.token1.decimals

            result["qtty_token0"] /= 10**decimals_token0
            result["qtty_token1"] /= 10**decimals_token1

        # return result
        return result

    def as_dict(self) -> dict:
        result = super().as_dict()

        # result["factory"] = self.factory
        result["fee"] = self.fee
        # result["feeGrowthGlobal0X128"] = str(self.feeGrowthGlobal0X128)
        # result["feeGrowthGlobal1X128"] = str(self.feeGrowthGlobal1X128)
        # result["liquidity"] = str(self.liquidity)
        # result["maxLiquidityPerTick"] = str(self.maxLiquidityPerTick)
        # result["protocolFees"] = self.protocolFees
        # result["slot0"] = self.slot0
        # result["tickSpacing"] = self.tickSpacing
        result["token0"] = self.token0.as_dict()
        result["token1"] = self.token1.as_dict()

        return result


class univ3_pool_cached(univ3_pool):

    SAVE2FILE = True

    # PROPERTIES
    @property
    def factory(self) -> str:
        prop_name = "factory"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def fee(self) -> int:
        """The pool's fee in hundredths of a bip, i.e. 1e-6"""
        prop_name = "fee"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
        prop_name = "feeGrowthGlobal0X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeGrowthGlobal1X128(self) -> int:
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
        prop_name = "feeGrowthGlobal1X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def liquidity(self) -> int:
        prop_name = "liquidity"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxLiquidityPerTick(self) -> int:
        prop_name = "maxLiquidityPerTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def protocolFees(self):
        """The amounts of token0 and token1 that are owed to the protocol

        Returns:
           _type_: token0   uint128 :  0
                   token1   uint128 :  0
        """
        prop_name = "protocolFees"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def slot0(self) -> dict:
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  0
                   unlocked   bool :  true
        """
        prop_name = "slot0"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 == None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        """The second of the two tokens of the pool, sorted by address_

        Returns:
           erc20:
        """
        if self._token1 == None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1


class quickswapv3_dataStorageOperator(web3wrap):

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = (
            "dataStorageOperator" if abi_filename == "" else abi_filename
        )
        self._abi_path = "data/abi/quickswap/v3" if abi_path == "" else abi_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # TODO: all

    @property
    def feeConfig(self) -> dict:
        return self._contract.functions.feeConfig().call(block_identifier=self.block)


class quickswapv3_dataStorageOperator_cached(quickswapv3_dataStorageOperator):
    @property
    def feeConfig(self) -> dict:
        prop_name = "feeConfig"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class quickswapv3_pool(web3wrap):

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):

        self._abi_filename = "quickv3pool" if abi_filename == "" else abi_filename
        self._abi_path = "data/abi/quickswap/v3" if abi_path == "" else abi_path

        self._token0: erc20 = None
        self._token1: erc20 = None

        self._dataStorage: quickswapv3_dataStorageOperator = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # https://polygonscan.com/address/0xae81fac689a1b4b1e06e7ef4a2ab4cd8ac0a087d#readContract

    # PROPERTIES

    @property
    def activeIncentive(self) -> str:
        """activeIncentive

        Returns:
            str: address
        """
        return self._contract.functions.activeIncentive().call(
            block_identifier=self.block
        )

    @property
    def dataStorageOperator(self) -> quickswapv3_dataStorageOperator:
        """ """
        if self._dataStorage == None:
            self._dataStorage = quickswapv3_dataStorageOperator(
                address=self._contract.functions.dataStorageOperator().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._dataStorage

    @property
    def factory(self) -> str:
        return self._contract.functions.factory().call(block_identifier=self.block)

    @property
    def getInnerCumulatives(self, bottomTick: int, topTick: int) -> dict:
        return self._contract.functions.getInnerCumulatives(bottomTick, topTick).call(
            block_identifier=self.block
        )

    @property
    def getTimepoints(self, secondsAgo: int) -> dict:
        return self._contract.functions.getTimepoints(secondsAgo).call(
            block_identifier=self.block
        )

    @property
    def globalState(self) -> dict:
        """

        Returns:
           dict:   price  uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   fee   uint16 :  198
                   timepointIndex   uint16 :  300
                   communityFeeToken0   uint8 :  300
                   communityFeeToken1   uint8 :  0
                   unlocked   bool :  true
        """
        tmp = self._contract.functions.globalState().call(block_identifier=self.block)
        result = {
            "sqrtPriceX96": tmp[0],
            "tick": tmp[1],
            "fee": tmp[2],
            "timepointIndex": tmp[3],
            "communityFeeToken0": tmp[4],
            "communityFeeToken1": tmp[5],
            "unlocked": tmp[6],
        }
        return result

    @property
    def liquidity(self) -> int:
        return self._contract.functions.liquidity().call(block_identifier=self.block)

    @property
    def liquidityCooldown(self) -> int:
        return self._contract.functions.liquidityCooldown().call(
            block_identifier=self.block
        )

    @property
    def maxLiquidityPerTick(self) -> int:
        return self._contract.functions.maxLiquidityPerTick().call(
            block_identifier=self.block
        )

    def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   lastLiquidityAddTimestamp
                   innerFeeGrowth0Token   uint256 :  (feeGrowthInside0LastX128)
                   innerFeeGrowth1Token   uint256 :  (feeGrowthInside1LastX128)
                   fees0   uint128 :  0  (tokensOwed0)
                   fees1   uint128 :  0  ( tokensOwed1)
        """
        result = self._contract.functions.positions(position_key).call(
            block_identifier=self.block
        )
        return {
            "liquidity": result[0],
            "lastLiquidityAddTimestamp": result[1],
            "feeGrowthInside0LastX128": result[2],
            "feeGrowthInside1LastX128": result[3],
            "tokensOwed0": result[4],
            "tokensOwed1": result[5],
        }

    @property
    def tickSpacing(self) -> int:
        return self._contract.functions.tickSpacing().call(block_identifier=self.block)

    def tickTable(self, value: int) -> int:
        return self._contract.functions.tickTable(value).call(
            block_identifier=self.block
        )

    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0        liquidityTotal
                       liquidityNet   int128 :  0           liquidityDelta
                       feeGrowthOutside0X128   uint256 :  0 outerFeeGrowth0Token
                       feeGrowthOutside1X128   uint256 :  0 outerFeeGrowth1Token
                       tickCumulativeOutside   int56 :  0   outerTickCumulative
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0    outerSecondsPerLiquidity
                       secondsOutside   uint32 :  0         outerSecondsSpent
                       initialized   bool :  false          initialized
        """
        result = self._contract.functions.ticks(tick).call(block_identifier=self.block)
        return {
            "liquidityGross": result[0],
            "liquidityNet": result[1],
            "feeGrowthOutside0X128": result[2],
            "feeGrowthOutside1X128": result[3],
            "tickCumulativeOutside": result[4],
            "secondsPerLiquidityOutsideX128": result[5],
            "secondsOutside": result[6],
            "initialized": result[7],
        }

    def timepoints(self, index: int) -> dict:
        #   initialized bool, blockTimestamp uint32, tickCumulative int56, secondsPerLiquidityCumulative uint160, volatilityCumulative uint88, averageTick int24, volumePerLiquidityCumulative uint144
        result = self._contract.functions.timepoints(index).call(
            block_identifier=self.block
        )

    @property
    def token0(self) -> erc20:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 == None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 == None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
        return self._contract.functions.totalFeeGrowth0Token().call(
            block_identifier=self.block
        )

    @property
    def feeGrowthGlobal1X128(self) -> int:
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
        return self._contract.functions.totalFeeGrowth1Token().call(
            block_identifier=self.block
        )

    # CUSTOM PROPERTIES
    @property
    def block(self) -> int:
        return self._block

    @block.setter
    def block(self, value: int):
        # set block
        self._block = value
        self.token0.block = value
        self.token1.block = value

    # CUSTOM FUNCTIONS
    def position(self, ownerAddress: str, tickLower: int, tickUpper: int) -> dict:
        """

        Returns:
           dict:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        key = univ3_formulas.get_positionKey_algebra(
            ownerAddress=ownerAddress,
            tickLower=tickLower,
            tickUpper=tickUpper,
        )
        return self.positions(key)

    def get_qtty_depoloyed(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Args:
           ownerAddress (str):
           tickUpper (int):
           tickLower (int):
           inDecimal (bool): return result in a decimal format?

        Returns:
           dict: {
                   "qtty_token0":0,        (float) # quantity of token 0 deployed in dex
                   "qtty_token1":0,        (float) # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,   (float) # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,   (float) # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """

        result = {
            "qtty_token0": 0,  # quantity of token 0 deployed in dex
            "qtty_token1": 0,  # quantity of token 1 deployed in dex
            "fees_owed_token0": 0,  # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
            "fees_owed_token1": 0,  # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )
        # get slot data
        slot0 = self.globalState

        # get current tick from slot
        tickCurrent = slot0["tick"]
        sqrtRatioX96 = slot0["sqrtPriceX96"]
        sqrtRatioAX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickLower)
        sqrtRatioBX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickUpper)
        # calc quantity from liquidity
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.LiquidityAmounts.getAmountsForLiquidity(
            sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, pos["liquidity"]
        )

        # add owed tokens
        result["fees_owed_token0"] = pos["tokensOwed0"]
        result["fees_owed_token1"] = pos["tokensOwed1"]

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            decimals_token0 = self.token0.decimals
            decimals_token1 = self.token1.decimals

            result["qtty_token0"] /= 10**decimals_token0
            result["qtty_token1"] /= 10**decimals_token1
            result["fees_owed_token0"] /= 10**decimals_token0
            result["fees_owed_token1"] /= 10**decimals_token1

        # return result
        return result

    def get_fees_uncollected(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Args:
            ownerAddress (str):
            tickUpper (int):
            tickLower (int):
            inDecimal (bool): return result in a decimal format?

        Returns:
            dict: {
                    "qtty_token0":0,   (float)     # quantity of uncollected token 0
                    "qtty_token1":0,   (float)     # quantity of uncollected token 1
                }
        """

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # get ticks
        tickCurrent = self.globalState["tick"]
        ticks_lower = self.ticks(tickLower)
        ticks_upper = self.ticks(tickUpper)

        # calc token0 uncollected fees
        # feeGrowthOutside0X128_lower = ticks_lower["feeGrowthOutside0X128"]
        # feeGrowthOutside0X128_upper = ticks_upper["feeGrowthOutside0X128"]
        # feeGrowthInside0LastX128 = pos["feeGrowthInside0LastX128"]

        # calc token1 uncollected fees
        # feeGrowthOutside1X128_lower = ticks_lower["feeGrowthOutside1X128"]
        # feeGrowthOutside1X128_upper = ticks_upper["feeGrowthOutside1X128"]
        # feeGrowthInside1LastX128 = pos["feeGrowthInside1LastX128"]

        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.get_uncollected_fees_vGammawire(
            fee_growth_global_0=self.feeGrowthGlobal0X128,
            fee_growth_global_1=self.feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower,
            tick_upper=tickUpper,
            fee_growth_outside_0_lower=ticks_lower["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=ticks_lower["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=ticks_upper["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=ticks_upper["feeGrowthOutside1X128"],
            liquidity=pos["liquidity"],
            fee_growth_inside_last_0=pos["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pos["feeGrowthInside1LastX128"],
        )

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            decimals_token0 = self.token0.decimals
            decimals_token1 = self.token1.decimals

            result["qtty_token0"] /= 10**decimals_token0
            result["qtty_token1"] /= 10**decimals_token1

        # return result
        return result

    def get_fees_uncollected_original(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        # DO NOT USE

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # get ticks
        tickCurrent = self.globalState["tick"]
        ticks_lower = self.ticks(tickLower)
        ticks_upper = self.ticks(tickUpper)

        # calc token0 uncollected fees
        feeGrowthOutside0X128_lower = ticks_lower["feeGrowthOutside0X128"]
        feeGrowthOutside0X128_upper = ticks_upper["feeGrowthOutside0X128"]
        feeGrowthInside0LastX128 = pos["feeGrowthInside0LastX128"]
        # add to result
        result["qtty_token0"] = univ3_formulas.get_uncollected_fees(
            feeGrowthGlobal=self.feeGrowthGlobal0X128,
            feeGrowthOutsideLower=feeGrowthOutside0X128_lower,
            feeGrowthOutsideUpper=feeGrowthOutside0X128_upper,
            feeGrowthInsideLast=feeGrowthInside0LastX128,
            tickCurrent=tickCurrent,
            liquidity=pos["liquidity"],
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # calc token1 uncollected fees
        feeGrowthOutside1X128_lower = ticks_lower["feeGrowthOutside1X128"]
        feeGrowthOutside1X128_upper = ticks_upper["feeGrowthOutside1X128"]
        feeGrowthInside1LastX128 = pos["feeGrowthInside1LastX128"]
        #       add fee1 to result
        result["qtty_token1"] = univ3_formulas.get_uncollected_fees(
            feeGrowthGlobal=self.feeGrowthGlobal1X128,
            feeGrowthOutsideLower=feeGrowthOutside1X128_lower,
            feeGrowthOutsideUpper=feeGrowthOutside1X128_upper,
            feeGrowthInsideLast=feeGrowthInside1LastX128,
            tickCurrent=tickCurrent,
            liquidity=pos["liquidity"],
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            decimals_token0 = self.token0.decimals
            decimals_token1 = self.token1.decimals

            result["qtty_token0"] /= 10**decimals_token0
            result["qtty_token1"] /= 10**decimals_token1

        # return result
        return result

    def as_dict(self) -> dict:
        result = super().as_dict()

        result["fee"] = self.globalState["fee"]

        result["token0"] = self.token0.as_dict()
        result["token1"] = self.token1.as_dict()

        return result


class quickswapv3_pool_cached(quickswapv3_pool):

    SAVE2FILE = True

    # PROPERTIES

    @property
    def activeIncentive(self) -> str:
        prop_name = "activeIncentive"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def dataStorageOperator(self) -> quickswapv3_dataStorageOperator:
        """ """
        if self._dataStorage == None:
            self._dataStorage = quickswapv3_dataStorageOperator_cached(
                address=self._contract.functions.dataStorageOperator().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._dataStorage

    @property
    def factory(self) -> str:
        prop_name = "factory"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def globalState(self) -> dict:
        prop_name = "globalState"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def liquidity(self) -> int:
        prop_name = "liquidity"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def liquidityCooldown(self) -> int:
        prop_name = "liquidityCooldown"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxLiquidityPerTick(self) -> int:
        prop_name = "maxLiquidityPerTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20_cached:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 == None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20_cached:
        if self._token1 == None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def feeGrowthGlobal0X128(self) -> int:
        prop_name = "feeGrowthGlobal0X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeGrowthGlobal1X128(self) -> int:
        prop_name = "feeGrowthGlobal1X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


# PROTOCOLS
class gamma_hypervisor(erc20):

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = "hypervisor" if abi_filename == "" else abi_filename
        self._abi_path = "data/abi/gamma" if abi_path == "" else abi_path

        self._pool: univ3_pool = None
        self._token0: erc20 = None
        self._token1: erc20 = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # PROPERTIES
    @property
    def baseUpper(self):
        return self._contract.functions.baseUpper().call(block_identifier=self.block)

    @property
    def baseLower(self):
        return self._contract.functions.baseLower().call(block_identifier=self.block)

    @property
    def currentTick(self) -> int:
        return self._contract.functions.currentTick().call(block_identifier=self.block)

    @property
    def deposit0Max(self) -> float:
        return self._contract.functions.deposit0Max().call(block_identifier=self.block)

    @property
    def deposit1Max(self) -> float:
        return self._contract.functions.deposit1Max().call(block_identifier=self.block)

    @property
    def directDeposit(self) -> bool:
        return self._contract.functions.directDeposit().call(
            block_identifier=self.block
        )

    @property
    def fee(self) -> int:
        return self._contract.functions.fee().call(block_identifier=self.block)

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   28.7141300490401993
               amount0     72.329994
               amount1     56.5062023318300677907
               }
        """
        tmp = self._contract.functions.getBasePosition().call(
            block_identifier=self.block
        )
        result = {
            "liquidity": tmp[0],
            "amount0": tmp[1] / (10**self.token0.decimals),
            "amount1": tmp[2] / (10**self.token1.decimals),
        }
        return result

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   28.7141300490401993
               amount0     72.329994
               amount1     56.5062023318300677907
               }
        """
        tmp = self._contract.functions.getLimitPosition().call(
            block_identifier=self.block
        )
        result = {
            "liquidity": tmp[0],
            "amount0": tmp[1] / (10**self.token0.decimals),
            "amount1": tmp[2] / (10**self.token1.decimals),
        }
        return result

    @property
    def getTotalAmounts(self) -> dict:
        """

        Returns:
           _type_: total0   2.902086313
                   total1  56.5062023318300678136
        """
        tmp = self._contract.functions.getTotalAmounts().call(
            block_identifier=self.block
        )
        result = {
            "total0": tmp[0] / (10**self.token0.decimals),
            "total1": tmp[1] / (10**self.token1.decimals),
        }
        return result

    @property
    def limitLower(self):
        return self._contract.functions.limitLower().call(block_identifier=self.block)

    @property
    def limitUpper(self):
        return self._contract.functions.limitUpper().call(block_identifier=self.block)

    @property
    def maxTotalSupply(self) -> float:
        return self._contract.functions.maxTotalSupply().call(
            block_identifier=self.block
        ) / (10**self.decimals)

    @property
    def name(self) -> str:
        return self._contract.functions.name().call(block_identifier=self.block)

    def nonces(self, owner: str):
        return self._contract.functions.nonces()(Web3.toChecksumAddress(owner)).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pool(self) -> univ3_pool:
        if self._pool == None:
            self._pool = univ3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        return self._contract.functions.tickSpacing().call(block_identifier=self.block)

    @property
    def token0(self) -> erc20:
        if self._token0 == None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 == None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def block(self) -> int:
        return self._block

    @block.setter
    def block(self, value):
        self._block = value
        self.pool.block = value
        self.token0.block = value
        self.token1.block = value

    # CUSTOM FUNCTIONS
    def get_all_events(self):
        filters = [
            event.createFilter(fromBlock=self.block)
            for event in self.contract.events
            if issubclass(event, ContractEvent)
        ]
        return filters

    def get_qtty_depoloyed(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Returns:
           dict: {
                   "qtty_token0":0,         # quantity of token 0 deployed in dex
                   "qtty_token1":0,         # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,    # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,    # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """
        # positions
        base = self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            inDecimal=inDecimal,
        )
        limit = self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            inDecimal=inDecimal,
        )

        # add up
        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    def get_fees_uncollected(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Returns:
            dict: {
                    "qtty_token0":0,  # quantity of uncollected token 0
                    "qtty_token1":0,  # quantity of uncollected token 1
                }
        """
        # positions
        base = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            inDecimal=inDecimal,
        )
        limit = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            inDecimal=inDecimal,
        )

        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    def get_tvl(self) -> dict:
        """get total value locked of both positions
           TVL = deployed + parked + owed

        Returns:
           dict: {" tvl_token0": ,      (float) sum of below's token 0 (total)
                   "tvl_token1": ,      (float)
                   "deployed_token0": , (float) quantity of token 0 LPing
                   "deployed_token1": , (float)
                   "fees_owed_token0": ,(float) fees owed to the position by dex
                   "fees_owed_token1": ,(float)
                   "parked_token0": ,   (float) quantity of token 0 parked at contract (not deployed)
                   "parked_token1": ,   (float)
                   }
        """
        # get deployed fees as int
        deployed = self.get_qtty_depoloyed(inDecimal=False)

        result = dict()

        # get parked tokens as int
        result["parked_token0"] = self.pool.token0.balanceOf(self.address)
        result["parked_token1"] = self.pool.token1.balanceOf(self.address)

        result["deployed_token0"] = deployed["qtty_token0"]
        result["deployed_token1"] = deployed["qtty_token1"]
        result["fees_owed_token0"] = deployed["fees_owed_token0"]
        result["fees_owed_token1"] = deployed["fees_owed_token1"]

        # sumup
        result["tvl_token0"] = (
            result["deployed_token0"]
            + result["fees_owed_token0"]
            + result["parked_token0"]
        )
        result["tvl_token1"] = (
            result["deployed_token1"]
            + result["fees_owed_token1"]
            + result["parked_token1"]
        )

        # transform everythin to deicmal
        for key in result.keys():
            if "token0" in key:
                result[key] /= 10**self.token0.decimals
            elif "token1" in key:
                result[key] /= 10**self.token1.decimals
            else:
                raise ValueError("Cant convert '{}' field to decimal".format(key))

        return result

    def as_dict(self) -> dict:
        result = super().as_dict()

        result["baseLower"] = self.baseLower
        result["baseUpper"] = self.baseUpper
        result["currentTick"] = self.currentTick

        # result["deposit0Max"] = self.deposit0Max
        # result["deposit1Max"] = self.deposit1Max

        # result["directDeposit"] = self.directDeposit  # not working
        result["fee"] = self.fee

        # result["basePosition"] = self.getBasePosition
        # result["limitPosition"] = self.getLimitPosition

        result["totalAmounts"] = self.getTotalAmounts
        result["limitLower"] = self.limitLower
        result["limitUpper"] = self.limitUpper
        result["maxTotalSupply"] = self.maxTotalSupply
        result["name"] = self.name

        result["pool"] = self.pool.as_dict()

        # result["tickSpacing"] = self.tickSpacing
        # result["token0"] = self.token0.as_dict()
        # result["token1"] = self.token1.as_dict()

        result["tvl"] = self.get_tvl()
        result["qtty_depoloyed"] = self.get_qtty_depoloyed()
        result["fees_uncollected"] = self.get_fees_uncollected()

        return result


class gamma_hypervisor_quickswap(gamma_hypervisor):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = (
            "algebra_hypervisor" if abi_filename == "" else abi_filename
        )
        self._abi_path = "data/abi/gamma" if abi_path == "" else abi_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    @property
    def pool(self) -> quickswapv3_pool:
        if self._pool == None:
            self._pool = quickswapv3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_cached(gamma_hypervisor):

    SAVE2FILE = True

    # PROPERTIES
    @property
    def baseLower(self):
        prop_name = "baseLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def baseUpper(self):
        prop_name = "baseUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def currentTick(self) -> int:
        prop_name = "currentTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit0Max(self) -> float:
        prop_name = "deposit0Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit1Max(self) -> float:
        prop_name = "deposit1Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def directDeposit(self) -> bool:
        prop_name = "directDeposit"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def fee(self) -> int:
        prop_name = "fee"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   28.7141300490401993
               amount0     72.329994
               amount1     56.5062023318300677907
               }
        """
        prop_name = "getBasePosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   28.7141300490401993
               amount0     72.329994
               amount1     56.5062023318300677907
               }
        """
        prop_name = "getLimitPosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getTotalAmounts(self) -> dict:
        """_

        Returns:
           _type_: total0   2.902086313
                   total1  56.5062023318300678136
        """
        prop_name = "getTotalAmounts"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )

        return result

    @property
    def limitLower(self):
        prop_name = "limitLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def limitUpper(self):
        prop_name = "limitUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxTotalSupply(self) -> int:
        prop_name = "maxTotalSupply"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def name(self) -> str:
        prop_name = "name"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def owner(self) -> str:
        prop_name = "owner"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def pool(self) -> str:
        if self._pool == None:
            self._pool = univ3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20:
        if self._token0 == None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 == None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def witelistedAddress(self) -> str:
        prop_name = "witelistedAddress"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class gamma_hypervisor_quickswap_cached(gamma_hypervisor_quickswap):

    SAVE2FILE = True

    # PROPERTIES
    @property
    def baseLower(self):
        prop_name = "baseLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def baseUpper(self):
        prop_name = "baseUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def currentTick(self) -> int:
        prop_name = "currentTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit0Max(self) -> float:
        prop_name = "deposit0Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit1Max(self) -> float:
        prop_name = "deposit1Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def directDeposit(self) -> bool:
        prop_name = "directDeposit"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def fee(self) -> int:
        prop_name = "fee"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   28.7141300490401993
               amount0     72.329994
               amount1     56.5062023318300677907
               }
        """
        prop_name = "getBasePosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   28.7141300490401993
               amount0     72.329994
               amount1     56.5062023318300677907
               }
        """
        prop_name = "getLimitPosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getTotalAmounts(self) -> dict:
        """_

        Returns:
           _type_: total0   2.902086313
                   total1  56.5062023318300678136
        """
        prop_name = "getTotalAmounts"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )

        return result

    @property
    def limitLower(self):
        prop_name = "limitLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def limitUpper(self):
        prop_name = "limitUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxTotalSupply(self) -> int:
        prop_name = "maxTotalSupply"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def name(self) -> str:
        prop_name = "name"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def owner(self) -> str:
        prop_name = "owner"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def pool(self) -> str:
        if self._pool == None:
            self._pool = univ3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20:
        if self._token0 == None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 == None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def witelistedAddress(self) -> str:
        prop_name = "witelistedAddress"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class gamma_hypervisor_registry(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = "registry" if abi_filename == "" else abi_filename
        self._abi_path = "data/abi/gamma/ethereum" if abi_path == "" else abi_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # TODO: implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {
        "ethereum": ["0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"],  # address:index
        "polygon": ["0xa9782a2c9c3fb83937f14cdfac9a6d23946c9255"],
        "optimism": ["0xc7722271281Aa6D5D027fC9B21989BE99424834f"],
        "arbitrum": ["0x38f81e638f9e268e8417F2Ff76C270597fa077A0"],
    }

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value

        Returns:
            int: positions of hypervisors in registry
        """
        return self._contract.functions.counter().call(block_identifier=self.block)

    def hypeByIndex(self, index: int) -> str:
        return self._contract.functions.hypeByIndex(index).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def registry(self, index: int) -> str:
        return self._contract.functions.registry(index).call(
            block_identifier=self.block
        )

    def registryMap(self, address: str) -> int:
        return self._contract.functions.registryMap(
            Web3.toChecksumAddress(address)
        ).call(block_identifier=self.block)

    # CUSTOM FUNCTIONS
    def get_hypervisors_generator(self) -> gamma_hypervisor:
        """Retrieve hypervisors from registry

        Returns:
           gamma_hypervisor
        """
        total_qtty = self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                hypervisor_id = self.registry(index=i)

                # build hypervisor
                hypervisor = gamma_hypervisor(
                    address=hypervisor_id,
                    network=self._network,
                    block=self.block,
                )
                # check this is actually an hypervisor (erroneous addresses exist like "ethereum":{"0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"})
                hypervisor.getTotalAmounts

                # return correct hypervisor
                yield hypervisor
            except:
                logging.getLogger(__name__).warning(
                    " Hypervisor registry returned the address {} and may not be an hypervisor ( at web3 chain id: {} )".format(
                        hypervisor_id, self._chain_id
                    )
                )

    def get_hypervisors_addresses(self) -> list[str]:
        """Retrieve hypervisors all addresses from registry

        Returns:
           list of addresses
        """
        # get list of erroneous addresses
        err_adrs = list()
        for dex, value in HYPERVISOR_REGISTRIES.items():
            for network, address in value.items():
                if network in self.__blacklist_addresses:
                    if address.lower() == self.address.lower():
                        err_adrs.extend(self.__blacklist_addresses[network])

        total_qtty = self.counter + 1  # index positions ini=0 end=counter

        result = list()
        for i in range(total_qtty):
            try:
                tmp = self.registry(index=i)
                if tmp not in err_adrs:
                    result.append(tmp)
            except:
                # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
                pass

        return result


class arrakis_hypervisor(erc20):

    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):

        self._abi_filename = "gunipool" if abi_filename == "" else abi_filename
        self._abi_path = "data/abi/arrakis" if abi_path == "" else abi_path

        self._pool: univ3_pool = None

        self._token0: erc20 = None
        self._token1: erc20 = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # PROPERTIES
    @property
    def gelatoBalance0(self) -> int:
        return self._contract.functions.gelatoBalance0().call(
            block_identifier=self.block
        )

    @property
    def gelatoBalance1(self) -> int:
        return self._contract.functions.gelatoBalance1().call(
            block_identifier=self.block
        )

    @property
    def gelatoFeeBPS(self) -> int:
        return self._contract.functions.gelatoFeeBPS().call(block_identifier=self.block)

    @property
    def gelatoRebalanceBPS(self) -> int:
        return self._contract.functions.gelatoRebalanceBPS().call(
            block_identifier=self.block
        )

    @property
    def gelatoSlippageBPS(self) -> int:
        return self._contract.functions.gelatoSlippageBPS().call(
            block_identifier=self.block
        )

    @property
    def gelatoSlippageInterval(self) -> int:
        return self._contract.functions.gelatoSlippageInterval().call(
            block_identifier=self.block
        )

    @property
    def gelatoWithdrawBPS(self) -> int:
        return self._contract.functions.gelatoWithdrawBPS().call(
            block_identifier=self.block
        )

    def getMintAmounts(self, amount0Max, amount1Max) -> dict:
        """
        Args:
           amount0Max (_type_):
           amount1Max (_type_):

        Returns:
           dict: amount0 uint256, amount1 uint256, mintAmount uint256
        """

        tmp = self._contract.functions.getMintAmounts(amount0Max, amount1Max).call(
            block_identifier=self.block
        )
        return {"amount0": tmp[0], "amount1": tmp[1], "mintAmount": tmp[2]}

    @property
    def getPositionID(self) -> str:
        return self._contract.functions.getPositionID().call(
            block_identifier=self.block
        )

    @property
    def getUnderlyingBalances(self) -> dict:
        """getUnderlyingBalances _summary_

        Returns:
           dict: amount0Current: current total underlying balance of token0
                   amount1Current: current total underlying balance of token1
        """
        tmp = self._contract.functions.getUnderlyingBalances().call(
            block_identifier=self.block
        )
        return {
            "amount0Current": tmp[0],
            "amount1Current": tmp[1],
        }

    def getUnderlyingBalancesAtPrice(self, sqrtRatioX96) -> dict:
        """

        Returns:
           dict: amount0Current: current total underlying balance of token0 at price
                 amount1Current: current total underlying balance of token1 at price
        """
        tmp = self._contract.functions.getUnderlyingBalancesAtPrice(sqrtRatioX96).call(
            block_identifier=self.block
        )
        return {
            "amount0Current": tmp[0],
            "amount1Current": tmp[1],
        }

    @property
    def lowerTick(self) -> int:
        return self._contract.functions.lowerTick().call(block_identifier=self.block)

    @property
    def manager(self) -> str:
        return self._contract.functions.manager().call(block_identifier=self.block)

    @property
    def managerBalance0(self) -> int:
        return self._contract.functions.managerBalance0().call(
            block_identifier=self.block
        )

    @property
    def managerBalance1(self) -> int:
        return self._contract.functions.managerBalance1().call(
            block_identifier=self.block
        )

    @property
    def managerFeeBPS(self) -> int:
        return self._contract.functions.managerFeeBPS().call(
            block_identifier=self.block
        )

    @property
    def managerTreasury(self) -> str:
        return self._contract.functions.managerTreasury().call(
            block_identifier=self.block
        )

    @property
    def name(self) -> str:
        return self._contract.functions.name().call(block_identifier=self.block)

    @property
    def pool(self) -> str:
        if self._pool == None:
            self._pool = univ3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def token0(self) -> erc20:
        if self._token0 == None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 == None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def upperTick(self) -> int:
        return self._contract.functions.upperTick().call(block_identifier=self.block)

    @property
    def version(self) -> str:
        return self._contract.functions.version().call(block_identifier=self.block)

    # CUSTOM PROPERTIES
    @property
    def block(self):
        """ """
        return self._block

    @block.setter
    def block(self, value):
        self._block = value
        self.pool.block = value
        self.token0.block = value
        self.token1.block = value

    # CUSTOM FUNCTIONS
    def get_qtty_depoloyed(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Returns:
           dict: {
                   "qtty_token0":0,         # quantity of token 0 deployed in dex
                   "qtty_token1":0,         # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,    # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,    # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """
        # position
        return self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.upperTick,
            tickLower=self.lowerTick,
            inDecimal=inDecimal,
        )

    def get_fees_uncollected(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Returns:
            dict: {
                    "qtty_token0":0,  # quantity of uncollected token 0
                    "qtty_token1":0,  # quantity of uncollected token 1
                }
        """
        # positions
        return self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.upperTick,
            tickLower=self.lowerTick,
            inDecimal=inDecimal,
        )

    def get_tvl(self) -> dict:
        """get total value locked of both positions
           TVL = deployed + parked + owed

        Returns:
           dict: {" tvl_token0": ,      (float) Total quantity locked of token 0
                   "tvl_token1": ,      (float) Total quantity locked of token 1
                   "deployed_token0": , (float)
                   "deployed_token1": , (float)
                   "fees_owed_token0": ,(float)
                   "fees_owed_token1": ,(float)
                   "parked_token0": ,   (float) quantity of token 0 parked at contract (not deployed)
                   "parked_token1": ,   (float)  quantity of token 1 parked at contract (not deployed)
                   }
        """
        # get deployed fees as int
        deployed = self.get_qtty_depoloyed(inDecimal=False)

        result = dict()

        # get parked tokens as int
        result["parked_token0"] = self.pool.token0.balanceOf(self.address)
        result["parked_token1"] = self.pool.token1.balanceOf(self.address)

        result["deployed_token0"] = deployed["qtty_token0"]
        result["deployed_token1"] = deployed["qtty_token1"]
        result["fees_owed_token0"] = deployed["fees_owed_token0"]
        result["fees_owed_token1"] = deployed["fees_owed_token1"]

        # sumup
        result["tvl_token0"] = (
            result["deployed_token0"]
            + result["fees_owed_token0"]
            + result["parked_token0"]
        )
        result["tvl_token1"] = (
            result["deployed_token1"]
            + result["fees_owed_token1"]
            + result["parked_token1"]
        )

        # transform everythin to deicmal
        for key in result.keys():
            if "token0" in key:
                result[key] /= 10**self.token0.decimals
            elif "token1" in key:
                result[key] /= 10**self.token1.decimals
            else:
                raise ValueError("Cant convert '{}' field to decimal".format(key))

        return result


class arrakis_hypervisor_cached(arrakis_hypervisor):

    SAVE2FILE = True

    # PROPERTIES
    @property
    def gelatoBalance0(self) -> int:
        prop_name = "gelatoBalance0"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def gelatoBalance1(self) -> int:
        prop_name = "gelatoBalance1"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def gelatoFeeBPS(self) -> int:
        prop_name = "gelatoFeeBPS"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def gelatoRebalanceBPS(self) -> int:
        prop_name = "gelatoRebalanceBPS"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def gelatoSlippageBPS(self) -> int:
        prop_name = "gelatoSlippageBPS"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def gelatoSlippageInterval(self) -> int:
        prop_name = "gelatoSlippageInterval"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def gelatoWithdrawBPS(self) -> int:
        prop_name = "gelatoWithdrawBPS"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getPositionID(self) -> str:
        prop_name = "getPositionID"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getUnderlyingBalances(self) -> dict:
        """getUnderlyingBalances _summary_

        Returns:
           dict: amount0Current: current total underlying balance of token0
                   amount1Current: current total underlying balance of token1
        """
        prop_name = "getUnderlyingBalances"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )

        return result

    @property
    def lowerTick(self) -> int:
        prop_name = "lowerTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def manager(self) -> str:
        prop_name = "manager"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def managerBalance0(self) -> int:
        prop_name = "managerBalance0"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def managerBalance1(self) -> int:
        prop_name = "managerBalance1"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def managerFeeBPS(self) -> int:
        prop_name = "managerFeeBPS"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def managerTreasury(self) -> str:
        prop_name = "managerTreasury"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def name(self) -> str:
        prop_name = "name"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def upperTick(self) -> int:
        prop_name = "upperTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def version(self) -> str:
        prop_name = "version"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result == None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20:
        if self._token0 == None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 == None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def pool(self) -> str:
        if self._pool == None:
            self._pool = univ3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool
