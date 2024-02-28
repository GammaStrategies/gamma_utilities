import logging
import sys

from eth_abi import abi, exceptions
from hexbytes import HexBytes

from web3 import Web3, contract
from web3.middleware import geth_poa_middleware, simple_cache_middleware
from bins.errors.general import ProcessingError
from bins.general.enums import error_identity

from bins.w3.helpers.rpcs import RPC_MANAGER

from ....configuration import CONFIGURATION
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
            self._topics_reversed = {
                v[0] if isinstance(v, list) else v: k for k, v in self._topics.items()
            }

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
        contracts: list = None,
        topics: dict = {},
        topics_data_decoders: dict = {},
        max_blocks: int = 5000,
    ):
        """operation item generator

        Args:
            block_ini (int): _description_
            block_end (int): _description_
            contracts (list, optional):
            topics (dict, optional):  Defaults to {}.
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
            eventfilter=(
                {
                    "fromBlock": block_ini,
                    "toBlock": block_end,
                    "address": contracts,
                    "topics": [[v for k, v in self._topics.items()]],
                }
                if contracts
                else {
                    "fromBlock": block_ini,
                    "toBlock": block_end,
                    "topics": [[v for k, v in self._topics.items()]],
                }
            ),
            max_blocks=max_blocks,
        )

        for filter in filter_chunks:
            entries = []
            # try catch processing error too many blocks to lower the chunk size
            try:
                # fill entries
                entries = self._web3_helper.get_all_entries(
                    filter=filter, rpcKey_names=["private", "public"]
                )
            except ProcessingError as e:
                if e.identity == error_identity.TOO_MANY_BLOCKS_TO_QUERY:
                    # lower the chunk size to 1000 per query
                    logging.getLogger(__name__).warning(
                        f" Too many blocks in filter error query. Lowering this chunk size item from {max_blocks} to 1000 "
                    )
                    sub_filter_chunks = self._web3_helper.create_eventFilter_chunks(
                        eventfilter=filter,
                        max_blocks=1000,
                    )
                    # fill entries
                    for sub_chunk in sub_filter_chunks:
                        entries += self._web3_helper.get_all_entries(
                            filter=sub_chunk, rpcKey_names=["private", "public"]
                        )

            if entries:
                chunk_result = []
                for event in entries:
                    _tmp_topic_hex = event.topics[0].hex()
                    # check if topic is in the list
                    if not _tmp_topic_hex in self._topics_reversed:
                        logging.getLogger(__name__).error(
                            f" Topic not found in topics list. Discarding topic {_tmp_topic_hex}. [ topics {event.topics} ]"
                        )
                        continue
                    # get topic name found
                    topic = self._topics_reversed[event.topics[0].hex()]
                    # first topic is topic id
                    custom_abi_data = self._topics_data_decoders[topic]
                    # decode
                    data = None
                    try:
                        data = abi.decode(custom_abi_data, HexBytes(event.data))
                    except exceptions.InsufficientDataBytes:
                        # probably a transfer of a non ERC20 token ( veNFT ? )
                        data = None
                    except Exception as e:
                        logging.getLogger(__name__).error(
                            f" Error decoding data of event at block {event.blockNumber}  event:{event}"
                        )
                        raise

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

                # yield when there is data
                if chunk_result:
                    yield chunk_result

            elif self._progress_callback:
                # no data found at current filter
                self._progress_callback(
                    text=f" no match from {filter['fromBlock']} to {filter['toBlock']}",
                    total=block_end - block_ini,
                    remaining=block_end - filter["toBlock"],
                )

    # HELPERS
    def _convert_topic(self, topic: str, event, data) -> dict:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address.lower()

        itm["timestamp"] = ""
        itm["decimals_token0"] = ""
        itm["decimals_token1"] = ""
        itm["decimals_contract"] = ""

        # specific vars
        if topic in ["gamma_deposit", "gamma_withdraw"]:
            itm["sender"] = event.topics[1][-20:].hex().lower()
            itm["to"] = event.topics[2][-20:].hex().lower()
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
            itm["src"] = event.topics[1][-20:].hex().lower()
            itm["dst"] = event.topics[2][-20:].hex().lower()
            itm["qtty"] = str(data[0])

        elif topic in ["arrakis_deposit", "arrakis_withdraw"]:
            itm["sender"] = (
                data[0].lower() if topic == "arrakis_deposit" else event.address.lower()
            )
            itm["to"] = (
                data[0].lower()
                if topic == "arrakis_withdraw"
                else event.address.lower()
            )
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
            itm["recipient"] = data[0].lower()
            itm["amount0"] = str(data[1])
            itm["amount1"] = str(data[2])

        elif topic == "transfer":
            itm["src"] = event.topics[1][-20:].hex().lower()
            itm["dst"] = event.topics[2][-20:].hex().lower()
            # when DATA is None, consider it a transfer of a non ERC20 token
            itm["qtty"] = str(data[0]) if data else "1"
        elif topic == "rewardPaid":
            itm["user"] = event.topics[1][-20:].hex().lower()
            itm["rewardToken"] = event.topics[2][-20:].hex().lower()
            itm["qtty"] = str(data[0])

        else:
            logging.getLogger(__name__).warning(
                f" Can't find topic [{topic}] converter. Discarding  event [{event}]  with data [{data}] "
            )

        return itm


class wallet_transfers_collector(data_collector):
    # SETUP
    def __init__(
        self,
        wallet_addresses: list[str],
        network: str,
    ):
        self.network = network
        self.wallet_addresses = wallet_addresses
        self._progress_callback = None  # log purp
        # univ3_pool helpers simulating contract functionality just to be able to use the tokenX decimal part
        self._token_helpers = dict()
        # all data retrieved will be saved here. { <contract_address>: {<topic>: <topic defined content> } }
        self._data = dict()

        # define helper
        self._web3_helper = (
            bep20(address="0x0000000000000000000000000000000000000000", network=network)
            if network == "binance"
            else erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
        )

    def _get_topics(self, fromTo: str = "to") -> list[str]:
        """Create topics to look for transfers from or to the wallet addresses

        Args:
            fromTo (str, optional): transaction direction with regards to the wallet . Defaults to "to".

        Returns:
            list[str]: topic list filter
        """
        self.transfer_abi = Web3.sha3(text="Transfer(address,address,uint256)").hex()
        self._topics = []
        addresses = []
        for address in self.wallet_addresses:
            # create address to
            address_to = Web3.toBytes(hexstr=Web3.toChecksumAddress(address))
            # left pad address to 32 bytes
            address_to = "0x" + (bytes(32 - len(address_to)) + address_to).hex()
            # add to topics
            addresses.append(address_to)

        # add to address ( operations going into the wallet)
        self._topics = [
            Web3.sha3(text="Transfer(address,address,uint256)").hex(),
            addresses if fromTo == "from" else None,
            addresses if fromTo == "to" else None,
        ]
        return self._topics

    @property
    def contract_abi(self) -> list[dict]:
        return [
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "from", "type": "address"},
                    {"indexed": True, "name": "to", "type": "address"},
                    {"indexed": False, "name": "value", "type": "uint256"},
                ],
                "name": "Transfer",
                "type": "event",
            }
        ]

    def operations_generator(
        self,
        block_ini: int,
        block_end: int | None = None,
        contract_addresses: list[str] = None,
        fromTo: str = "to",
        max_blocks: int = 5000,
    ):
        """operation item generator

        Args:
            block_ini (int):
            block_end (int):
            contract_addresses (list[str], optional): List of contracts firing the event. Defaults to None.
            fromTo (str, optional): transaction direction with regards to the wallet  . Defaults to "to".
            max_blocks (int, optional): . Defaults to 5000.

        Returns:
            dict: includes topic operation transfers...

        Yields:
            Iterator[dict]:
        """

        if not block_end or block_end == 0:
            block_end = self._web3_helper._getBlockData(block="latest")["number"]

        # create event filter
        if contract_addresses:
            event_filter = {
                "fromBlock": block_ini,
                "toBlock": block_end,
                "address": contract_addresses,
                "topics": self._get_topics(fromTo=fromTo),
            }
        else:
            event_filter = {
                "fromBlock": block_ini,
                "toBlock": block_end,
                "topics": self._get_topics(fromTo=fromTo),
            }

        # get a list of events
        filter_chunks = self._web3_helper.create_eventFilter_chunks(
            eventfilter=event_filter,
            max_blocks=max_blocks,
        )

        for filter in filter_chunks:
            entries = []
            # try catch processing error too many blocks to lower the chunk size
            try:
                # fill entries
                entries = self._web3_helper.get_all_entries(
                    filter=filter, rpcKey_names=["private", "public"]
                )
            except ProcessingError as e:
                if e.identity == error_identity.TOO_MANY_BLOCKS_TO_QUERY:
                    # lower the chunk size to 1000 per query
                    logging.getLogger(__name__).warning(
                        f" Too many blocks in filter error query. Lowering this chunk size item from {max_blocks} to 1000 "
                    )
                    sub_filter_chunks = self._web3_helper.create_eventFilter_chunks(
                        eventfilter=filter,
                        max_blocks=1000,
                    )
                    # fill entries
                    for sub_chunk in sub_filter_chunks:
                        entries += self._web3_helper.get_all_entries(
                            filter=sub_chunk, rpcKey_names=["private", "public"]
                        )

            if entries:
                chunk_result = []
                for event in entries:
                    # get topic name found
                    topic = "transfer"
                    # first topic is topic id
                    custom_abi_data = ["uint256"]
                    # decode:
                    try:
                        data = abi.decode(custom_abi_data, HexBytes(event.data))
                    except exceptions.InsufficientDataBytes:
                        # probably a transfer of a non ERC20 token ( veNFT ? )
                        data = None
                    except Exception as e:
                        logging.getLogger(__name__).error(
                            f" Error decoding data of event at block {event.blockNumber}  event:{event}"
                        )
                        raise

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
                    result_item["topic"] = "transfer"
                    result_item["logIndex"] = event.logIndex

                    chunk_result.append(result_item)

                # yield when there is data
                if chunk_result:
                    yield chunk_result

            elif self._progress_callback:
                # no data found at current filter
                self._progress_callback(
                    text=f" no match from {filter['fromBlock']} to {filter['toBlock']}",
                    total=block_end - block_ini,
                    remaining=block_end - filter["toBlock"],
                )

    # HELPERS
    def _convert_topic(self, topic: str, event, data) -> dict:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address.lower()

        # itm["timestamp"] = ""
        # itm["decimals_token0"] = ""
        # itm["decimals_token1"] = ""
        # itm["decimals_contract"] = ""

        # specific vars
        if topic in ["transfer"]:
            itm["src"] = event.topics[1][-20:].hex().lower()
            itm["dst"] = event.topics[2][-20:].hex().lower()
            # when DATA is None, consider it a transfer of a non ERC20 token
            itm["qtty"] = str(data[0]) if data else "1"

        else:
            logging.getLogger(__name__).warning(
                f" Can't find topic [{topic}] converter. Discarding  event [{event}]  with data [{data}] "
            )

        return itm


class rewardPaid_collector(data_collector):
    # SETUP
    def __init__(
        self,
        wallet_addresses: list[str],
        network: str,
    ):
        self.network = network
        self.wallet_addresses = wallet_addresses
        self._progress_callback = None  # log purp
        # univ3_pool helpers simulating contract functionality just to be able to use the tokenX decimal part
        self._token_helpers = dict()
        # all data retrieved will be saved here. { <contract_address>: {<topic>: <topic defined content> } }
        self._data = dict()

        # define helper
        self._web3_helper = (
            bep20(address="0x0000000000000000000000000000000000000000", network=network)
            if network == "binance"
            else erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
        )

    def _get_topics(self) -> list[str]:
        """Create topics to look for transfers from or to the wallet addresses

        Args:
            fromTo (str, optional): transaction direction with regards to the wallet . Defaults to "to".

        Returns:
            list[str]: topic list filter
        """
        self.transfer_abi = Web3.sha3(text="RewardPaid(address,address,uint256)").hex()
        self._topics = []
        addresses = []
        for address in self.wallet_addresses:
            # create address to
            address_to = Web3.toBytes(hexstr=Web3.toChecksumAddress(address))
            # left pad address to 32 bytes
            address_to = "0x" + (bytes(32 - len(address_to)) + address_to).hex()
            # add to topics
            addresses.append(address_to)

        # add to address ( operations going into the wallet)
        self._topics = [
            Web3.sha3(text="RewardPaid(address,address,uint256)").hex(),
            addresses,
            None,
        ]
        return self._topics

    def operations_generator(
        self,
        block_ini: int,
        block_end: int | None = None,
        contract_addresses: list[str] = None,
        max_blocks: int = 5000,
    ):
        """operation item generator

        Args:
            block_ini (int):
            block_end (int):
            contract_addresses (list[str], optional): List of contracts firing the event. Defaults to None.
            max_blocks (int, optional): . Defaults to 5000.

        Returns:
            dict: includes topic operation transfers...

        Yields:
            Iterator[dict]:
        """

        if not block_end or block_end == 0:
            block_end = self._web3_helper._getBlockData(block="latest")["number"]

        # create event filter
        if contract_addresses:
            event_filter = {
                "fromBlock": block_ini,
                "toBlock": block_end,
                "address": contract_addresses,
                "topics": self._get_topics(),
            }
        else:
            event_filter = {
                "fromBlock": block_ini,
                "toBlock": block_end,
                "topics": self._get_topics(),
            }

        # get a list of events
        filter_chunks = self._web3_helper.create_eventFilter_chunks(
            eventfilter=event_filter,
            max_blocks=max_blocks,
        )

        for filter in filter_chunks:
            entries = []
            # try catch processing error too many blocks to lower the chunk size
            try:
                # fill entries
                entries = self._web3_helper.get_all_entries(
                    filter=filter, rpcKey_names=["private", "public"]
                )
            except ProcessingError as e:
                if e.identity == error_identity.TOO_MANY_BLOCKS_TO_QUERY:
                    # lower the chunk size to 1000 per query
                    logging.getLogger(__name__).warning(
                        f" Too many blocks in filter error query. Lowering this chunk size item from {max_blocks} to 1000 "
                    )
                    sub_filter_chunks = self._web3_helper.create_eventFilter_chunks(
                        eventfilter=filter,
                        max_blocks=1000,
                    )
                    # fill entries
                    for sub_chunk in sub_filter_chunks:
                        entries += self._web3_helper.get_all_entries(
                            filter=sub_chunk, rpcKey_names=["private", "public"]
                        )

            if entries:
                chunk_result = []
                for event in entries:
                    # get topic name found
                    topic = "rewardPaid"
                    # first topic is topic id
                    custom_abi_data = ["uint256"]
                    # decode:
                    try:
                        data = abi.decode(custom_abi_data, HexBytes(event.data))
                    except Exception as e:
                        logging.getLogger(__name__).error(
                            f" Error decoding data of event at block {event.blockNumber}  event:{event}"
                        )
                        raise

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
                    result_item["topic"] = "rewardPaid"
                    result_item["logIndex"] = event.logIndex

                    chunk_result.append(result_item)

                # yield when there is data
                if chunk_result:
                    yield chunk_result

            elif self._progress_callback:
                # no data found at current filter
                self._progress_callback(
                    text=f" no match from {filter['fromBlock']} to {filter['toBlock']}",
                    total=block_end - block_ini,
                    remaining=block_end - filter["toBlock"],
                )

    # HELPERS
    def _convert_topic(self, topic: str, event, data) -> dict:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address.lower()

        # itm["timestamp"] = ""
        # itm["decimals_token0"] = ""
        # itm["decimals_token1"] = ""
        # itm["decimals_contract"] = ""

        # specific vars
        if topic in ["rewardPaid"]:
            # user = destination = wallet addresses
            itm["user"] = event.topics[1][-20:].hex().lower()
            itm["token"] = event.topics[2][-20:].hex().lower()
            # when DATA is None, consider it a transfer of a non ERC20 token
            itm["qtty"] = str(data[0]) if data else "1"

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
