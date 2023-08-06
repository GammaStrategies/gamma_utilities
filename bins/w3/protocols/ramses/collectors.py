import logging

from eth_abi import abi
from hexbytes import HexBytes

from bins.w3.protocols.general import bep20, erc20


class multiFeeDistribution_data_collector:
    """ """

    # SETUP
    def __init__(
        self,
        topics: dict,
        topics_data_decoders: dict,
        network: str,
    ):
        self.network = network

        self._progress_callback = None  # log purp

        self._token_helpers = dict()
        # all data retrieved will be saved here. { <contract_address>: {<topic>: <topic defined content> } }
        self._data = dict()

        # set topics vars
        self.setup_topics(topics=topics, topics_data_decoders=topics_data_decoders)

        # define web3 erc standard helper to retireve token info when needed
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
        logging.getLogger(__name__).debug(
            f"   created {len(filter_chunks)} filter chunks to process the topic data search "
        )

        for idx, filter in enumerate(filter_chunks):
            logging.getLogger(__name__).debug(
                f"    getting entries for chunk {idx}/{len(filter_chunks)} "
            )
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
                    if result_item := self._convert_topic(topic, event, data):
                        # add topic to result item
                        result_item["topic"] = "{}".format(topic.split("_")[1])
                        result_item["logIndex"] = event.logIndex

                        chunk_result.append(result_item)

                # yield when data is available
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
    def _convert_topic(self, topic: str, event, data) -> dict | None:
        # init result
        itm = dict()

        # common vars
        itm["transactionHash"] = event.transactionHash.hex()
        itm["blockHash"] = event.blockHash.hex()
        itm["blockNumber"] = event.blockNumber
        itm["address"] = event.address

        itm["timestamp"] = ""

        # do not process if removed ( chain reorg )
        if event.removed:
            logging.getLogger(__name__).debug(
                f"  topic {topic} txhash {itm['transactionHash']} was tagged as removed. Not processing"
            )
            return None

        # specific vars
        if topic in ["mdf_unstake", "mdf_stake"]:
            # topic_hash =  event.topics[0][-20:].hex()
            itm["user"] = event.topics[1][-20:].hex()
            itm["qtty"] = str(data[0])
        elif topic == "mdf_getAllRewards":
            itm["user"] = event.topics[1][-20:].hex()
            itm["reward_token"] = event.topics[2][-20:].hex()

        elif topic in [
            "mdf_adminChanged",
            "mdf_initialized",
            "mdf_ownershipTransfer",
            "mdf_upgraded",
        ]:
            return None

        else:
            logging.getLogger(__name__).warning(
                f" Can't find topic [{topic}] converter. Discarding  event [{event}]  with data [{data}] "
            )
            return None

        # add is_last_item field so that it can be classified appropriately later when processing
        itm["is_last_item"] = True

        return itm


def create_multiFeeDistribution_data_collector(
    network: str,
) -> multiFeeDistribution_data_collector:
    """Create a data collector class

    Args:
       network (str):

    Returns:
       multiFeeDistribution_data_collector:
    """
    result = multiFeeDistribution_data_collector(
        topics={
            "mdf_getAllRewards": "0x540798df468d7b23d11f156fdb954cb19ad414d150722a7b6d55ba369dea792e",  # event_signature_hash = web3.keccak(text="transfer(uint32...)").hex()
            "mdf_unstake": "0x85082129d87b2fe11527cb1b3b7a520aeb5aa6913f88a3d8757fe40d1db02fdd",
            "mdf_stake": "0xebedb8b3c678666e7f36970bc8f57abf6d8fa2e828c0da91ea5b75bf68ed101a",
            "mdf_adminChanged": "0x7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f",
            "mdf_initialized": "0x7f26b83ff96e1f2b6a682f133852f6798a09c465da95921460cefb3847402498",
            "mdf_ownershipTransfer": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
            "mdf_upgraded": "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b",
        },
        topics_data_decoders={
            # index_topic_1 address user, index_topic_2 address rewardToken, uint256 amount
            "mdf_getAllRewards": ["uint256"],
            # index_topic_1 address owner, uint256 tokens
            "mdf_unstake": ["uint256"],
            # index_topic_1 address owner, uint256 tokens
            "mdf_stake": ["uint256"],
            # address previousAdmin, address newAdmin
            "mdf_adminChanged": ["address", "address"],
            # uint8 version
            "mdf_initialized": ["uint8"],
            # index_topic_1 address previousOwner, index_topic_2 address newOwner
            "mdf_ownershipTransfer": [],
            # index_topic_1 address implementation
            "mdf_upgraded": [],
        },
        network=network,
    )
    return result
