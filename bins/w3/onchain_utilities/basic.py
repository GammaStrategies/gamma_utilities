import logging
import sys
import math
import datetime as dt

from decimal import Decimal
from web3 import Web3, exceptions
from web3.middleware import geth_poa_middleware

from bins.configuration import CONFIGURATION
from bins.general import file_utilities
from bins.cache import cache_utilities


class web3wrap:

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
        if self._network in CONFIGURATION.WEB3_CHAIN_IDS:
            self._chain_id = CONFIGURATION.WEB3_CHAIN_IDS[self._network]
        else:
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

    def timestampFromBlockNumber(self, block: int) -> int:

        block_obj = None
        if block < 1:
            block_obj = self._w3.eth.get_block("latest")
        else:
            block_obj = self._w3.eth.get_block(block)

        # return closest block found
        return block_obj.timestamp

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

    def identify_dex_name(self) -> str:
        """Return dex name using the calling object's type

        Returns:
            str: "uniswapv3", "quickswap" or  not Implemented error
        """
        # cross reference import
        from bins.w3.onchain_utilities.protocols import (
            gamma_hypervisor,
            gamma_hypervisor_quickswap,
        )
        from bins.w3.onchain_utilities.exchanges import univ3_pool, quickswapv3_pool

        #######################

        if isinstance(
            self, (gamma_hypervisor_quickswap, quickswapv3_pool)
        ) or issubclass(type(self), (gamma_hypervisor_quickswap, quickswapv3_pool)):
            return "quickswap"
        elif isinstance(self, (gamma_hypervisor, univ3_pool)) or issubclass(
            type(self), (gamma_hypervisor, univ3_pool)
        ):
            return "uniswapv3"
        else:
            raise NotImplementedError(
                f" Dex name cannot be identified using object type {type(self)}"
            )

    def as_dict(self, convert_bint=False) -> dict:
        result = dict()
        result["block"] = self.block
        # add timestamp ( block timestamp)
        result["timestamp"] = self.timestampFromBlockNumber(block=self.block)
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

    def balanceOf(self, address: str) -> int:
        return self._contract.functions.balanceOf(Web3.toChecksumAddress(address)).call(
            block_identifier=self.block
        )

    @property
    def totalSupply(self) -> int:
        return self._contract.functions.totalSupply().call(block_identifier=self.block)

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

    def as_dict(self, convert_bint=False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to strings ? . Defaults to False.

        Returns:
            dict: decimals, totalSupply(bint) and symbol dict
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["decimals"] = self.decimals
        result["totalSupply"] = (
            self.totalSupply if not convert_bint else str(self.totalSupply)
        )
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
    def totalSupply(self) -> int:
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
