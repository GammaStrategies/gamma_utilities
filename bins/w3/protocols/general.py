import logging
import random
import math
import datetime as dt

import requests
from web3 import Web3, exceptions, types
from web3.contract import Contract
from web3.middleware import geth_poa_middleware, simple_cache_middleware

from bins.w3.helpers.rpcs import RPC_MANAGER, w3Provider

from ...configuration import CONFIGURATION, WEB3_CHAIN_IDS
from ...general import file_utilities
from ...cache import cache_utilities
from ...general.enums import Chain


# main base class


class web3wrap:
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        # set init vars
        self._address = Web3.toChecksumAddress(address)
        self._network = network
        # progress
        self._progress_callback = None

        # force to use either a private or a public RPC service
        self._custom_rpcType: str | None = None

        # set optionals
        self.setup_abi(abi_filename=abi_filename, abi_path=abi_path)

        # setup Web3
        self._w3 = custom_web3 or self.setup_w3(
            network=self._network, web3Url=custom_web3Url
        )

        # setup contract to query
        self.setup_contract(contract_address=self._address, contract_abi=self._abi)
        # setup cache helper
        self.setup_cache()

        # set block
        if not block:
            _block_data = self._getBlockData("latest")
            self._block = _block_data.number
            self._timestamp = _block_data.timestamp
        else:
            self._block = block
            if timestamp == 0:
                # find timestamp
                _block_data = self._getBlockData(self._block)
                self._timestamp = _block_data.timestamp
            else:
                self._timestamp = timestamp

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

    def setup_w3(self, network: str, web3Url: str | None = None) -> Web3:
        # create Web3 helper
        if not web3Url:
            if rpclist := RPC_MANAGER.get_rpc_list(
                network=self._network, rpcKey_names=["private"]
            ):
                web3Url = rpclist[0].url
            elif rpclist := RPC_MANAGER.get_rpc_list(network=self._network):
                # there are no private RPCs available
                logging.getLogger(__name__).warning(
                    f"  no private RPCs available for network {self._network} address {self._address}. Using any available RPC."
                )
                web3Url = RPC_MANAGER.get_rpc_list(network=self._network)[0].url
            else:
                # there are no public nor private RPCs available
                logging.getLogger(__name__).error(
                    f"  no public nor private RPCs available for network {self._network} address {self._address}..."
                )
                raise Exception(f" No RPCs available for network {self._network}.")

        result = Web3(
            Web3.HTTPProvider(
                web3Url,
                request_kwargs={"timeout": 60},
            )
        )
        # add simple cache module
        result.middleware_onion.add(simple_cache_middleware)

        # add middleware as needed
        if network not in [Chain.ETHEREUM.database_name]:
            result.middleware_onion.inject(geth_poa_middleware, layer=0)

        return result

    def setup_contract(self, contract_address: str, contract_abi: str):
        # set contract
        self._contract = self._w3.eth.contract(
            address=contract_address, abi=contract_abi
        )

    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = {"decimals": False, "symbol": False}

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

    # CUSTOM PROPERTIES

    @property
    def abi_root_path(self) -> str:
        # where to find the abi files
        return CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"

    @property
    def address(self) -> str:
        return self._address

    @property
    def w3(self) -> Web3:
        return self._w3

    @property
    def contract(self) -> Contract:
        return self._contract

    @property
    def block(self) -> int:
        """ """
        return self._block

    @block.setter
    def block(self, value: int):
        self._block = value

    @property
    def custom_rpcType(self) -> str | None:
        """ """
        return self._custom_rpcType

    @custom_rpcType.setter
    def custom_rpcType(self, value: str | None):
        self._custom_rpcType = value

    # HELPERS
    def average_blockTime(self, blocksaway: int = 500) -> dt.datetime.timestamp:
        """Average time of block creation

        Args:
           blocksaway (int, optional): blocks used compute average. Defaults to 500.

        Returns:
           dt.datetime.timestamp: average time per block
        """
        result: int = 0
        # no decimals allowed
        blocksaway: int = math.floor(blocksaway)
        #
        if blocksaway > 0:
            block_current: int = self._getBlockData("latest")
            block_past: int = self._getBlockData(block_current.number - blocksaway)
            result: int = (block_current.timestamp - block_past.timestamp) / blocksaway
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
        min_block = self._getBlockData(1)
        if min_block.timestamp > timestamp:
            return 1

        queries_cost = 0
        found_exact = False

        block_curr = self._getBlockData("latest")
        first_step = math.ceil(block_curr.number * 0.85)

        # make sure we have positive block result
        while (block_curr.number + first_step) <= 0:
            first_step -= 1
        # calc blocks to go up/down closer to goal
        block_past = self._getBlockData(block_curr.number - (first_step))
        blocks_x_timestamp = (
            abs(block_curr.timestamp - block_past.timestamp) / first_step
        )

        block_step = (block_curr.timestamp - timestamp) / blocks_x_timestamp
        block_step_sign = -1

        _startime = dt.datetime.now(dt.timezone.utc)

        while block_curr.timestamp != timestamp:
            queries_cost += 1

            # make sure we have positive block result
            while (block_curr.number + (block_step * block_step_sign)) <= 0:
                if queries_cost != 1:
                    # change sign and lower steps
                    block_step_sign *= -1
                # first time here, set lower block steps
                block_step /= 2
            # go to block
            try:
                block_curr = self._getBlockData(
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
            if (dt.datetime.now(dt.timezone.utc) - _startime).total_seconds() > 15:
                if inexact_mode == "before":
                    # select block smaller than objective
                    while block_curr.timestamp > timestamp:
                        block_curr = self._getBlockData(block_curr.number - 1)
                elif inexact_mode == "after":
                    # select block greater than objective
                    while block_curr.timestamp < timestamp:
                        block_curr = self._getBlockData(block_curr.number + 1)
                else:
                    raise ValueError(
                        f" Inexact method chosen is not valid:->  {inexact_mode}"
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
                f" Took {queries_cost} on-chain queries to find block number {block_curr.number} of timestamp {timestamp}"
            )

        else:
            logging.getLogger(__name__).warning(
                f" Could not find the exact block number from timestamp -> took {queries_cost} on-chain queries to find block number {block_curr.number} ({block_curr.timestamp}) closest to timestamp {timestamp}  -> original-found difference {timestamp - block_curr.timestamp}"
            )

        # return closest block found
        return result

    def timestampFromBlockNumber(self, block: int) -> int:
        block_obj = None
        if block < 1:
            block_obj = self._getBlockData("latest")
        else:
            block_obj = self._getBlockData(block)

        # return closest block found
        return block_obj.timestamp

    def get_sameTimestampBlocks(self, block, queries_cost: int):
        result = []
        # try go backwards till different timestamp is found
        curr_block = block
        while curr_block.timestamp == block.timestamp:
            if curr_block.number != block.number:
                result.append(curr_block.number)
            curr_block = self._getBlockData(curr_block.number - 1)
            queries_cost += 1
        # try go forward till different timestamp is found
        curr_block = block
        while curr_block.timestamp == block.timestamp:
            if curr_block.number != block.number:
                result.append(curr_block.number)
            curr_block = self._getBlockData(curr_block.number + 1)
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
        result = []
        tmp_filter = dict(eventfilter)
        toBlock = eventfilter["toBlock"]
        fromBlock = eventfilter["fromBlock"]
        blocksXfilter = math.ceil((toBlock - fromBlock) / max_blocks)

        current_fromBlock = tmp_filter["fromBlock"]
        current_toBlock = current_fromBlock + max_blocks
        for _ in range(blocksXfilter):
            # mod filter blocks
            tmp_filter["toBlock"] = current_toBlock
            tmp_filter["fromBlock"] = current_fromBlock

            # append filter
            result.append(dict(tmp_filter))

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

    def get_chunked_events(
        self,
        eventfilter,
        max_blocks=2000,
        rpcKey_names: list[str] | None = None,
    ):
        # get a list of filters with different block chunks
        for _filter in self.create_eventFilter_chunks(
            eventfilter=eventfilter, max_blocks=max_blocks
        ):
            entries = self.get_all_entries(filter=_filter, rpcKey_names=rpcKey_names)

            # progress if no data found
            if self._progress_callback and len(entries) == 0:
                self._progress_callback(
                    text=f'no matches from blocks {_filter["fromBlock"]} to {_filter["toBlock"]}',
                    remaining=eventfilter["toBlock"] - _filter["toBlock"],
                    total=eventfilter["toBlock"] - eventfilter["fromBlock"],
                )

            # filter blockchain data
            yield from entries

    def get_all_entries(
        self,
        filter,
        rpcKey_names: list[str] | None = None,
    ) -> list:
        entries = []

        for rpc in RPC_MANAGER.get_rpc_list(
            network=self._network, rpcKey_names=rpcKey_names
        ):
            # set rpc
            self._w3 = self.setup_w3(network=self._network, web3Url=rpc.url)
            logging.getLogger(__name__).debug(
                f"   Using {rpc.url} to gather {self._network}'s events"
            )
            # get chunk entries
            try:
                # add rpc attempt
                rpc.add_attempt()
                if entries := self._w3.eth.filter(filter).get_all_entries():
                    # exit rpc loop
                    break
            except (requests.exceptions.HTTPError, ValueError) as e:
                logging.getLogger(__name__).debug(
                    f" Could not get {self._network}'s events usig {rpc.url} from filter  -> {e}"
                )
                # failed rpc event
                rpc.add_failed(error=e)
                # try changing the rpcURL and retry
                continue

        # return all found
        return entries

    def identify_dex_name(self) -> str:
        """Return dex name using the calling object's type"""
        raise NotImplementedError(
            f" Dex name cannot be identified using object type {type(self)}"
        )

    def as_dict(self, convert_bint=False) -> dict:
        result = {
            "block": self.block,
            "timestamp": self._timestamp
            if self._timestamp and self._timestamp > 0
            else self.timestampFromBlockNumber(block=self.block),
        }

        # lower case address to be able to be directly compared
        result["address"] = self.address.lower()
        return result

    # universal failover execute funcion
    def call_function(self, function_name: str, rpcs: list[w3Provider], *args):
        # loop choose url
        for rpc in rpcs:
            try:
                rpc.add_attempt()
                # create web3 conn
                chain_connection = self.setup_w3(network=self._network, web3Url=rpc.url)
                # set root w3 conn
                self._w3 = chain_connection
                # create contract
                contract = chain_connection.eth.contract(
                    address=self._address, abi=self._abi
                )
                # execute function
                return getattr(contract.functions, function_name)(*args).call(
                    block_identifier=self.block
                )

            except Exception as e:
                # not working rpc or function at block has no data
                logging.getLogger(__name__).debug(
                    f"  Error calling function {function_name} using {rpc.url} rpc: {e}  address: {self._address}"
                )
                rpc.add_failed(error=e)

        # no rpcUrl worked
        return None

    def call_function_autoRpc(
        self,
        function_name: str,
        rpcKey_names: list[str] | None = None,
        *args,
    ):
        """Call a function using an RPC list from configuration file

        Args:
            function_name (str): contract function name to call
            rpcKey_names (list[str]): private or public or whatever is placed in config w3Providers
            args: function arguments
        Returns:
            Any or None: depending on the function called
        """

        if not rpcKey_names and self._custom_rpcType:
            rpcKey_names = [self._custom_rpcType]

        result = self.call_function(
            function_name,
            RPC_MANAGER.get_rpc_list(network=self._network, rpcKey_names=rpcKey_names),
            *args,
        )
        # result = self.call_function(
        #     function_name,
        #     self.get_rpcUrls(rpcKey_names=rpcKey_names),
        #     *args,
        # )
        if not result is None:
            return result
        else:
            logging.getLogger(__name__).error(
                f" Could not use any rpcProvider calling function {function_name} with params {args} on {self._network} network {self.address} block {self.block}"
            )

        return None

    def _getTransactionReceipt(self, txHash: str):
        """Get transaction receipt

        Args:
            txHash (str): transaction hash

        Returns:
            dict: transaction receipt
        """

        # get w3Provider list
        for rpc in RPC_MANAGER.get_rpc_list(network=self._network):
            try:
                rpc.add_attempt()
                _w3 = self.setup_w3(network=self._network, web3Url=rpc.url)
                return _w3.eth.get_transaction_receipt(txHash)
            except Exception as e:
                logging.getLogger(__name__).debug(
                    f" error getting transaction receipt using {rpc.url} rpc: {e}"
                )
                rpc.add_failed(error=e)
                continue

        return None

    def _getBlockData(self, block: int | str) -> types.BlockData:
        """Get block data

        Args:
            block (int): block number or 'latest'

        """

        # get w3Provider list
        for rpc in RPC_MANAGER.get_rpc_list(network=self._network):
            try:
                rpc.add_attempt()
                _w3 = self.setup_w3(network=self._network, web3Url=rpc.url)
                return _w3.eth.get_block(block)
            except Exception as e:
                logging.getLogger(__name__).debug(
                    f" error getting block data using {rpc.url} rpc: {e}"
                )
                rpc.add_failed(error=e)
                continue

        return None


# ERC20


class erc20(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "erc20"
        self._abi_path = abi_path or self.abi_root_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    # PROPERTIES
    @property
    def decimals(self) -> int:
        return self.call_function_autoRpc(function_name="decimals")

    def balanceOf(self, address: str) -> int:
        return self.call_function_autoRpc(
            "balanceOf", None, Web3.toChecksumAddress(address)
        )

    @property
    def totalSupply(self) -> int:
        return self.call_function_autoRpc(function_name="totalSupply")

    @property
    def symbol(self) -> str:
        # MKR special: ( has a too large for python int )
        if self.address == "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2":
            return "MKR"
        return self.call_function_autoRpc(function_name="symbol")

    def allowance(self, owner: str, spender: str) -> int:
        return self.call_function_autoRpc(
            "allowance",
            None,
            Web3.toChecksumAddress(owner),
            Web3.toChecksumAddress(spender),
        )

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
            str(self.totalSupply) if convert_bint else self.totalSupply
        )

        result["symbol"] = self.symbol

        return result


class erc20_cached(erc20):
    SAVE2FILE = True

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = {"decimals": False, "symbol": False}

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

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
        if result is None:
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
        if result is None:
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
        if result is None:
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


# BEP20


class bep20(erc20):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "bep20"
        self._abi_path = abi_path or self.abi_root_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )


class bep20_cached(bep20):
    SAVE2FILE = True

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = {"decimals": False, "symbol": False}

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

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
        if result is None:
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
        if result is None:
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
        if result is None:
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
