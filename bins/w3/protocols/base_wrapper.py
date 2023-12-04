from http.client import RemoteDisconnected
import logging
import math
import datetime as dt

import requests
from web3 import Web3, exceptions, types
from web3.contract import Contract
from web3.middleware import geth_poa_middleware, simple_cache_middleware

from ..helpers.rpcs import RPC_MANAGER, w3Provider
from ...errors.general import ProcessingError

from ...configuration import CONFIGURATION
from ...config.current import WEB3_CHAIN_IDS  # ,CFG
from ...general import file_utilities
from ...cache import cache_utilities
from ...general.enums import Chain, cuType, error_identity, text_to_chain


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
        if not block or block == 0:
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

    def merge_abi(self, abi_filename: str, abi_path: str):
        # merge abi
        self._abi += file_utilities.load_json(
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
                    f"  no public nor private RPCs available for network {self._network} address {self._address}. Forcing the use of any private RPC."
                )
                # force the use any private RPC
                web3Url = RPC_MANAGER.get_rpc_list(
                    network=self._network,
                    rpcKey_names=["private"],
                    availability_filter=False,
                )[0].url

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

    @property
    def contract_functions(self) -> list[dict]:
        """list of contract functions settings

        Returns:
            list[dict]:
        """
        return [
            abi_itm
            for abi_itm in self._abi
            if abi_itm["type"] == "function"
            and abi_itm["stateMutability"] == "view"
            and len(abi_itm["inputs"]) == 0
        ]

    # HELPERS
    def average_blockTime(self, blocksaway: int = 500) -> float:
        """Average time of block creation

        Args:
           blocksaway (int, optional): blocks used compute average. Defaults to 500.

        Returns:
           float: average time per block in seconds
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
           eventfilter (dict):  {  'fromBlock': ,
                                   'toBlock': block,
                                   ...
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
                rpc.add_attempt(method=cuType.eth_getLogs)
                if entries := self._w3.eth.filter(filter).get_all_entries():
                    # exit rpc loop
                    break
            except (requests.exceptions.HTTPError, ValueError) as e:
                # {'code': -32602, 'message': 'eth_newFilter is limited to 1024 block range. Please check the parameter requirements at  https://docs.blockpi.io/documentations/api-reference'}
                if (
                    isinstance(e, ValueError)
                    and isinstance(e.args[0], dict)
                    and (
                        e.args[0].get("code", None) == -32602
                        or (
                            e.args[0].get("code", None) == -32000
                            and "too many blocks" in e.args[0].get("message", "")
                        )
                    )
                ):
                    # -32000 is an execution reverted, so check if lowering block range helps
                    # https://api.avax.network/ext/bc/C/rpc from filter  -> {'code': -32000, 'message': 'requested too many blocks from 37069639 to 37074639, maximum is set to 2048

                    # too many blocks to query
                    logging.getLogger(__name__).debug(
                        f" {rpc.type} RPC {rpc.url} returned a too many blocks to query error. Trying to lower blocks per query to 1000"
                    )
                    # rpc.add_failed(error=e)
                    # raise loop
                    raise ProcessingError(
                        chain=text_to_chain(self._network),
                        item={"address": self._address},
                        identity=error_identity.TOO_MANY_BLOCKS_TO_QUERY,
                        action="sleepNretry",
                        message=f"  too many blocks to query for network {self._network} address {self._address}. Trying to lower blocks per query to 1000",
                    )

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

    def as_dict(self, convert_bint=False, minimal: bool = False) -> dict:
        result = {
            "block": self.block,
            "timestamp": self._timestamp
            if self._timestamp and self._timestamp > 0
            else self.timestampFromBlockNumber(block=self.block),
        }

        # lower case address to be able to be directly compared
        result["address"] = self.address.lower()
        return result

    def get_abi_function(
        self,
        name: str,
        type: str = "function",
        stateMutability: str = "view",
        outputs_qtty: int | None = None,
    ) -> dict:
        """Get the ABI of a function

        Args:
            name (str): _description_
            type (str, optional): _description_. Defaults to "function".
            stateMutability (str, optional): _description_. Defaults to "view".
            outputs_qtty (int | None, optional): _description_. Defaults to None.

        Returns:
            dict:      {
                        "inputs": [],
                        "name": "xToken",
                        "outputs": [
                            {
                                "internalType": "contract IERC20",
                                "name": "",
                                "type": "address"
                            }
                        ],
                        "stateMutability": "view",
                        "type": "function"
                    }
        """
        for fn in self._abi:
            if (
                fn.get("name", None) == name
                and fn.get("type", None) == type
                and fn.get("stateMutability", None) == stateMutability
            ):
                if outputs_qtty is None:
                    return fn
                elif len(fn["outputs"]) == outputs_qtty:
                    return fn

    def get_abi_functions(
        self,
        names: list[str] | None = None,
        types: list[str] = ["function"],
        stateMutabilitys: list[str] = ["view"],
        inputs_exist: bool = False,
    ) -> list[dict]:
        """Get multiple ABI  functions or props from the same contract

        Args:
            names (list[str], optional): . Defaults to None.
            types (list[str], optional): . Defaults to "function".
            stateMutabilitys (list[str], optional): . Defaults to "view".
            inputs_exist (bool, optional): Must have inputs?. Defaults to False.

        Returns:
            list[dict]:  [    {
                        "inputs": [],
                        "name": "xToken",
                        "outputs": [
                            {
                                "internalType": "contract IERC20",
                                "name": "",
                                "type": "address"
                            }
                        ],
                        "stateMutability": "view",
                        "type": "function"
                    },...]
        """
        result = []
        for fn in self._abi:
            if (
                ((fn.get("name", None) in names) if names else True)
                and ((fn.get("type", None) in types) if types else True)
                and (
                    (fn.get("stateMutability", None) in stateMutabilitys)
                    if stateMutabilitys
                    else True
                )
            ):
                if inputs_exist and len(fn["inputs"]) == 0:
                    continue
                elif not inputs_exist and len(fn["inputs"]) > 0:
                    continue
                result.append(fn)

        return result

    # universal failover execute funcion
    def call_function(self, function_name: str, rpcs: list[w3Provider], *args):
        # loop choose url
        for rpc in rpcs:
            try:
                rpc.add_attempt(method=cuType.eth_call)
                # create web3 conn
                chain_connection = self.setup_w3(network=self._network, web3Url=rpc.url)
                # set root w3 conn
                self._w3 = chain_connection
                # create contract
                contract = chain_connection.eth.contract(
                    address=self._address, abi=self._abi
                )
                # execute function ( result can be zero )
                result = getattr(contract.functions, function_name)(*args).call(
                    block_identifier=self.block
                )
                logging.getLogger(__name__).debug(
                    f" {rpc.type} RPC {rpc.url} successfully returned result when calling function {function_name} in {self._network}'s contract {self.address} at block {self.block}"
                )
                return result

            except ValueError as e:
                if isinstance(e, exceptions.ContractLogicError):
                    # function not found in contract, maybe this function was not available at this block, etc...
                    if function_name == "currentFee":
                        # Ramses added this function to the contract after launch, so it will fail for older blocks.
                        # This is a known error, so do not add failed attempt and return 0
                        logging.getLogger(__name__).debug(
                            f" function {function_name} in {self._network}'s contract {self.address} at block {self.block} is called but does not exist. Returned 0"
                        )
                        return 0

                    # log error
                    logging.getLogger(__name__).debug(
                        f" function {function_name} in {self._network}'s contract {self.address} at block {self.block} seems to not exist. Check it. err: {e}"
                    )
                    # return so the other rpcs are not futil used
                    return None

                else:
                    #
                    for err in e.args:
                        # try to react from the code
                        if code := err.get("code", None):
                            if code in [4294935296, -32000]:
                                # 'header not found' / 'missing trie node': this rpc endpoint does not have the data. Try another one and do not add failed attempt.
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} seems to not return information when calling function {function_name} in {self._network}'s contract {self.address} at block {self.block} message: {err.get('message')}. Continue without adding failed attempt."
                                )
                                continue
                            elif code in [
                                429,
                                4294935297,
                            ] or "exceeded its concurrent requests capacity" in err.get(
                                "message", ""
                            ):
                                # exceeded concurrent requests capacity
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} exceeded its concurrent requests capacity. Adding failed attempt. err: {err.get('message')}"
                                )
                                rpc.add_failed(error=e)
                                # exit loop
                                break
                            elif code == 1:
                                # {'message': 'No response or no available upstream for eth_call', 'code': 1}
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} is not responding. Adding failed attempt. err: {err.get('message')}"
                                )
                                rpc.add_failed(error=e)

                            elif code == 12:
                                # {'message': "Can't route your request to suitable provider, if you specified certain providers revise the list", 'code': 12}
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} is overwhelmed. Adding failed attempt. err: {err.get('message')}"
                                )
                                rpc.add_failed(error=e)
                            elif code in [-32005]:
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} current plan is not enough to place this kind of calls. Adding failed attempt. err: {err.get('message')}"
                                )
                                rpc.add_failed(error=e)
                            elif code in [-32801]:
                                #'message': 'no historical RPC is available for this historical (pre-bedrock) execution request'}
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} has no data to return for {self._network}'s contract {self.address} at block {self.block}. err: {err.get('message')}"
                                )

                            elif code == 0:
                                # {'code': 0, 'message': "we can't execute this request"}
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} returned a code 0 while querying function {function_name}. Adding failed attempt. err: {err.get('message')}"
                                )
                                rpc.add_failed(error=e)

                            elif (
                                code == 4294935199
                                or "rate limit" in err.get("message", "").lower()
                            ):
                                # https://lb.drpc.org/ogrpc?network=arbitrum&dkey...   -> {'message': 'Rate limit reached', 'code': 4294935199}
                                logging.getLogger(__name__).debug(
                                    f" {rpc.type} RPC {rpc.url} returned a rate limit error while querying function {function_name}. Adding failed attempt. err: {err.get('message')}"
                                )
                                rpc.add_failed(error=e)

                            else:
                                # "Your app has exceeded its concurrent requests capacity. If you have retries enabled, you can safely ignore this message
                                logging.getLogger(__name__).exception(
                                    f" [1]Unknown ValueError: {rpc.type} RPC {rpc.url} function {function_name}   -> {err}"
                                )
                                rpc.add_failed(error=e)

                        else:
                            # {'message': 'Upgrade to an archive plan add-on for your account. Max height of block allowed on archive for your current plan: 128', 'code': -32005}
                            logging.getLogger(__name__).exception(
                                f" [2]Unknown ValueError: {rpc.type} RPC {rpc.url} function {function_name}   -> {err}"
                            )
                            rpc.add_failed(error=e)

            except requests.HTTPError as e:
                for err in e.args:
                    if "too many requests" in err.lower() or "429" in err:
                        # "Too Many Requests": adding failed attempt  ('429 Client Error: Too Many Requests for url)
                        logging.getLogger(__name__).debug(
                            f" Too many requests made to the {rpc.type} RPC {rpc.url} Continue adding failed attempt."
                        )
                        rpc.add_failed(error=e)
                    elif "401" in err:
                        # Unknown requests.HTTPError: 401 Client Error: Unauthorized for url
                        logging.getLogger(__name__).debug(
                            f" Too many requests made to the {rpc.type} RPC {rpc.url} Disabling it for 120 sec."
                        )
                        rpc._set_unavailable(cooldown=120)
                    elif "402" in err:
                        # Unknown requests.HTTPError: 402 Client Error: Payment Required for url:
                        logging.getLogger(__name__).debug(
                            f" Too many requests made to the {rpc.type} RPC {rpc.url} Payment required... Disabling it for 120 hours"
                        )
                        rpc._set_unavailable(cooldown=60 * 60 * 120)

                    elif "403" in err:
                        # Unknown requests.HTTPError: 403 Client Error: Forbidden for url
                        logging.getLogger(__name__).debug(
                            f" Forbidden response querying {rpc.type} RPC {rpc.url}. Disabling it for 120 sec."
                        )
                        rpc._set_unavailable(cooldown=120)
                    elif "502" in err:
                        # 502 Server Error: Bad Gateway for url
                        logging.getLogger(__name__).debug(
                            f" Bad getaway response querying {rpc.type} RPC {rpc.url}. Disabling it for 120 sec."
                        )
                        rpc._set_unavailable(cooldown=120)
                    elif "0" in err:
                        # unable to execute request
                        logging.getLogger(__name__).debug(
                            f"  can't execute this request querying {rpc.type} RPC {rpc.url}. Disabling it for 120 sec."
                        )
                        rpc._set_unavailable(cooldown=120)
                    else:
                        logging.getLogger(__name__).exception(
                            f" Unknown requests.HTTPError: {e}"
                        )
                        rpc.add_failed(error=e)
            except RemoteDisconnected as e:
                logging.getLogger(__name__).debug(
                    f" {rpc.type} RPC {rpc.url} disconnected without response. adding failed attempt."
                )
                rpc.add_failed(error=e)

            except exceptions.BadFunctionCallOutput as e:
                # web3.exceptions.BadFunctionCallOutput: Could not decode contract ..... with return data: b''

                if len(e.args) and "with return data: b''" in e.args[0]:
                    # No bytes returned error: this may be an RPC error, so add it n try again
                    rpc.add_failed(error=e)
                else:
                    logging.getLogger(__name__).error(
                        f" {rpc.type} RPC {rpc.url} returned a BadFunctionCallOutput while calling function {function_name} in {self._network}'s contract {self.address} at block {self.block}. BREAKING. err: {e}"
                    )

                    # raise error to process
                    raise ProcessingError(
                        chain=text_to_chain(self._network),
                        item={
                            "address": self._address,
                            "function": function_name,
                            "block": self.block,
                            "error": e.args[0],
                            "rpc_type": rpc.type,
                            "rpc_url": rpc.url,
                        },
                        identity=error_identity.WRONG_CONTRACT_FIELD_TYPE,
                        action="important_message",
                        message=f" Wrong ABI found for {self._network}'s contract {self.address} (fn:{function_name})",
                    )
                    # exit loop
                    # break

            except Exception as e:
                # unknown error
                # not working rpc or function at block has no data
                logging.getLogger(__name__).exception(
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
        if not result is None:
            return result
        else:
            logging.getLogger(__name__).error(
                f" Could not use any rpcProvider calling function {function_name} with params {args} on {self._network} network {self.address} block {self.block}"
            )

        raise ProcessingError(
            chain=text_to_chain(self._network),
            item={
                "address": self._address,
                "type": type(self).__name__,
            },
            identity=error_identity.NO_RPC_AVAILABLE,
            action="sleepNretry",
            message=f"  no public nor private RPCs available for network {self._network}",
        )

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
                rpc.add_attempt(method=cuType.eth_getTransactionReceipt)
                _w3 = self.setup_w3(network=self._network, web3Url=rpc.url)
                if "GENESIS" in txHash:
                    logging.getLogger(__name__).debug(
                        f" txHash [{txHash}] is a genesis tx, so it will fail to be retrieved from any rpc. Return none"
                    )
                    return None

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

        last_error = None

        # get w3Provider list
        for rpc in RPC_MANAGER.get_rpc_list(network=self._network):
            try:
                rpc.add_attempt(method=cuType.eth_getBlockByNumber)
                _w3 = self.setup_w3(network=self._network, web3Url=rpc.url)
                if not isinstance(block, str) and not isinstance(block, int):
                    logging.getLogger(__name__).error(
                        f" ERROR --> Block {block} is not valid. address:{self._address} {self._network} {rpc.url}"
                    )
                return _w3.eth.get_block(block)

            except exceptions.BlockNotFound as e:
                logging.getLogger(__name__).error(
                    f" Block {block} not found at {self._network} using {rpc.url}"
                )
                last_error = exceptions.BlockNotFound(
                    f"Block {block} not found at {self._network} using {rpc.url}"
                )
            except Exception as e:
                for err in e.args:
                    if isinstance(err, dict):
                        if err.get("code", None) in [-32005, -32001]:
                            # {'code': -32001, 'message': 'Resource not found.'}
                            logging.getLogger(__name__).error(
                                f" Block {block} not found at {self._network} using {rpc.url}"
                            )
                            last_error = exceptions.BlockNotFound(
                                f"Block {block} not found at {self._network} using {rpc.url}"
                            )
                        else:
                            logging.getLogger(__name__).debug(
                                f" [1]error getting block: {block}  data using {rpc.url} rpc: {e}"
                            )
                    elif isinstance(err, str) and "not found" in err.lower():
                        logging.getLogger(__name__).error(
                            f" Block {block} not found at {self._network} using {rpc.url}"
                        )
                        last_error = exceptions.BlockNotFound(
                            f"Block {block} not found at {self._network} using {rpc.url}"
                        )
                    else:
                        #  Block with id: '0x1167a59' not found.
                        logging.getLogger(__name__).debug(
                            f" [2]error getting block: {block}  data using {rpc.url} rpc: {e}"
                        )
                        last_error = exceptions.BlockNotFound(
                            f"Block {block} not found at {self._network} using {rpc.url}"
                        )
                rpc.add_failed(error=e)

        # raise last error if there is no result to return, and there was an error
        if last_error:
            raise last_error

        return None

    def isContract(self) -> bool:
        """Check if an address corresponds to a contract or not using the contract's bytecode.
        If connection RPC errors do not let the check thru, return True
        """

        # get w3Provider list
        for rpc in RPC_MANAGER.get_rpc_list(network=self._network):
            try:
                rpc.add_attempt(method=cuType.eth_getCode)
                _w3 = self.setup_w3(network=self._network, web3Url=rpc.url)
                if contract_bytecode := self._w3.eth.get_code(
                    Web3.toChecksumAddress(self.address)
                ):
                    return True
                else:
                    return False
            except Exception as e:
                logging.getLogger(__name__).debug(
                    f" error getting contract's bytecode data using {rpc.url} rpc: {e}"
                )
                rpc.add_failed(error=e)
                continue

        logging.getLogger(__name__).error(
            f" Could not use any rpcProvider to check if {self.address} is a contract. Return true."
        )
        return True
