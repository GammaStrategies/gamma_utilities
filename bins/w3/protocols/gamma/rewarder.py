import contextlib
import logging

from web3 import Web3
from bins.general.enums import Chain, text_to_chain

from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls
from ..base_wrapper import web3wrap
from ..general import erc20_cached


class gamma_rewarder(web3wrap):
    # Custom conversion
    def convert_to_status(self) -> dict:
        """Convert rewarder to areward status format

        Returns:
            dict:       network: str
                        block: int
                        timestamp: int
                        hypervisor_address: str
                        rewarder_address: str
                        rewarder_type: str
                        rewarder_refIds: list[str]
                        rewardToken: str
                        rewardToken_symbol: str
                        rewardToken_decimals: int
                        rewards_perSecond: int
                        total_hypervisorToken_qtty: int
        """
        return {}


class gamma_masterchef_rewarder(gamma_rewarder):
    # uniswapv3
    "https://polygonscan.com/address/0x4d7A374Fce77eec67b3a002549a3A49DEeC9307C#readContract"

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
        self._abi_filename = abi_filename or "masterchef_rewarder"
        self._abi_path = abi_path or f"{self.abi_root_path}/gamma/masterchef"

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

    @property
    def acc_token_precision(self) -> int:
        return self.call_function_autoRpc("ACC_TOKEN_PRECISION")

    @property
    def masterchef_v2(self) -> str:
        return self.call_function_autoRpc("MASTERCHEF_V2")

    @property
    def funder(self) -> str:
        return self.call_function_autoRpc("funder")

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    @property
    def pendingOwner(self) -> str:
        return self.call_function_autoRpc("pendingOwner")

    def pendingToken(self, pid: int, user: str) -> int:
        return self.call_function_autoRpc("pendingToken", None, pid, user)

    def pendingTokens(self, pid: int, user: str, input: int) -> tuple[list, list]:
        # rewardTokens address[], rewardAmounts uint256[]
        return self.call_function_autoRpc("pendingTokens", None, pid, user, input)

    def poolIds(self, input: int) -> int:
        return self.call_function_autoRpc("poolIds", None, input)

    def poolInfo(self, input: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input (int): _description_

        Returns:
            tuple[int, int, int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
                accSushiPerShare — accumulated SUSHI per share, times 1e12.
                lastRewardBlock — number of block, when the reward in the pool was the last time calculated
                allocPoint — allocation points assigned to the pool. SUSHI to distribute per block per pool = SUSHI per block * pool.allocPoint / totalAllocPoint
        """
        return self.call_function_autoRpc("poolInfo", None, input)

    @property
    def poolLength(self) -> int:
        return self.call_function_autoRpc("poolLength")

    @property
    def rewardPerSecond(self) -> int:
        return self.call_function_autoRpc("rewardPerSecond")

    @property
    def rewardToken(self) -> str:
        return self.call_function_autoRpc("rewardToken")

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self.call_function_autoRpc("totalAllocPoint")

    def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user

        """
        return self.call_function_autoRpc("userInfo", None, pid, user)

    # CUSTOM
    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["type"] = "gamma"

        result["token_precision"] = (
            str(self.acc_token_precision) if convert_bint else self.acc_token_precision
        )
        result["masterchef_address"] = (self.masterchef_v2).lower()
        result["owner"] = (self.owner).lower()
        result["pendingOwner"] = (self.pendingOwner).lower()

        result["poolLength"] = self.poolLength

        result["rewardPerSecond"] = (
            str(self.rewardPerSecond) if convert_bint else self.rewardPerSecond
        )
        result["rewardToken"] = (self.rewardToken).lower()

        result["totalAllocPoint"] = (
            str(self.totalAllocPoint) if convert_bint else self.totalAllocPoint
        )

        # only return when static mode is off
        if not static_mode:
            pass

        return result


# Gamma
# rewarder registry
class gamma_masterchef_v1(gamma_rewarder):
    # https://optimistic.etherscan.io/address/0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365#readContract

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
        self._abi_filename = abi_filename or "masterchef_v1"
        self._abi_path = abi_path or f"{self.abi_root_path}/gamma/masterchef"

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

    @property
    def sushi(self) -> str:
        """The SUSHI token contract address

        Returns:
            str: token address
        """
        return self.call_function_autoRpc("SUSHI")

    def getRewarder(self, pid: int, rid: int) -> str:
        """Retrieve rewarder address from masterchef

        Args:
            pid (int): The index of the pool
            rid (int): The index of the rewarder

        Returns:
            str: address
        """
        return self.call_function_autoRpc("getRewarder", None, pid, rid)

    def lpToken(self, pid: int) -> str:
        """Retrieve lp token address (hypervisor) from masterchef

        Args:
            index (int): index of the pool ( same of rewarder )

        Returns:
            str:  hypervisor address ( LP token)
        """
        return self.call_function_autoRpc("lpToken", None, pid)

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    @property
    def pendingOwner(self) -> str:
        return self.call_function_autoRpc("pendingOwner")

    def pendingSushi(self, pid: int, user: str) -> int:
        """pending SUSHI reward for a given user

        Args:
            pid (int): The index of the pool
            user (str):  address

        Returns:
            int: _description_
        """
        return self.call_function_autoRpc("pendingSushi", None, pid, user)

    def poolInfo(
        self,
        pid: int,
    ) -> tuple[int, int, int]:
        """_summary_

        Returns:
            tuple[int,int,int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
        """
        return self.call_function_autoRpc("poolInfo", None, pid)

    @property
    def poolLength(self) -> int:
        """Returns the number of MCV2 pools
        Returns:
            int:
        """
        return self.call_function_autoRpc("poolLength")

    # multicall version will only work if the lenght of getRewarder and lpToken are the same ( not always true https://polygonscan.com/address/0x5cA8b7EB3222E7CE6864E59807dDd1A3c3073826#readContract)
    def get_rewarders_donotUSE(self, rid: int = 0) -> dict:
        """Get all rewarder addresses and lpTokens from the masterchef

        Returns:
            dict: {<index>: {"rewarder_address": str, "lpToken": str}
        """
        chain = text_to_chain(self._network)
        _max_calls_atOnce = 50

        # 1) poolLength: qtty of rewarders n LPtokens to get
        # 2) getRewarder(pool idx, rewarder idx) :  call to get rewarder address for index in range(poolLength)
        #   lpToken(pool idx) :  call to get hypervisor address for index in range(poolLength)

        # OPTIONAL 3) filter rewarder_addresses by checking lptoken is a valid hypervisor

        # 4) get from each rewarer: rewardToken and rewardsPerSecond

        pool_length = self.poolLength

        # call 2  get rewarder addresses
        abi_part_getRewarder = self.get_abi_function("getRewarder")
        abi_part_lpToken = self.get_abi_function("lpToken")
        factory_calls = [
            build_call_with_abi_part(
                abi_part=abi_part_getRewarder,
                inputs_values=[idx, rid],
                address=self.address,
                object="masterchef",
            )
            for idx in range(pool_length)
        ] + [
            build_call_with_abi_part(
                abi_part=abi_part_lpToken,
                inputs_values=[idx],
                address=self.address,
                object="masterchef",
            )
            for idx in range(pool_length)
        ]
        #   place call:  build address list if ...[1] != 0
        result = {}
        discard_indexes = []
        for i in range(0, len(factory_calls), _max_calls_atOnce):
            # execute calls
            for _item in execute_parse_calls(
                network=chain.database_name,
                block=self.block,
                calls=factory_calls[i : i + _max_calls_atOnce],
                convert_bint=False,
            ):
                if _item["name"] == "getRewarder":
                    _input_index = _item["inputs"][0]["value"]
                    # No value can happen... discard.
                    if "value" not in _item["outputs"][0]:
                        discard_indexes.append(_input_index)
                        if _input_index in result:
                            del result[_input_index]
                        continue
                    # discard if needed
                    if _input_index in discard_indexes:
                        continue

                    # process rewarder address
                    _rewarder_address = _item["outputs"][0]["value"].lower()

                    if not _input_index in result:
                        result[_input_index] = {"rewarder_address": _rewarder_address}
                    else:
                        result[_input_index]["rewarder_address"] = _rewarder_address

                elif _item["name"] == "lpToken":
                    _input_index = _item["inputs"][0]["value"]
                    # discard if needed
                    if _input_index in discard_indexes:
                        continue
                    # No value can happen... discard.
                    if "value" not in _item["outputs"][0]:
                        discard_indexes.append(_input_index)
                        if _input_index in result:
                            del result[_input_index]
                        continue
                    # process lp token address
                    _lptoken_address = _item["outputs"][0]["value"].lower()
                    if not _input_index in result:
                        result[_input_index] = {"lpToken": _lptoken_address}
                    else:
                        result[_input_index]["lpToken"] = _lptoken_address

                else:
                    raise ValueError(f" not identifiable call: {_item} ")

        # return non blacklisted addresses
        return result

    def get_rewarders(self, rid: int = 0) -> dict:
        """Get a dict of rewarder addresses and lpTokens from the masterchef

        Args:
            rid (int, optional): _description_. Defaults to 0.

        Returns:
            dict: <rewarder address> : <lp token address>
        """

        ## if x: = get poolInfo(i) for i in range(1000)
        #       try get rewarder address and lpToken address
        ##      if both are valid, add to result

        _max_loop = 1000
        # not needed. delete
        pool_length = self.poolLength
        result = {}
        _processed = 0
        _looped = 0
        # call getRewarder(pool idx, rid)
        for i in range(_max_loop):
            if _processed >= pool_length:
                break

            rewarder_address = None
            lpToken_address = None
            try:
                rewarder_address = self.getRewarder(i, rid)
            except Exception as e:
                # this is actually a rewarder when no getRewarder function is found
                pass
            try:
                lpToken_address = self.lpToken(i)
                _processed += 1
            except Exception as e:
                pass

            if rewarder_address and lpToken_address:
                result[rewarder_address.lower()] = lpToken_address.lower()

            _looped += 1

        if _looped >= _max_loop:
            logging.getLogger(__name__).warning(
                f" masterchef get_rewarders: MAXED OUT the looped times {_looped}. Consider increasing the loop limit."
            )

        return result


# Gamma's Quickswap masterchef v2 ( Farmv3 )
class gamma_masterchef_v2(gamma_rewarder):
    # https://polygonscan.com/address/0xcc54afcecd0d89e0b2db58f5d9e58468e7ad20dc#readContract

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
        self._abi_filename = abi_filename or "masterchef_v2"
        self._abi_path = abi_path or f"{self.abi_root_path}/gamma/masterchef"

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

    def deposited(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): _description_
            user (str): _description_

        Returns:
            int: _description_
        """
        return self.call_function_autoRpc("deposited", None, pid, user)

    @property
    def endTimestamp(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        return self.call_function_autoRpc("endTimestamp")

    @property
    def erc20(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("erc20")

    @property
    def feeAddress(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("feeAddress")

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("owner")

    @property
    def paidOut(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        return self.call_function_autoRpc("paidOut")

    def pending(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            int: _description_
        """
        return self.call_function_autoRpc("pending", None, pid, user)

    def poolInfo(self, pid: int) -> tuple[str, int, int, int, int]:
        """_summary_

        Args:
            pid (int): pool index

        Returns:
            tuple:
                lpToken address,
                allocPoint uint256,
                lastRewardTimestamp uint256,
                accERC20PerShare uint256,
                depositFeeBP uint16
        """
        return self.call_function_autoRpc("poolInfo", None, pid)

    @property
    def poolLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("poolLength")

    @property
    def rewardPerSecond(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("rewardPerSecond")

    @property
    def startTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("startTimestamp")

    @property
    def totalAllocPoint(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("totalAllocPoint")

    def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            tuple:
                amount uint256,
                rewardDebt uint256
        """
        return self.call_function_autoRpc("userInfo", None, pid, user)

    def get_rewards_donotUSE(
        self,
        hypervisor_addresses: list[str] | None = None,
        pids: list[int] | None = None,
    ) -> list[dict]:
        """Search for rewards data


        Args:
            hypervisor_addresses (list[str] | None, optional): list of lower case hypervisor addresses. When defaults to None, all rewarded hypes ( gamma or non gamma) will be returned.
            pids (list[int] | None, optional): pool ids linked to hypervisor. When defaults to None, all pools will be returned.
        Returns:
            list[dict]: network: str
                        block: int
                        timestamp: int
                        hypervisor_address: str
                        rewarder_address: str
                        rewarder_type: str
                        rewarder_refIds: list[str]
                        rewardToken: str
                        rewardToken_symbol: str
                        rewardToken_decimals: int
                        rewards_perSecond: int
                        total_hypervisorToken_qtty: int
        """
        result = []

        for pid in pids or range(self.poolLength):
            # lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accerc20PerShare uint256, depositFeeBP uint16
            pinfo = self.poolInfo(pid)
            hypervisor_address = pinfo[0].lower()

            if not hypervisor_addresses or hypervisor_address in hypervisor_addresses:
                # build reward token instance
                rewardToken = self.erc20
                reward_token_instance = erc20_cached(
                    address=rewardToken,
                    network=self._network,
                    block=self.block,
                )
                # get reward token data
                rewardToken_symbol = reward_token_instance.symbol
                rewardToken_decimals = reward_token_instance.decimals

                # simplify access to vars
                alloc_point = pinfo[1]
                accerc20_per_share = pinfo[3] / (10**rewardToken_decimals)
                total_alloc_point = self.totalAllocPoint

                # transform reward per second to decimal
                rewardsPerSec = self.rewardPerSecond / (10**rewardToken_decimals)
                weighted_rewardsPerSec = (
                    (rewardsPerSec * (alloc_point / total_alloc_point))
                    if total_alloc_point
                    else 0
                )

                # try get balance of hypervisor token
                masterchef_as_erc20 = erc20_cached(
                    address=self.address, network=self._network, block=self.block
                )
                total_hypervisorToken_qtty = masterchef_as_erc20.balanceOf(
                    address=hypervisor_address
                )

                result.append(
                    {
                        "network": self._network,
                        "block": self.block,
                        "timestamp": self._timestamp,
                        "hypervisor_address": hypervisor_address,
                        "rewarder_address": self.address,
                        "rewarder_type": "gamma_masterchef_v2",
                        "rewarder_refIds": [pid],
                        "rewardToken": rewardToken,
                        "rewardToken_symbol": rewardToken_symbol,
                        "rewardToken_decimals": rewardToken_decimals,
                        "rewards_perSecond": weighted_rewardsPerSec,
                        "rewards_perShare": accerc20_per_share,
                        "total_hypervisorToken_qtty": total_hypervisorToken_qtty,
                    }
                )

        return result

    # multicall version
    def get_rewarders(self, rid: int = 0) -> dict:
        """Get all rewarder addresses and lpTokens from the masterchef

        Returns:
            dict: {<index>: {"rewarder_address": str, "lpToken": str}
        """
        chain = text_to_chain(self._network)
        _max_calls_atOnce = 1000

        # 1) poolLength: qtty of rewarders n LPtokens to get
        # 2) getRewarder(pool idx, rewarder idx) :  call to get rewarder address for index in range(poolLength)
        #   lpToken(pool idx) :  call to get hypervisor address for index in range(poolLength)

        # OPTIONAL 3) filter rewarder_addresses by checking lptoken is a valid hypervisor

        # 4) get from each rewarer: rewardToken and rewardsPerSecond

        pool_length = self.poolLength

        # call 2  get rewarder addresses
        abi_part_getRewarder = self.get_abi_function("getRewarder")
        abi_part_lpToken = self.get_abi_function("lpToken")
        factory_calls = [
            build_call_with_abi_part(
                abi_part=abi_part_getRewarder,
                inputs_values=[idx, rid],
                address=self.address,
                object="masterchef",
            )
            for idx in range(pool_length)
        ] + [
            build_call_with_abi_part(
                abi_part=abi_part_lpToken,
                inputs_values=[idx],
                address=self.address,
                object="masterchef",
            )
            for idx in range(pool_length)
        ]
        #   place call:  build address list if ...[1] != 0
        result = {}
        for i in range(0, len(factory_calls), _max_calls_atOnce):
            # execute calls
            for _item in execute_parse_calls(
                network=chain.database_name,
                block=self.block,
                calls=factory_calls[i : i + _max_calls_atOnce],
                convert_bint=False,
            ):
                if _item["name"] == "getRewarder":
                    _rewarder_address = _item["outputs"][0]["value"].lower()
                    _input_index = _item["inputs"][0]["value"]
                    if not _input_index in result:
                        result[_input_index] = {"rewarder_address": _rewarder_address}
                    else:
                        result[_input_index]["rewarder_address"] = _rewarder_address

                elif _item["name"] == "lpToken":
                    _lptoken_address = _item["outputs"][0]["value"].lower()
                    _input_index = _item["inputs"][0]["value"]
                    if not _input_index in result:
                        result[_input_index] = {"lpToken": _rewarder_address}
                    else:
                        result[_input_index]["lpToken"] = _rewarder_address

                else:
                    raise ValueError(f" not identifiable call: {_item} ")

        # return non blacklisted addresses
        return result


# masterchef registry ( registry of the "rewarders registry")
class gamma_masterchef_registry(web3wrap):
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
        self._abi_filename = abi_filename or "masterchef_registry_v1"
        self._abi_path = abi_path or f"{self.abi_root_path}/gamma/masterchef"

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

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {}

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value-1

        Returns:
            int: positions of hypervisors in registry
        """
        return self.call_function_autoRpc("counter")

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """SOME (at least one) DEPLOYED CONTRACTS DO NOT HAVE THIS FUNCTION ( fantom )
            Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return self.call_function_autoRpc("hypeByIndex", None, index)

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    def registry(self, index: int) -> str:
        return self.call_function_autoRpc("registry", None, index)

    def registryMap(self, address: str) -> int:
        return self.call_function_autoRpc(
            "registryMap", None, Web3.toChecksumAddress(address)
        )

    # CUSTOM FUNCTIONS

    def get_masterchef_addresses_deprecated(self) -> list[str]:
        """Retrieve masterchef addresses from registry

        Returns:
           list of addresses
        """

        total_masterchefs_qtty = self.counter
        addresses_scraped = []
        disabled = []
        # retrieve all valid hypervisors addresses
        # loop until all hypervisors have been retrieved ( no while loop to avoid infinite loop)
        for i in range(10000):
            # exit
            if len(addresses_scraped) >= total_masterchefs_qtty:
                break

            with contextlib.suppress(Exception):
                address, idx = self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    disabled.append(address.lower())
                    continue

                addresses_scraped.append(address.lower())

        return addresses_scraped

    def get_masterchef_addresses(self) -> list[str]:
        """Retrieve masterchef addresses from registry, using the REGISTRY function

        Returns:
           list of addresses
        """

        total_masterchefs_qtty = self.counter
        qtty_addresses_scraped = 0
        result = []
        # retrieve all valid hypervisors addresses
        # loop until all hypervisors have been retrieved ( no while loop to avoid infinite loop)
        for i in range(10000):
            # exit
            if qtty_addresses_scraped >= total_masterchefs_qtty:
                break

            with contextlib.suppress(Exception):
                if address := self.registry(index=i):
                    qtty_addresses_scraped += 1
                    # filter blacklisted hypes
                    if (
                        self._network in self.__blacklist_addresses
                        and address.lower() in self.__blacklist_addresses[self._network]
                    ):
                        # hypervisor is blacklisted: loop
                        continue

                    result.append(address.lower())

        return result

    def get_masterchef_addresses_multicall(self) -> list[str]:
        """Retrieve masterchef addresses from registry

        Returns:
           list of addresses
        """

        addresses_scraped = []
        disabled = []

        chain = text_to_chain(self._network)
        _max_calls_atOnce = 20

        # call 2  get all addresses
        abi_part = self.get_abi_function("hypeByIndex")
        factory_calls = [
            build_call_with_abi_part(
                abi_part=abi_part,
                inputs_values=[idx],
                address=self.address,
                object="registry",
            )
            for idx in range(300)
        ]
        #   place call:  build address list if ...[1] != 0
        for i in range(0, len(factory_calls), _max_calls_atOnce):
            # execute calls
            for _item in execute_parse_calls(
                network=chain.database_name,
                block=self.block,
                calls=factory_calls[i : i + _max_calls_atOnce],
                convert_bint=False,
                requireSuccess=False,
            ):
                if not _item["outputs"]:
                    continue
                # when 0, hype is disabled ( if no value, treat as potential end)
                if _item["outputs"][1].get("value", 0) != 0:
                    addresses_scraped.append(_item["outputs"][0]["value"].lower())
                else:
                    # disabled or failed
                    disabled.append(_item["outputs"][0]["value"].lower())

        # return non blacklisted addresses
        return [
            x
            for x in addresses_scraped
            if x not in self.__blacklist_addresses.get(self._network, [])
        ]
