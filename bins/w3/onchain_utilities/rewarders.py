import contextlib
import logging
from web3 import Web3

from bins.w3.onchain_utilities.basic import erc20_cached, web3wrap


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


# rewarders


class gamma_masterchef_rewarder(gamma_rewarder):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_rewarder"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def acc_token_precision(self) -> int:
        return self._contract.functions.ACC_TOKEN_PRECISION().call(
            block_identifier=self.block
        )

    @property
    def masterchef_v2(self) -> str:
        return self._contract.functions.MASTERCHEF_V2().call(
            block_identifier=self.block
        )

    @property
    def funder(self) -> str:
        return self._contract.functions.funder().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingOwner(self) -> str:
        return self._contract.functions.pendingOwner().call(block_identifier=self.block)

    def pendingToken(self, pid: int, user: str) -> int:
        return self._contract.functions.pendingToken(pid, user).call(
            block_identifier=self.block
        )

    def pendingTokens(self, pid: int, user: str, input: int) -> tuple[list, list]:
        # rewardTokens address[], rewardAmounts uint256[]
        return self._contract.functions.pendingTokens(pid, user, input).call(
            block_identifier=self.block
        )

    def poolIds(self, input: int) -> int:
        return self._contract.functions.poolIds(input).call(block_identifier=self.block)

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
        return self._contract.functions.poolInfo(input).call(
            block_identifier=self.block
        )

    @property
    def poolLength(self) -> int:
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    @property
    def rewardPerSecond(self) -> int:
        return self._contract.functions.rewardPerSecond().call(
            block_identifier=self.block
        )

    @property
    def rewardToken(self) -> str:
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

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
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

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
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_v1"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def sushi(self) -> str:
        """The SUSHI token contract address

        Returns:
            str: token address
        """
        return self._contract.functions.SUSHI().call(block_identifier=self.block)

    def getRewarder(self, pid: int, rid: int) -> str:
        """Retrieve rewarder address from masterchef

        Args:
            pid (int): The index of the pool
            rid (int): The index of the rewarder

        Returns:
            str: address
        """
        return self._contract.functions.getRewarder(pid, rid).call(
            block_identifier=self.block
        )

    def lpToken(self, pid: int) -> str:
        """Retrieve lp token address (hypervisor) from masterchef

        Args:
            index (int): index of the pool ( same of rewarder )

        Returns:
            str:  hypervisor address ( LP token)
        """
        return self._contract.functions.lpToken(pid).call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingOwner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingSushi(self, pid: int, user: str) -> int:
        """pending SUSHI reward for a given user

        Args:
            pid (int): The index of the pool
            user (str):  address

        Returns:
            int: _description_
        """
        return self._contract.functions.pendingSushi(pid, user).call(
            block_identifier=self.block
        )

    def poolInfo(
        self,
    ) -> tuple[int, int, int]:
        """_summary_

        Returns:
            tuple[int,int,int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
        """
        return self._contract.functions.poolInfo().call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """Returns the number of MCV2 pools
        Returns:
            int:
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)


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
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_v2"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
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
        return self._contract.functions.deposited(pid, user).call(
            block_identifier=self.block
        )

    @property
    def endTimestamp(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        return self._contract.functions.endTimestamp().call(block_identifier=self.block)

    @property
    def erc20(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.erc20().call(block_identifier=self.block)

    @property
    def feeAddress(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.feeAddress().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def paidOut(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        return self._contract.functions.paidOut().call(block_identifier=self.block)

    def pending(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            int: _description_
        """
        return self._contract.functions.pending(pid, user).call(
            block_identifier=self.block
        )

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
        return self._contract.functions.poolInfo(pid).call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    @property
    def rewardPerSecond(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerSecond().call(
            block_identifier=self.block
        )

    @property
    def startTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.startTimestamp().call(
            block_identifier=self.block
        )

    @property
    def totalAllocPoint(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

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
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

    # get all rewards
    def get_rewards(
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
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_registry_v1"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
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
        return self._contract.functions.counter().call(block_identifier=self.block)

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
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

    # TODO: manage versions
    def get_masterchef_generator(self) -> gamma_masterchef_v1:
        """Retrieve masterchef contracts from registry

        Returns:
           masterchefV2 contract
        """
        total_qtty = self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                address, idx = self.hypeByIndex(index=i)

                # filter blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                yield gamma_masterchef_v1(
                    address=address,
                    network=self._network,
                    block=self.block,
                )

            except Exception:
                logging.getLogger(__name__).warning(
                    f" Masterchef registry returned the address {address} and may not be a masterchef contract ( at web3 chain id: {self._chain_id} )"
                )

    def get_masterchef_addresses(self) -> list[str]:
        """Retrieve masterchef addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                address, idx = self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(address)

        return result


# Zyberswap


class zyberswap_masterchef_rewarder(gamma_rewarder):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "zyberchef_rewarder"
        self._abi_path = abi_path or "data/abi/zyberchef/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    def _getTimeElapsed(self, _from: int, _to: int, _endTimestamp: int) -> int:
        return self._contract.functions._getTimeElapsed(_from, _to, _endTimestamp).call(
            block_identifier=self.block
        )

    def currentTimestamp(self, pid: int) -> int:
        return self._contract.functions._getTimeElapsed(pid).call(
            block_identifier=self.block
        )

    @property
    def distributorV2(self) -> str:
        return self._contract.functions.distributorV2().call(
            block_identifier=self.block
        )

    @property
    def isNative(self) -> bool:
        return self._contract.functions.isNative().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def pendingTokens(self, pid: int, user: str) -> int:
        return self._contract.functions.pendingTokens(pid, user).call(
            block_identifier=self.block
        )

    def poolIds(self, input: int) -> int:
        return self._contract.functions.poolIds(input).call(block_identifier=self.block)

    def poolInfo(self, pid: int) -> tuple[int, int, int, int, int]:
        """

        Args:
            pid (int): pool index

        Returns:
            tuple[int, int, int, int, int]:
                accTokenPerShare uint256
                startTimestamp unit256
                lastRewardTimestamp uint256
                allocPoint uint256 — allocation points assigned to the pool.
                totalRewards uint256 — total rewards for the pool
        """
        return self._contract.functions.poolInfo(pid).call(block_identifier=self.block)

    def poolRewardInfo(self, input1: int, input2: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input1 (int): _description_
            input2 (int): _description_

        Returns:
            tuple[int,int,int]:  startTimestamp uint256, endTimestamp uint256, rewardPerSec uint256
        """
        return self._contract.functions.poolRewardInfo(input1, input2).call(
            block_identifier=self.block
        )

    def poolRewardsPerSec(self, pid: int) -> int:
        return self._contract.functions.poolRewardsPerSec(pid).call(
            block_identifier=self.block
        )

    @property
    def rewardInfoLimit(self) -> int:
        return self._contract.functions.rewardInfoLimit().call(
            block_identifier=self.block
        )

    @property
    def rewardToken(self) -> str:
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

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
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

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

        result["type"] = "zyberswap"
        # result["token_precision"] = (
        #     str(self.acc_token_precision) if convert_bint else self.acc_token_precision
        # )
        result["masterchef_address"] = (self.distributorV2).lower()
        result["owner"] = (self.owner).lower()
        # result["pendingOwner"] = ""

        # result["poolLength"] = self.poolLength

        # result["rewardPerSecond"] = (
        #     str(self.rewardPerSecond) if convert_bint else self.rewardPerSecond
        # )
        result["rewardToken"] = (self.rewardToken).lower()

        result["totalAllocPoint"] = (
            str(self.totalAllocPoint) if convert_bint else self.totalAllocPoint
        )

        # only return when static mode is off
        if not static_mode:
            pass

        return result

    # get all rewards
    def get_rewards(
        self,
        pid: int,
    ) -> list[dict]:
        """Search for rewards data

        Args:
            pid (int ): pool ids linked to hypervisor.
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

        return result


class zyberswap_masterchef_v1(gamma_rewarder):
    # https://arbiscan.io/address/0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "zyberchef_v1"
        self._abi_path = abi_path or "data/abi/zyberswap/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def maximum_deposit_fee_rate(self) -> int:
        """maximum deposit fee rate

        Returns:
            int: unit16
        """
        return self._contract.functions.MAXIMUM_DEPOSIT_FEE_RATE().call(
            block_identifier=self.block
        )

    @property
    def maximum_harvest_interval(self) -> int:
        """maximum harvest interval

        Returns:
            int: unit256
        """
        return self._contract.functions.MAXIMUM_HARVEST_INTERVAL().call(
            block_identifier=self.block
        )

    def canHarvest(self, pid: int, user: str) -> bool:
        """can harvest

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            bool: _description_
        """
        return self._contract.functions.canHarvest(pid, user).call(
            block_identifier=self.block
        )

    @property
    def feeAddress(self) -> str:
        """fee address

        Returns:
            str: address
        """
        return self._contract.functions.feeAddress().call(block_identifier=self.block)

    @property
    def getZyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        return self._contract.functions.getZyberPerSec().call(
            block_identifier=self.block
        )

    @property
    def marketingAddress(self) -> str:
        """marketing address

        Returns:
            str: address
        """
        return self._contract.functions.marketingAddress().call(
            block_identifier=self.block
        )

    @property
    def marketingPercent(self) -> int:
        """marketing percent

        Returns:
            int: unit256
        """
        return self._contract.functions.marketingPercent().call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        """owner

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    def pendingTokens(
        self, pid: int, user: str
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """pending tokens

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            tuple: addresses address[], symbols string[], decimals uint256[], amounts uint256[]
        """
        return self._contract.functions.pendingTokens(pid, user).call(
            block_identifier=self.block
        )

    def poolInfo(self, pid: int) -> tuple[str, int, int, int, int, int, int, int]:
        """pool info

        Args:
            pid (int): pool id

        Returns:
            tuple:
                lpToken address,
                allocPoint uint256,
                lastRewardTimestamp uint256,
                accZyberPerShare uint256,
                depositFeeBP uint16,
                harvestInterval uint256,
                totalLp uint256
        """
        return self._contract.functions.poolInfo(pid).call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """pool length

        Returns:
            int: unit256
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    def poolRewarders(self, pid: int) -> list[str]:
        """pool rewarders

        Args:
            pid (int): pool id

        Returns:
            list[str]: address[]
        """
        return self._contract.functions.poolRewarders(pid).call(
            block_identifier=self.block
        )

    def poolRewardsPerSec(
        self, pid: int
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """pool rewards per sec

        Args:
            pid (int): pool id

        Returns:
            tuple: addresses address[],
            symbols string[],
            decimals uint256[],
            rewardsPerSec uint256[]
        """
        return self._contract.functions.poolRewardsPerSec(pid).call(
            block_identifier=self.block
        )

    def poolTotalLp(self, pid: int) -> int:
        """pool total lp

        Args:
            pid (int): pool id

        Returns:
            int: unit256
        """
        return self._contract.functions.poolTotalLp(pid).call(
            block_identifier=self.block
        )

    @property
    def startTimestamp(self) -> int:
        """start timestamp

        Returns:
            int: unit256
        """
        return self._contract.functions.startTimestamp().call(
            block_identifier=self.block
        )

    @property
    def teamAddress(self) -> str:
        """team address

        Returns:
            str: address
        """
        return self._contract.functions.teamAddress().call(block_identifier=self.block)

    @property
    def teamPercent(self) -> int:
        """team percent

        Returns:
            int: unit256
        """
        return self._contract.functions.teamPercent().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """total alloc point

        Returns:
            int: unit256
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

    @property
    def totalLockedUpRewards(self) -> int:
        """total locked up rewards

        Returns:
            int: unit256
        """
        return self._contract.functions.totalLockedUpRewards().call(
            block_identifier=self.block
        )

    @property
    def totalZyberInPools(self) -> int:
        """total zyber in pools

        Returns:
            int: unit256
        """
        return self._contract.functions.totalZyberInPools().call(
            block_identifier=self.block
        )

    def userInfo(self, pid: int, user: str) -> tuple[int, int, int, int]:
        """user info

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            tuple:
                amount uint256,
                rewardDebt uint256,
                rewardLockedUp uint256,
                nextHarvestUntil uint256
        """
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

    @property
    def zyber(self) -> str:
        """zyber

        Returns:
            str: address
        """
        return self._contract.functions.zyber().call(block_identifier=self.block)

    @property
    def zyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        return self._contract.functions.zyberPerSec().call(block_identifier=self.block)

    # get all rewards
    def get_rewards(
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
            # lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
            pinfo = self.poolInfo(pid)

            if not hypervisor_addresses or pinfo[0].lower() in hypervisor_addresses:
                # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
                poolRewardsPerSec = self.poolRewardsPerSec(pid)

                # get rewards data
                rewards = {}
                for address, symbol, decimals, rewardsPerSec in zip(
                    poolRewardsPerSec[0],
                    poolRewardsPerSec[1],
                    poolRewardsPerSec[2],
                    poolRewardsPerSec[3],
                ):
                    if rewardsPerSec:
                        result.append(
                            {
                                "network": self._network,
                                "block": self.block,
                                "timestamp": self._timestamp,
                                "hypervisor_address": pinfo[0].lower(),
                                "rewarder_address": self.address,
                                "rewarder_type": "zyberswap_masterchef_v1",
                                "rewarder_refIds": [pid],
                                "rewardToken": address,
                                "rewardToken_symbol": symbol,
                                "rewardToken_decimals": decimals,
                                "rewards_perSecond": rewardsPerSec,
                                "total_hypervisorToken_qtty": pinfo[6],
                            }
                        )

        return result


# Duo
class duo_masterchef_v1(gamma_rewarder):
    # TODO: https://arbiscan.io/address/0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba#code

    # pools affected:
    #        wide pool pid 25   0xD75faCEC47A40b29522FA2515AAf269a9Ce7049e
    #        narrow pool pid 26 0xEF207FbF72710021a838935a6574e62CFfAa7C10
    pass


# Thena


class thena_voter_v3(web3wrap):
    # https://bscscan.com/address/0x374cc2276b842fecd65af36d7c60a5b78373ede1#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "voterV3"
        self._abi_path = abi_path or "data/abi/thena/binance"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def max_vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.MAX_VOTE_DELAY().call(
            block_identifier=self.block
        )

    @property
    def vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.VOTE_DELAY().call(block_identifier=self.block)

    @property
    def _epochTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions._epochTimestamp().call(
            block_identifier=self.block
        )

    @property
    def _factories(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: address[]
        """
        return self._contract.functions._factories().call(block_identifier=self.block)

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions._ve().call(block_identifier=self.block)

    @property
    def bribefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.bribefactory().call(block_identifier=self.block)

    def claimable(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.claimable(address).call(
            block_identifier=self.block
        )

    def external_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self._contract.functions.external_bribes(address).call(
            block_identifier=self.block
        )

    def factories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.factories(index).call(
            block_identifier=self.block
        )

    @property
    def factory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.factory().call(block_identifier=self.block)

    @property
    def factoryLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.factoryLength().call(
            block_identifier=self.block
        )

    def gaugeFactories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.gaugeFactories(index).call(
            block_identifier=self.block
        )

    @property
    def gaugeFactoriesLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.gaugeFactoriesLength().call(
            block_identifier=self.block
        )

    @property
    def gaugefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.gaugefactory().call(block_identifier=self.block)

    def gauges(self, address: str) -> str:
        """_summary_

        Args:
            address (str):

        Returns:
            str: address
        """
        return self._contract.functions.gauges(Web3.toChecksumAddress(address)).call(
            block_identifier=self.block
        )

    def gaugesDistributionTimestamp(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.gaugesDistributionTimestamp(address).call(
            block_identifier=self.block
        )

    def internal_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self._contract.functions.internal_bribes(address).call(
            block_identifier=self.block
        )

    @property
    def isAlive(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        return self._contract.functions.isAlive().call(block_identifier=self.block)

    def isFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isFactory(address).call(
            block_identifier=self.block
        )

    def isGauge(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isGauge(address).call(
            block_identifier=self.block
        )

    def isGaugeFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isGaugeFactory(address).call(
            block_identifier=self.block
        )

    def isWhitelisted(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isWhitelisted(address).call(
            block_identifier=self.block
        )

    def lastVoted(self, index: int) -> int:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.lastVoted(index).call(
            block_identifier=self.block
        )

    @property
    def length(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.length().call(block_identifier=self.block)

    @property
    def minter(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.minter().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def permissionRegistry(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.permissionRegistry().call(
            block_identifier=self.block
        )

    def poolForGauge(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self._contract.functions.poolForGauge(address).call(
            block_identifier=self.block
        )

    def poolVote(self, input1: int, input2: int) -> str:
        """_summary_

        Args:
            input1 (int): uint256
            input2 (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.poolVote(input1, input2).call(
            block_identifier=self.block
        )

    def poolVoteLength(self, tokenId: int) -> int:
        """_summary_

        Args:
            tokenId (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.poolVoteLength(tokenId).call(
            block_identifier=self.block
        )

    def pools(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.pools(index).call(block_identifier=self.block)

    @property
    def totalWeight(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.totalWeight().call(block_identifier=self.block)

    def totalWeightAt(self, time: int) -> int:
        """_summary_

        Args:
            time (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.totalWeightAt(time).call(
            block_identifier=self.block
        )

    def usedWeights(self, index: int) -> int:
        """_summary_

        Args:
            index (int)

        Returns:
            int: uint256
        """
        return self._contract.functions.usedWeights(index).call(
            block_identifier=self.block
        )

    def votes(self, index: int, address: str) -> int:
        """_summary_

        Args:
            index (int): uint256
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.votes(index, address).call(
            block_identifier=self.block
        )

    def weights(self, pool_address: str) -> int:
        """_summary_

        Args:
            pool_address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.weights(pool_address).call(
            block_identifier=self.block
        )

    def weightsAt(self, pool_address: str, time: int) -> int:
        """_summary_

        Args:
            pool_address (str): address
            time (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.weightsAt(pool_address, time).call(
            block_identifier=self.block
        )

    # custom functions
    # get all rewards
    def get_rewards(
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

        if hypervisor_addresses:
            for hypervisor_address in hypervisor_addresses:
                # get managing gauge from hype address
                gauge_address = self.gauges(address=hypervisor_address)
                if gauge_address != "0x0000000000000000000000000000000000000000":
                    # build a gauge
                    thena_gauge = thena_gauge_v2(
                        address=gauge_address, network=self._network, block=self.block
                    )
                    rewardRate = thena_gauge.rewardRate
                    rewardToken = thena_gauge.rewardToken
                    totalSupply = thena_gauge.totalSupply

                    # build reward token instance
                    reward_token_instance = erc20_cached(
                        address=rewardToken,
                        network=self._network,
                        block=self.block,
                    )
                    # get reward token data
                    rewardToken_symbol = reward_token_instance.symbol
                    rewardToken_decimals = reward_token_instance.decimals

                    result.append(
                        {
                            "network": self._network,
                            "block": self.block,
                            "timestamp": self._timestamp,
                            "hypervisor_address": hypervisor_address.lower(),
                            "rewarder_address": gauge_address,
                            "rewarder_type": "thena_gauge_v2",
                            "rewarder_refIds": [],
                            "rewardToken": rewardToken,
                            "rewardToken_symbol": rewardToken_symbol,
                            "rewardToken_decimals": rewardToken_decimals,
                            "rewards_perSecond": rewardRate,
                            "total_hypervisorToken_qtty": totalSupply,
                        }
                    )

        else:
            # TODO: get all hypervisors data ... by pid
            raise NotImplementedError("Not implemented yet")

        return result


class thena_gauge_v2(gamma_rewarder):
    # https://bscscan.com/address/0x0C83DbCdf4a43F5F015Bf65C0761024D328F3776#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "gaugeV2_CL"
        self._abi_path = abi_path or "data/abi/thena/binance"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def distribution(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.DISTRIBUTION().call(block_identifier=self.block)

    @property
    def duration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.DURATION().call(block_identifier=self.block)

    @property
    def token(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.TOKEN().call(block_identifier=self.block)

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions._VE().call(block_identifier=self.block)

    def _balances(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions._balances(address).call(
            block_identifier=self.block
        )

    @property
    def _periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions._periodFinish().call(
            block_identifier=self.block
        )

    @property
    def _totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions._totalSupply().call(block_identifier=self.block)

    def balanceOf(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.balanceOf(address).call(
            block_identifier=self.block
        )

    def earned(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.earned(address).call(
            block_identifier=self.block
        )

    @property
    def emergency(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        return self._contract.functions.emergency().call(block_identifier=self.block)

    @property
    def external_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.external_bribe().call(
            block_identifier=self.block
        )

    @property
    def feeVault(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.feeVault().call(block_identifier=self.block)

    @property
    def fees0(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.fees0().call(block_identifier=self.block)

    @property
    def fees1(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.fees1().call(block_identifier=self.block)

    @property
    def gaugeRewarder(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.gaugeRewarder().call(
            block_identifier=self.block
        )

    @property
    def internal_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.internal_bribe().call(
            block_identifier=self.block
        )

    @property
    def lastTimeRewardApplicable(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.lastTimeRewardApplicable().call(
            block_identifier=self.block
        )

    @property
    def lastUpdateTime(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.lastUpdateTime().call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.periodFinish().call(block_identifier=self.block)

    @property
    def rewardPerDuration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerDuration().call(
            block_identifier=self.block
        )

    @property
    def rewardPerToken(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerToken().call(
            block_identifier=self.block
        )

    @property
    def rewardPerTokenStored(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerTokenStored().call(
            block_identifier=self.block
        )

    @property
    def rewardRate(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardRate().call(block_identifier=self.block)

    @property
    def rewardToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def rewardPid(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPid().call(block_identifier=self.block)

    def rewards(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.rewards(address).call(
            block_identifier=self.block
        )

    @property
    def totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.totalSupply().call(block_identifier=self.block)

    def userRewardPerTokenPaid(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.userRewardPerTokenPaid(address).call(
            block_identifier=self.block
        )
