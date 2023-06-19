import logging
from web3 import Web3
from bins.general.enums import rewarderType

from bins.w3.protocols.gamma.rewarder import gamma_rewarder


class zyberswap_masterchef_rewarder(gamma_rewarder):
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
        self._abi_filename = abi_filename or "zyberchef_rewarder"
        self._abi_path = abi_path or "data/abi/zyberswap/masterchef"

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

    def _getTimeElapsed(self, _from: int, _to: int, _endTimestamp: int) -> int:
        return self.call_function_autoRpc(
            "_getTimeElapsed", None, _from, _to, _endTimestamp
        )

    def currentTimestamp(self, pid: int) -> int:
        return self.call_function_autoRpc("currentTimestamp", None, pid)

    @property
    def distributorV2(self) -> str:
        return self.call_function_autoRpc("distributorV2")

    @property
    def isNative(self) -> bool:
        return self.call_function_autoRpc("isNative")

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    def pendingTokens(self, pid: int, user: str) -> int:
        return self.call_function_autoRpc(
            "pendingTokens", None, pid, Web3.toChecksumAddress(user)
        )

    def poolIds(self, input: int) -> int:
        return self.call_function_autoRpc("poolIds", None, input)

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
        return self.call_function_autoRpc("poolInfo", None, pid)

    def poolRewardInfo(self, input1: int, input2: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input1 (int): _description_
            input2 (int): _description_

        Returns:
            tuple[int,int,int]:  startTimestamp uint256, endTimestamp uint256, rewardPerSec uint256
        """
        return self.call_function_autoRpc("poolRewardInfo", None, input1, input2)

    def poolRewardsPerSec(self, pid: int) -> int:
        return self.call_function_autoRpc("poolRewardsPerSec", None, pid)

    @property
    def rewardInfoLimit(self) -> int:
        return self.call_function_autoRpc("rewardInfoLimit")

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
        pids: list[int] | None = None,
        convert_bint: bool = False,
    ) -> list[dict]:
        """Search for rewards data

        Args:
            pids (list[int] | None, optional): pool ids linked to hypervisor. One pool id normally
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
        Returns:
            list[dict]: network: str
                        block: int
                        timestamp: int

                        rewarder_address: str
                        rewarder_type: str
                        rewarder_refIds: list[str]
                        rewardToken: str

                        rewards_perSecond: int
        """
        result = []

        for pid in pids:
            try:
                poolRewardsPerSec = self.poolRewardsPerSec(pid)

                # get rewards data
                result.append(
                    {
                        # "network": self._network,
                        "block": self.block,
                        "timestamp": self._timestamp,
                        # "hypervisor_address": pinfo[0].lower(), # there is no hype address in this contract
                        "rewarder_address": self.address.lower(),
                        "rewarder_type": rewarderType.ZYBERSWAP_masterchef_v1_rewarder,
                        "rewarder_refIds": [pid],
                        "rewarder_registry": self.address.lower(),
                        "rewardToken": self.rewardToken.lower(),
                        # "rewardToken_symbol": symbol,
                        # "rewardToken_decimals": decimals,
                        "rewards_perSecond": str(poolRewardsPerSec)
                        if convert_bint
                        else poolRewardsPerSec,
                        # "total_hypervisorToken_qtty": str(pinfo[6])
                        # if convert_bint
                        # else pinfo[6],
                    }
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error encountered while constructing zyberswap rewards -> {e}"
                )

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
        timestamp: int = 0,
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
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def maximum_deposit_fee_rate(self) -> int:
        """maximum deposit fee rate

        Returns:
            int: unit16
        """
        return self.call_function_autoRpc("MAXIMUM_DEPOSIT_FEE_RATE")

    @property
    def maximum_harvest_interval(self) -> int:
        """maximum harvest interval

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("MAXIMUM_HARVEST_INTERVAL")

    def canHarvest(self, pid: int, user: str) -> bool:
        """can harvest

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            bool: _description_
        """
        return self.call_function_autoRpc("canHarvest", None, pid, user)

    @property
    def feeAddress(self) -> str:
        """fee address

        Returns:
            str: address
        """
        return self.call_function_autoRpc("feeAddress")

    @property
    def getZyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("getZyberPerSec")

    @property
    def marketingAddress(self) -> str:
        """marketing address

        Returns:
            str: address
        """
        return self.call_function_autoRpc("marketingAddress")

    @property
    def marketingPercent(self) -> int:
        """marketing percent

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("marketingPercent")

    @property
    def owner(self) -> str:
        """owner

        Returns:
            str: address
        """
        return self.call_function_autoRpc("owner")

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
        return self.call_function_autoRpc("pendingTokens", None, pid, user)

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
        return self.call_function_autoRpc("poolInfo", None, pid)

    @property
    def poolLength(self) -> int:
        """pool length

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("poolLength")

    def poolRewarders(self, pid: int) -> list[str]:
        """pool rewarders

        Args:
            pid (int): pool id

        Returns:
            list[str]: address[]
        """
        return self.call_function_autoRpc("poolRewarders", None, pid)

    def poolRewardsPerSec(
        self, pid: int
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """pool rewards per sec
             first item is always ZYB ( without pool rewarder bc it is directly rewarded by the masterchef)
             subsequent items have pool rewarder ( when calling poolRewarders(pid))

        Args:
            pid (int): pool id

        Returns:
            tuple: addresses address[],
            symbols string[],
            decimals uint256[],
            rewardsPerSec uint256[]
        """
        return self.call_function_autoRpc("poolRewardsPerSec", None, pid)

    def poolTotalLp(self, pid: int) -> int:
        """pool total lp

        Args:
            pid (int): pool id

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("poolTotalLp", None, pid)

    @property
    def startTimestamp(self) -> int:
        """start timestamp

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("startTimestamp")

    @property
    def teamAddress(self) -> str:
        """team address

        Returns:
            str: address
        """
        return self.call_function_autoRpc("teamAddress")

    @property
    def teamPercent(self) -> int:
        """team percent

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("teamPercent")

    @property
    def totalAllocPoint(self) -> int:
        """total alloc point

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("totalAllocPoint")

    @property
    def totalLockedUpRewards(self) -> int:
        """total locked up rewards

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("totalLockedUpRewards")

    @property
    def totalZyberInPools(self) -> int:
        """total zyber in pools

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("totalZyberInPools")

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
        return self.call_function_autoRpc("userInfo", None, pid, user)

    @property
    def zyber(self) -> str:
        """zyber

        Returns:
            str: address
        """
        return self.call_function_autoRpc("zyber")

    @property
    def zyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        return self.call_function_autoRpc("zyberPerSec")

    # get all rewards
    def get_rewards(
        self,
        hypervisor_addresses: list[str] | None = None,
        pids: list[int] | None = None,
        convert_bint: bool = False,
    ) -> list[dict]:
        """Search for rewards data

        Args:
            hypervisor_addresses (list[str] | None, optional): list of lower case hypervisor addresses. When defaults to None, all rewarded hypes ( gamma or non gamma) will be returned.
            pids (list[int] | None, optional): pool ids linked to hypervisor. When defaults to None, all pools will be returned.
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
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
            if pinfo := self.poolInfo(pid):
                if not hypervisor_addresses or pinfo[0].lower() in hypervisor_addresses:
                    # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
                    poolRewardsPerSec = self.poolRewardsPerSec(pid)

                    poolRewarders = self.poolRewarders(pid)

                    # get rewards data
                    first_time = True
                    for address, symbol, decimals, rewardsPerSec in zip(
                        poolRewardsPerSec[0],
                        poolRewardsPerSec[1],
                        poolRewardsPerSec[2],
                        poolRewardsPerSec[3],
                    ):
                        rewarder_type = rewarderType.ZYBERSWAP_masterchef_v1_rewarder
                        rewarder_address = self.address.lower()
                        if first_time:
                            # first item is always ZYB ( without pool rewarder bc it is directly rewarded by the masterchef)
                            # subsequent items have pool rewarder
                            rewarder_address = self.address.lower()
                            first_time = False
                            rewarder_type = rewarderType.ZYBERSWAP_masterchef_v1
                        else:
                            rewarder_address = poolRewarders.pop(0).lower()

                        # if rewardsPerSec: # do not uncomment bc it leads to unknown result ( error or no result)
                        result.append(
                            {
                                # "network": self._network,
                                "block": self.block,
                                "timestamp": self._timestamp,
                                "hypervisor_address": pinfo[0].lower(),
                                "rewarder_address": rewarder_address,
                                "rewarder_type": rewarder_type,
                                "rewarder_refIds": [pid],
                                "rewarder_registry": self.address.lower(),
                                "rewardToken": address.lower(),
                                "rewardToken_symbol": symbol,
                                "rewardToken_decimals": decimals,
                                "rewards_perSecond": str(rewardsPerSec)
                                if convert_bint
                                else rewardsPerSec,
                                "total_hypervisorToken_qtty": str(pinfo[6])
                                if convert_bint
                                else pinfo[6],
                            }
                        )

        return result
