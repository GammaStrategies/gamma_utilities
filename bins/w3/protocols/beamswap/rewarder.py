import logging
from web3 import Web3
from bins.general.enums import Protocol, rewarderType

from bins.w3.protocols.gamma.rewarder import gamma_rewarder


class beamswap_masterchef_v2_rewarder(gamma_rewarder):
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
                        "network": self._network,
                        "block": self.block,
                        "timestamp": self._timestamp,
                        # "hypervisor_address": pinfo[0].lower(), # there is no hype address in this contract
                        "rewarder_address": self.address.lower(),
                        "rewarder_type": rewarderType.BEAMSWAP_masterchef_v2_rewarder,
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
                    f" Error encountered while constructing beamswap rewards -> {e}"
                )

        return result


class beamswap_masterchef_v2(gamma_rewarder):
    # https://moonscan.io/address/0x9d48141b234bb9528090e915085e0e6af5aad42c#code
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
        self._abi_filename = abi_filename or "BeamChefV2"
        self._abi_path = abi_path or "data/abi/beamswap/masterchef"

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

    # TODO: MAXIMUM_DEPOSIT_FEE_RATE
    #       MAXIMUM_HARVEST_INTERVAL
    #

    @property
    def beam(self) -> str:
        return self.call_function_autoRpc("beam")

    @property
    def beamPerSec(self) -> int:
        return self.call_function_autoRpc("beamPerSec")

    @property
    def beamShareAddress(self) -> str:
        return self.call_function_autoRpc("beamShareAddress")

    @property
    def beamSharePercent(self) -> int:
        return self.call_function_autoRpc("beamSharePercent")

    def canHarvest(self, pid: int, user: str) -> tuple[int, int]:
        return self.call_function_autoRpc("canHarvest", None, pid, user)

    @property
    def feeAddress(self) -> str:
        return self.call_function_autoRpc("feeAddress")

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    def pendingTokens(self, pid: int, user: str) -> tuple[int, int]:
        return self.call_function_autoRpc("pendingTokens", None, pid, user)

    def poolInfo(self, pid: int) -> tuple[str, int, int, int, int, int, int]:
        """

        Args:
            pid (int): pool index

        Returns:
            tuple[str, int, int, int, int, int, int]:
                lpToken   address :  0x99588867e817023162F4d4829995299054a5fC57
                allocPoint   uint256 :  1060
                lastRewardTimestamp   uint256 :  1686252702
                accBeamPerShare   uint256 :  25726015220264
                depositFeeBP   uint16 :  0
                harvestInterval   uint256 :  0
                totalLp   uint256 :  281163683193369998486959
        """
        return self.call_function_autoRpc("poolInfo", None, pid)

    @property
    def poolLength(self) -> int:
        return self.call_function_autoRpc("poolLength")

    def poolRewarders(self, pid: int) -> list[str]:
        return self.call_function_autoRpc("poolRewarders", None, pid)

    def poolRewardsPerSec(
        self, pid: int
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """first item is always GLINT ( without pool rewarder bc it is directly rewarded by the masterchef)
             subsequent items have pool rewarder ( when calling poolRewarders(pid))

        Args:
            pid (int): pool id

        Returns:
            tuple         addresses   address[] : [[0xcd3B51D98478D53F4515A306bE565c6EebeF1D58]]
                            symbols   string[] :  GLINT
                            decimals   uint256[] :  18
                            rewardsPerSec   uint256[] :  225475475475475475
        """
        return self.call_function_autoRpc("poolRewardsPerSec", None, pid)

    def poolTotalLp(self, pid: int) -> int:
        return self.call_function_autoRpc("poolTotalLp", None, pid)

    @property
    def stGlint(self) -> str:
        return self.call_function_autoRpc("stGlint")

    @property
    def stGlintRatio(self) -> int:
        return self.call_function_autoRpc("stGlintRatio")

    @property
    def startTimestamp(self) -> int:
        return self.call_function_autoRpc("startTimestamp")

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self.call_function_autoRpc("totalAllocPoint")

    @property
    def totalBeamInPools(self) -> int:
        return self.call_function_autoRpc("totalBeamInPools")

    @property
    def totalLockedUpRewards(self) -> int:
        return self.call_function_autoRpc("totalLockedUpRewards")

    def userInfo(self, pid: int, user: str) -> tuple[int, int, int, int]:
        """

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256, rewardLockedUp uint256, nextHarvestUntil uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user
                    rewardLockedUp — the amount of SUSHI locked in the MasterChef for the user
                    nextHarvestUntil — when can the user harvest again

        """
        return self.call_function_autoRpc("userInfo", None, pid, user)

    # CUSTOM

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
            # lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accBeamPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
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
                        rewarder_type = rewarderType.BEAMSWAP_masterchef_v2_rewarder
                        rewarder_address = self.address.lower()
                        if first_time:
                            # first item is always GLINT ( without pool rewarder bc it is directly rewarded by the masterchef)
                            # subsequent items have pool rewarder
                            rewarder_address = self.address.lower()
                            rewarder_type = rewarderType.BEAMSWAP_masterchef_v2
                            first_time = False
                        else:
                            rewarder_address = poolRewarders.pop(0).lower()

                        # if rewardsPerSec: # do not uncomment bc it leads to unknown result ( error or no result)
                        result.append(
                            {
                                "network": self._network,
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
