from web3 import Web3
from bins.w3.protocols.general import web3wrap


# gauge
class gauge(web3wrap):
    # https://arbiscan.io/address/0x7cb7ce3ba39f6f02e982b512df9962112ed1bf20#code
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
        self._abi_filename = abi_filename or "RamsesGaugeV2"
        self._abi_path = abi_path or "data/abi/ramses"

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

    def earned(self, token_address: str, token_id: int) -> int:
        """ """
        return self.call_function_autoRpc("earned", None, token_address, token_id)

    @property
    def feeCollector(self) -> str:
        """ """
        return self.call_function_autoRpc("feeCollector")

    @property
    def firstPerdiod(self) -> int:
        """ """
        return self.call_function_autoRpc("firstPerdiod")

    @property
    def gaugeFactory(self) -> str:
        """ """
        return self.call_function_autoRpc("gaugeFactory")

    @property
    def getRewardTokens(self) -> list[str]:
        """ """
        return self.call_function_autoRpc("getRewardTokens")

    def isReward(self, address: str) -> bool:
        """ """
        return self.call_function_autoRpc("isReward", None, address)

    def lastClaimByToken(self, address: str, var: bytes) -> int:
        """ """
        return self.call_function_autoRpc("lastClaimByToken", None, address, var)

    def left(self, token_address: str) -> int:
        """ """
        return self.call_function_autoRpc("left", None, token_address)

    @property
    def nfpManager(self) -> str:
        """ """
        return self.call_function_autoRpc("nfpManager")

    def periodClaimedAmount(self, var: int, data: bytes, address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "periodClaimedAmount", None, var, data, address
        )

    def periodEarned(self, period: int, token_address: str, token_id: int) -> int:
        """ """
        return self.call_function_autoRpc(
            "periodEarned", None, period, token_address, token_id
        )

    def periodEarned2(
        self,
        period: int,
        token_address: str,
        owner: str,
        index: int,
        tickLower: int,
        tickUpper: int,
    ) -> int:
        """ """
        return self.call_function_autoRpc(
            "periodEarned",
            None,
            period,
            token_address,
            owner,
            index,
            tickLower,
            tickUpper,
        )

    def periodTotalBoostedSeconds(self, var: int) -> int:
        """ """
        return self.call_function_autoRpc("periodTotalBoostedSeconds", None, var)

    @property
    def pool(self) -> str:
        """ """
        return self.call_function_autoRpc("pool")

    def positionHash(
        self, owner: str, index: int, tickUpper: int, tickLower: int
    ) -> int:
        """ """
        return self.call_function_autoRpc(
            "positionHash", None, owner, index, tickUpper, tickLower
        )

    def positionInfo(self, token_id: int):
        """
        Return:
            liquidity uint128, boostedLiquidity uint128, veRamTokenId uint256
        """
        return self.call_function_autoRpc("positionInfo", None, token_id)

    def rewardRate(self, token_address: str) -> int:
        """ """
        return self.call_function_autoRpc("rewardRate", None, token_address)

    def rewards(self, var: int) -> str:
        """ """
        return self.call_function_autoRpc("rewards", None, var)

    def tokenTotalSupplyByPeriod(self, var: int, address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "tokenTotalSupplyByPeriod", None, var, address
        )

    def veRamInfo(self, ve_ram_token_id: int):
        """
        Return:
            timesAttached uint128, veRamBoostUsedRatio uint128
        """
        return self.call_function_autoRpc("veRamInfo", None, ve_ram_token_id)

    @property
    def voter(self) -> str:
        """ """
        return self.call_function_autoRpc("voter")
