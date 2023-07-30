from web3 import Web3
from ....general.enums import rewarderType
from ..general import web3wrap
from .pool import pool

# [position_token0_amount, position_token1_amount] = token_amounts_from_current_price(pool['sqrtPrice'], range_delta, pool['liquidity'])

#  position_usd = (position_token0_amount * token0['price'] / 10**token0['decimals']) + (position_token1_amount * token1['price'] / 10**token1['decimals'])

#  pool['lpApr'] = (totalUSD * 36500 / (position_usd if position_usd > 0 else 1)) + (pool['feeApr'] if pool['feeApr'] < 300 else 0)

# current pool token amounts arround current price ( +-% deviation)

# week = 7 * 24 * 60 * 60
# now = datetime.datetime.now().timestamp()
# current_period = int(now // week * week + week)

# Ramses fee_distribution: --  https://github.com/RamsesExchange/Ramses-API/blob/master/cl/constants/feeDistribution.json
#   20% fees to LPs
#   80% fees to veRAM and treasury
# The current ratios upon newly made pools are:
# - 20% LPers
# - 5% Ecosystem Incentives fund.
# - 75% veRAM

# Competitive Rewarding Logic
# The CL Gauges determine rewards based on several factors:
# Tick Delta (Δ) [Upper - Lower] of the user's position
# Position size
# Position Utilization: In Range? [True or False]

# get gamma range lowtick uppertick and find out how many liquidity exist on that range ( token0 , token1) and then in usd
#
# gauge rewardRate per rewardToken ( reward rate reported by gauge contracts are already normalized to total unboosted liquidity)
#  rewardRate_decimal =  rewardRate * 60 * 60 * 24 / 10**token decimals
#  rewardsRate usd = rewardRate_decimals * token price
#
#  rewardsRate_usd a year / total liquidity in gamma range usd = APY
#
#


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

        self._pool: pool | None = None

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
        return self.call_function_autoRpc(
            "earned", None, Web3.toChecksumAddress(token_address), token_id
        )

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
        return self.call_function_autoRpc(
            "isReward", None, Web3.toChecksumAddress(address)
        )

    def lastClaimByToken(self, address: str, var: bytes) -> int:
        """ """
        return self.call_function_autoRpc(
            "lastClaimByToken", None, Web3.toChecksumAddress(address), var
        )

    def left(self, token_address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "left", None, Web3.toChecksumAddress(token_address)
        )

    @property
    def nfpManager(self) -> str:
        """ """
        return self.call_function_autoRpc("nfpManager")

    def periodClaimedAmount(self, period: int, data: bytes, address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "periodClaimedAmount", None, period, data, Web3.toChecksumAddress(address)
        )

    def periodEarned(self, period: int, token_address: str, token_id: int) -> int:
        """ """
        return self.call_function_autoRpc(
            "periodEarned",
            None,
            period,
            Web3.toChecksumAddress(token_address),
            token_id,
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
            Web3.toChecksumAddress(token_address),
            owner,
            index,
            tickLower,
            tickUpper,
        )

    def periodTotalBoostedSeconds(self, period: int) -> int:
        """ """
        return self.call_function_autoRpc("periodTotalBoostedSeconds", None, period)

    @property
    def pool(self) -> pool:
        """ """
        if self._pool is None:
            self._pool = pool(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

    def positionHash(
        self, owner: str, index: int, tickUpper: int, tickLower: int
    ) -> int:
        """ """
        return self.call_function_autoRpc(
            "positionHash",
            None,
            Web3.toChecksumAddress(owner),
            index,
            tickUpper,
            tickLower,
        )

    def positionInfo(self, token_id: int):
        """
        Return:
            liquidity uint128, boostedLiquidity uint128, veRamTokenId uint256
        """
        return self.call_function_autoRpc("positionInfo", None, token_id)

    def rewardRate(self, token_address: str) -> int:
        """normalized to total unboosted liquidity ..."""
        return self.call_function_autoRpc(
            "rewardRate", None, Web3.toChecksumAddress(token_address)
        )

    def rewards(self, var: int) -> str:
        """ """
        return self.call_function_autoRpc("rewards", None, var)

    def tokenTotalSupplyByPeriod(self, var: int, address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "tokenTotalSupplyByPeriod", None, var, Web3.toChecksumAddress(address)
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

    # get all rewards
    def get_rewards(
        self,
        convert_bint: bool = False,
    ) -> list[dict]:
        """Get hypervisor rewards data
            Be aware that some fields are to be filled outside this func
        Args:
            hypervisor_address (str): lower case hypervisor address.
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
        Returns:
            list[dict]: network: str
                        block: int
                        timestamp: int
                                hypervisor_address: str = None
                        rewarder_address: str
                        rewarder_type: str
                        rewarder_refIds: list[str]
                        rewardToken: str
                                rewardToken_symbol: str = None
                                rewardToken_decimals: int = None
                        rewards_perSecond: int
                                total_hypervisorToken_qtty: int = None
        """
        result = []
        for reward_token in self.getRewardTokens:
            # get reward rate
            RewardsPerSec = self.rewardRate(reward_token)

            result.append(
                {
                    # "network": self._network,
                    "block": self.block,
                    "timestamp": self._timestamp,
                    "hypervisor_address": None,
                    "rewarder_address": self.address.lower(),
                    "rewarder_type": rewarderType.RAMSES_v2,
                    "rewarder_refIds": [],
                    "rewarder_registry": self.gaugeFactory.lower(),
                    "rewardToken": reward_token.lower(),
                    "rewardToken_symbol": None,
                    "rewardToken_decimals": None,
                    "rewards_perSecond": str(RewardsPerSec)
                    if convert_bint
                    else RewardsPerSec,
                    "total_hypervisorToken_qtty": None,
                }
            )

        return result


# MultiFeeDistribution (hypervisor receiver )
# https://github.com/curvefi/multi-rewards
class multiFeeDistribution(web3wrap):
    # https://arbiscan.io/address/0xdfc86bf44dccc9529319e4fbc9579781c9345e18#readProxyContract
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
        self._abi_filename = abi_filename or "multiFeeDistribution"
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

    # TODO: complete functions

    @property
    def totalStakes(self) -> int:
        """ """
        return self.call_function_autoRpc("totalStakes")


# TODO: gaugeFactory
#   getGauge(pool address)
