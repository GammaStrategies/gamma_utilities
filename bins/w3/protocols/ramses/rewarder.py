from web3 import Web3

from bins.errors.general import ProcessingError
from ....general.enums import error_identity, rewarderType, text_to_chain
from ..base_wrapper import web3wrap
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
        self._abi_path = abi_path or f"{self.abi_root_path}/ramses"

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
        """Returns the amount of rewards earned for an NFP.
        Args:
            token_address (str): The address of the token for which to retrieve the earned rewards.
            token_id (int): The identifier of the specific NFP for which to retrieve the earned rewards.

        Returns:
            int: The amount of rewards earned for the specified NFP and tokens.
        """
        return self.call_function_autoRpc(
            "earned", None, Web3.toChecksumAddress(token_address), token_id
        )

    @property
    def feeCollector(self) -> str:
        """fee collector address"""
        return self.call_function_autoRpc("feeCollector")

    @property
    def firstPeriod(self) -> int:
        """Retrieves the value of the firstPeriod variable."""
        return self.call_function_autoRpc("firstPeriod")

    @property
    def gaugeFactory(self) -> str:
        """gauge factory address"""
        return self.call_function_autoRpc("gaugeFactory")

    @property
    def getRewardTokens(self) -> list[str]:
        """Returns an array of reward token addresses."""
        return self.call_function_autoRpc("getRewardTokens")

    def isReward(self, address: str) -> bool:
        """Checks if a given address is a valid reward."""
        return self.call_function_autoRpc(
            "isReward", None, Web3.toChecksumAddress(address)
        )

    def lastClaimByToken(self, address: str, positionHash: bytes) -> int:
        """Retrieves the last claimed period for a specific token, token ID combination

        Args:
            address (str): The address of the reward token for which to retrieve the last claimed period.
            positionHash (bytes): The identifier of the NFP for which to retrieve the last claimed period.

        Returns:
            int:  The last claimed period for the specified token and token ID.
        """
        return self.call_function_autoRpc(
            "lastClaimByToken", None, Web3.toChecksumAddress(address), positionHash
        )

    def left(self, token_address: str) -> int:
        """Retrieves the getTokenTotalSupplyByPeriod of the current period.
            included to support voter's left() check during distribute().
        Args:
            token_address (str): The address of the token for which to retrieve the remaining amount.

        Returns:
            int: The amount of tokens left to distribute in this period.
        """
        return self.call_function_autoRpc(
            "left", None, Web3.toChecksumAddress(token_address)
        )

    @property
    def nfpManager(self) -> str:
        """The address of the NFP manager"""
        return self.call_function_autoRpc("nfpManager")

    def periodClaimedAmount(
        self, period: int, positionHash: bytes, address: str
    ) -> int:
        """Retrieves the claimed amount for a specific period, position hash, and user address.
        Args:
            period (int): The period for which to retrieve the claimed amount.
            positionHash (bytes): The identifier of the NFP for which to retrieve the claimed amount.
            address (str):  The address of the token for the claimed amount.

        Returns:
            int: Claimed amount for the specified period, token ID, and user address.
        """
        return self.call_function_autoRpc(
            "periodClaimedAmount",
            None,
            period,
            positionHash,
            Web3.toChecksumAddress(address),
        )

    def periodEarned(self, period: int, token_address: str, token_id: int) -> int:
        """Returns the amount of rewards earned during a period for an NFP.

        Args:
            period (int): The period for which to retrieve the earned rewards.
            token_address (str): The address of the token for which to retrieve the earned rewards.
            token_id (int): The identifier of the specific NFP for which to retrieve the earned rewards.

        Returns:
            int: reward The amount of rewards earned for the specified NFP and tokens.
        """
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
        """Retrieves the earned rewards for a specific period, token, owner, index, tickLower, and tickUpper.

        Args:
            period (int): The period for which to retrieve the earned rewards.
            token_address (str): The address of the token for which to retrieve the earned rewards.
            owner (str): The address of the owner for which to retrieve the earned rewards.
            index (int): The index for which to retrieve the earned rewards.
            tickLower (int): The tick lower bound for which to retrieve the earned rewards.
            tickUpper (int): The tick upper bound for which to retrieve the earned rewards.

        Returns:
            int: The earned rewards for the specified period, token, owner, index, tickLower, and tickUpper.
        """
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
        """Retrieves the total boosted seconds for a specific period.

        Args:
            period (int): The period for which to retrieve the total boosted seconds.

        Returns:
            int: The total boosted seconds for the specified period.
        """
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
        """Retrieves the liquidity and boosted liquidity for a specific NFP.

        Args:
            token_id (int): The identifier of the NFP.

        Returns:
            liquidity The liquidity of the position token.
            boostedLiquidity The boosted liquidity of the position token.
            veRamTokenId The attached veRam token
        """
        return self.call_function_autoRpc("positionInfo", None, token_id)

    def rewardRate(self, token_address: str) -> int:
        """Retrieves the reward rate for a specific reward address.
            this method returns the base rate without boost

        Args:
            token_address (str): The address of the reward for which to retrieve the reward rate.

        Returns:
            int: The unboosted reward rate for the specified reward address.
        """
        return self.call_function_autoRpc(
            "rewardRate", None, Web3.toChecksumAddress(token_address)
        )

    def rewards(self, index: int) -> str:
        """Retrieves the reward address at the specified index in the rewards array.

        Args:
            index (int): The index of the reward address to retrieve.

        Returns:
            str: The reward address at the specified index.
        """
        return self.call_function_autoRpc("rewards", None, index)

    def tokenTotalSupplyByPeriod(self, period: int, token_address: str) -> int:
        """Retrieves the total supply of a specific token for a given period.

        Args:
            period (int): The period for which to retrieve the total supply.
            token_address (str): The address of the token for which to retrieve the total supply.

        Returns:
            int: The total supply of the specified token for the given period.
        """
        return self.call_function_autoRpc(
            "tokenTotalSupplyByPeriod",
            None,
            period,
            Web3.toChecksumAddress(token_address),
        )

    def veRamInfo(self, ve_ram_token_id: int):
        """
        Return:
            timesAttached uint128, veRamBoostUsedRatio uint128
        """
        return self.call_function_autoRpc("veRamInfo", None, ve_ram_token_id)

    @property
    def voter(self) -> str:
        """The contract that manages Ramses votes, which must adhere to the IVoter interface"""
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
                    "rewarder_registry": "",  # should be hypervisor receiver address
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
        self._abi_path = abi_path or f"{self.abi_root_path}/ramses"

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

    def rewardData(self, rewardToken_address: str) -> dict:
        """Amount of reward token saved in contract memory as per its last update time [ stake, unstake, claim and getAllRewards calls]

        Returns { amount uint256, lastTimeUpdated uint256, rewardPerToken uint256}
        """
        if tmp := self.call_function_autoRpc(
            "rewardData", None, Web3.toChecksumAddress(rewardToken_address)
        ):
            return {
                "amount": tmp[0],
                "lastTimeUpdated": tmp[1],
                "rewardPerToken": tmp[2],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "address": self.address,
                    "block": self.block,
                    "object": "multiFeeDistribution",
                },
                identity=error_identity.RETURN_NONE,
                action="none",
                message=f" can't get any result of rewardData({rewardToken_address}) call ",
            )

    def totalBalance(self, user_address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "totalBalance", None, Web3.toChecksumAddress(user_address)
        )

    def userData(self, user_address: str) -> dict:
        if tmp := self.call_function_autoRpc(
            "userData", None, Web3.toChecksumAddress(user_address)
        ):
            return {
                "tokenAmount": tmp[0],
                "lastTimeUpdated": tmp[1],
                "tokenClaimable": tmp[2],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "address": self.address,
                    "block": self.block,
                    "object": "multiFeeDistribution",
                },
                identity=error_identity.RETURN_NONE,
                action="none",
                message=f" can't get any result of userData({user_address}) call ",
            )

    def claimable(self, user_address: str, rewardToken_address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "claimable",
            None,
            Web3.toChecksumAddress(user_address),
            Web3.toChecksumAddress(rewardToken_address),
        )


# TODO: gaugeFactory
#   getGauge(pool address)
