from bins.errors.general import ProcessingError
from ....general.enums import error_identity, rewarderType
from ..ramses import rewarder as ramses_rewarder
from .pool import pool


# gauge
class gauge(ramses_rewarder.gauge):
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
                    "rewarder_type": rewarderType.CLEOPATRA,
                    "rewarder_refIds": [],
                    "rewarder_registry": "",  # should be hypervisor receiver address
                    "rewardToken": reward_token.lower(),
                    "rewardToken_symbol": None,
                    "rewardToken_decimals": None,
                    "rewards_perSecond": (
                        str(RewardsPerSec) if convert_bint else RewardsPerSec
                    ),
                    "total_hypervisorToken_qtty": None,
                }
            )

        return result


# MultiFeeDistribution (hypervisor receiver )
class multiFeeDistribution(ramses_rewarder.multiFeeDistribution):
    pass
