import logging
from web3 import Web3

from bins.errors.general import ProcessingError
from ....general.enums import rewarderType
from ..base_wrapper import web3wrap
from ..general import erc20_cached
from ..gamma.rewarder import gamma_rewarder


# https://lynex.gitbook.io/lynex-docs/security/contracts
class lynex_voter_v5(web3wrap):
    # https://vscode.blockscan.com/linea/0x2a9142ac7d587cad9c0616bdc1d7b39e052a2ff1
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
        self._abi_filename = abi_filename or "voterV5"
        self._abi_path = abi_path or f"{self.abi_root_path}/lynex"

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
    def max_vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("MAX_VOTE_DELAY")

    @property
    def vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("VOTE_DELAY")

    @property
    def _epochTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("_epochTimestamp")

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("_ve")

    @property
    def base(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("base")

    @property
    def bribefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("bribefactory")

    def claimable(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "claimable", None, Web3.toChecksumAddress(address)
        )

    def external_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self.call_function_autoRpc(
            "external_bribes", None, Web3.toChecksumAddress(address)
        )

    @property
    def factories(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: address[]
        """
        return self.call_function_autoRpc("factories")

    @property
    def factoryLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("factoryLength")

    @property
    def gaugeFactories(self) -> list[str]:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            list[str]: list of address
        """
        return self.call_function_autoRpc("gaugeFactories")

    @property
    def gaugeFactoriesLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("gaugeFactoriesLength")

    @property
    def gaugeLogic(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("gaugeLogic")

    def gauges(self, address: str) -> str:
        """_summary_

        Args:
            address (str):

        Returns:
            str: address
        """
        return self.call_function_autoRpc(
            "gauges", None, Web3.toChecksumAddress(address)
        )

    def gaugesDistributionTimestamp(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "gaugesDistributionTimestamp", None, Web3.toChecksumAddress(address)
        )

    def internal_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self.call_function_autoRpc(
            "internal_bribes", None, Web3.toChecksumAddress(address)
        )

    @property
    def isAlive(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc("isAlive")

    def isFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc(
            "isFactory", None, Web3.toChecksumAddress(address)
        )

    def isGauge(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc(
            "isGauge", None, Web3.toChecksumAddress(address)
        )

    def isGaugeDepositor(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc(
            "isGaugeDepositor", None, Web3.toChecksumAddress(address)
        )

    def isGaugeFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc(
            "isGaugeFactory", None, Web3.toChecksumAddress(address)
        )

    def isWhitelisted(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc(
            "isWhitelisted", None, Web3.toChecksumAddress(address)
        )

    def lastVoted(self, address: str) -> int:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "lastVoted", None, Web3.toChecksumAddress(address)
        )

    @property
    def length(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("length")

    @property
    def minter(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("minter")

    @property
    def oLynx(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("oLynx")

    @property
    def permissionRegistry(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("permissionRegistry")

    def poolForGauge(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self.call_function_autoRpc(
            "poolForGauge", None, Web3.toChecksumAddress(address)
        )

    def poolVote(self, address: str, input2: int) -> str:
        """_summary_

        Args:
            input1 (int): uint256
            input2 (int): uint256

        Returns:
            str: address
        """
        return self.call_function_autoRpc(
            "poolVote", None, Web3.toChecksumAddress(address), input2
        )

    def poolVoteLength(self, address: str) -> int:
        """_summary_

        Args:
            tokenId (int): uint256

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "poolVoteLength", None, Web3.toChecksumAddress(address)
        )

    def pools(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self.call_function_autoRpc("pools", None, index)

    @property
    def totalWeight(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("totalWeight")

    def totalWeightAt(self, time: int) -> int:
        """_summary_

        Args:
            time (int): uint256

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("totalWeightAt", None, time)

    def ve(self) -> str:
        """_summary_

        Args:
            index (int)

        Returns:
            str: address
        """
        return self.call_function_autoRpc("ve")

    def votes(self, address1: int, address2: str) -> int:
        """_summary_

        Args:
            index (int): uint256
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "votes",
            None,
            Web3.toChecksumAddress(address1),
            Web3.toChecksumAddress(address2),
        )

    def weights(self, pool_address: str) -> int:
        """_summary_

        Args:
            pool_address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "weights", None, Web3.toChecksumAddress(pool_address)
        )

    def weightsAt(self, pool_address: str, time: int) -> int:
        """_summary_

        Args:
            pool_address (str): address
            time (int): uint256

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "weightsAt", None, Web3.toChecksumAddress(pool_address), time
        )

    # custom functions
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

        if hypervisor_addresses:
            for hypervisor_address in hypervisor_addresses:
                # get managing gauge from hype address
                gauge_address = self.gauges(address=hypervisor_address)
                if gauge_address != "0x0000000000000000000000000000000000000000":
                    # build a gauge
                    _gauge = lynex_gauge_v2(
                        address=gauge_address,
                        network=self._network,
                        block=self.block,
                        timestamp=self._timestamp,
                    )
                    # add "rewarder_registry" to gauge result
                    if gauge_result := _gauge.get_rewards(convert_bint=convert_bint):
                        for gauge in gauge_result:
                            gauge["rewarder_registry"] = self.address.lower()
                        result += gauge_result
                else:
                    # no rewards for this hype
                    logging.getLogger(__name__).debug(
                        f" {self._network} lynex {hypervisor_address} has no gauge address set."
                    )

        else:
            # TODO: get all hypervisors data ... by pid
            raise NotImplementedError("Not implemented yet")

        return result


class lynex_gauge_v2(gamma_rewarder):
    # https://vscode.blockscan.com/linea/0x88f8B3679846A0c8E8E828b6950C6364B737daF9
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
        self._abi_filename = abi_filename or "gaugeV2_CL"
        self._abi_path = abi_path or f"{self.abi_root_path}/lynex"

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
    def distribution(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("DISTRIBUTION")

    @property
    def duration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("DURATION")

    @property
    def ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("VE")

    def availableBalance(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "availableBalance", None, Web3.toChecksumAddress(address)
        )

    def balanceOf(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "balanceOf", None, Web3.toChecksumAddress(address)
        )

    def balanceWithLock(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "balanceWithLock", None, Web3.toChecksumAddress(address)
        )

    def earned(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "earned", None, Web3.toChecksumAddress(address)
        )

    def earned_xtended(self, address: str, token_address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "earned",
            None,
            Web3.toChecksumAddress(address),
            Web3.toChecksumAddress(token_address),
        )

    @property
    def emergency(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        return self.call_function_autoRpc("emergency")

    @property
    def external_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("external_bribe")

    @property
    def feeVault(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("feeVault")

    @property
    def gaugeRewarder(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("gaugeRewarder")

    @property
    def internal_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("internal_bribe")

    @property
    def isForPair(self) -> bool:
        """_summary_

        Returns:
            bool:
        """
        return self.call_function_autoRpc("isForPair")

    def isReward(self, address: str) -> bool:
        """_summary_

        Returns:
            bool:
        """
        return self.call_function_autoRpc(
            "isReward", None, Web3.toChecksumAddress(address)
        )

    @property
    def lastEarn(self, address1: str, address2: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "lastEarn",
            None,
            Web3.toChecksumAddress(address1),
            Web3.toChecksumAddress(address2),
        )

    def lastTimeRewardApplicable(self, token_address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "lastTimeRewardApplicable", None, Web3.toChecksumAddress(token_address)
        )

    def lastUpdateTime(self, address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "lastUpdateTime", None, Web3.toChecksumAddress(address)
        )

    def left(self, token_address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "left", None, Web3.toChecksumAddress(token_address)
        )

    def lockEnd(self, address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "lockEnd", None, Web3.toChecksumAddress(address)
        )

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("owner")

    @property
    def pendingOwner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("pendingOwner")

    def periodFinish(self, token_address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "periodFinish", None, Web3.toChecksumAddress(token_address)
        )

    def periodFinishToken(self, address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "periodFinishToken", None, Web3.toChecksumAddress(address)
        )

    def rewardPerDuration(self, token_address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "rewardPerDuration", None, Web3.toChecksumAddress(token_address)
        )

    def rewardPerToken(self, token_address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "rewardPerToken", None, Web3.toChecksumAddress(token_address)
        )

    def rewardPerTokenStored(self, address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "rewardPerTokenStored", None, Web3.toChecksumAddress(address)
        )

    def rewardRate(self, token_address: str) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "rewardRate", None, Web3.toChecksumAddress(token_address)
        )

    @property
    def rewardToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("rewardToken")

    def rewards(self, idx: int) -> int:
        """_summary_

        Args:
            idx (int): index

        Returns:
            str: address
        """
        return self.call_function_autoRpc("rewards", None, idx)

    @property
    def stakeToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("stakeToken")

    @property
    def totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("totalSupply")

    def userRewardPerTokenPaid(self, address1: str, address2: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "userRewardPerTokenPaid",
            None,
            Web3.toChecksumAddress(address1),
            Web3.toChecksumAddress(address2),
        )

    def userRewardPerTokenStored(self, address1: str, address2: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "userRewardPerTokenStored",
            None,
            Web3.toChecksumAddress(address1),
            Web3.toChecksumAddress(address2),
        )

    # get all rewards
    def get_rewards(
        self,
        convert_bint: bool = False,
    ) -> list[dict]:
        """Get all rewards data


        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
        Returns:
            list[dict]:
        """

        # rewards(idx) has all token addresses rewarded to this hype. The rewardToken field returns the main one ( oLynx i guess).
        # no 'rewards' lenght found... brute loop till err
        # stakeToken is the hypervisor address ( must be, at least)

        rewardTokens_list = []
        max_bruteforce_reward_tokens = 10
        try:
            for i in range(max_bruteforce_reward_tokens):
                if _token_address := self.rewards(i):
                    rewardTokens_list.append(_token_address)
                else:
                    # exit bruteforce loop
                    break
        except ProcessingError as e:
            # this may fire when all RPC return an 'execution reverted' error. No problm.
            pass
        except Exception as e:
            # this may fire when all RPC return an 'execution reverted' error. No problm.
            # TODO: identify execution reverted vs other errors
            pass

        if not rewardTokens_list:
            logging.getLogger(__name__).debug(
                f" No reward token addresses found at {self._network}  lynex gauge {self.address}"
            )

        # Build result for each reward token
        result = []
        totalSupply = self.totalSupply
        for rewardToken in rewardTokens_list:

            # get reward rate for the token
            rewardRate = self.rewardRate(token_address=rewardToken)

            # build reward token instance
            reward_token_instance = erc20_cached(
                address=rewardToken,
                network=self._network,
                block=self.block,
                timestamp=self._timestamp,
            )
            # get reward token data
            rewardToken_symbol = reward_token_instance.symbol
            rewardToken_decimals = reward_token_instance.decimals

            result.append(
                {
                    "block": self.block,
                    "timestamp": self._timestamp,
                    "hypervisor_address": self.stakeToken.lower(),
                    "rewarder_address": self.address.lower(),
                    "rewarder_type": rewarderType.LYNEX_gauge_v2,
                    "rewarder_refIds": [],
                    "rewardToken": rewardToken.lower(),
                    "rewardToken_symbol": rewardToken_symbol,
                    "rewardToken_decimals": rewardToken_decimals,
                    "rewards_perSecond": (
                        str(rewardRate) if convert_bint else rewardRate
                    ),
                    "total_hypervisorToken_qtty": (
                        str(totalSupply) if convert_bint else totalSupply
                    ),
                }
            )

        # return build result
        return result
