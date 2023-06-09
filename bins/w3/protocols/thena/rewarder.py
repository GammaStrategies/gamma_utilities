from web3 import Web3
from bins.general.enums import rewarderType
from bins.w3.protocols.general import erc20_cached, web3wrap
from bins.w3.protocols.gamma.rewarder import gamma_rewarder


class thena_voter_v3(web3wrap):
    # https://bscscan.com/address/0x374cc2276b842fecd65af36d7c60a5b78373ede1#readContract
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
        self._abi_filename = abi_filename or "voterV3"
        self._abi_path = abi_path or "data/abi/thena/binance"

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
    def _factories(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: address[]
        """
        return self.call_function_autoRpc("_factories")

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("_ve")

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

    def factories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self.call_function_autoRpc("factories", None, index)

    @property
    def factory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("factory")

    @property
    def factoryLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("factoryLength")

    def gaugeFactories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self.call_function_autoRpc("gaugeFactories", None, index)

    @property
    def gaugeFactoriesLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("gaugeFactoriesLength")

    @property
    def gaugefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("gaugefactory")

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

    def lastVoted(self, index: int) -> int:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("lastVoted", None, index)

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
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("owner")

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

    def poolVote(self, input1: int, input2: int) -> str:
        """_summary_

        Args:
            input1 (int): uint256
            input2 (int): uint256

        Returns:
            str: address
        """
        return self.call_function_autoRpc("poolVote", None, input1, input2)

    def poolVoteLength(self, tokenId: int) -> int:
        """_summary_

        Args:
            tokenId (int): uint256

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("poolVoteLength", None, tokenId)

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

    def usedWeights(self, index: int) -> int:
        """_summary_

        Args:
            index (int)

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("usedWeights", None, index)

    def votes(self, index: int, address: str) -> int:
        """_summary_

        Args:
            index (int): uint256
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "votes", None, index, Web3.toChecksumAddress(address)
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
                    thena_gauge = thena_gauge_v2(
                        address=gauge_address,
                        network=self._network,
                        block=self.block,
                        timestamp=self._timestamp,
                    )
                    # add "rewarder_registry" to gauge result
                    if gauge_result := thena_gauge.get_rewards(
                        convert_bint=convert_bint
                    ):
                        for gauge in gauge_result:
                            gauge["rewarder_registry"] = self.address.lower()
                        result += gauge_result

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
        timestamp: int = 0,
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
    def token(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("TOKEN")

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("_VE")

    def _balances(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "_balances", None, Web3.toChecksumAddress(address)
        )

    @property
    def _periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("_periodFinish")

    @property
    def _totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("_totalSupply")

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
    def fees0(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("fees0")

    @property
    def fees1(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("fees1")

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
    def lastTimeRewardApplicable(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("lastTimeRewardApplicable")

    @property
    def lastUpdateTime(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("lastUpdateTime")

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("owner")

    @property
    def periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("periodFinish")

    @property
    def rewardPerDuration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("rewardPerDuration")

    @property
    def rewardPerToken(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("rewardPerToken")

    @property
    def rewardPerTokenStored(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("rewardPerTokenStored")

    @property
    def rewardRate(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("rewardRate")

    @property
    def rewardToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self.call_function_autoRpc("rewardToken")

    @property
    def rewardPid(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("rewardPid")

    def rewards(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "rewards", None, Web3.toChecksumAddress(address)
        )

    @property
    def totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc("totalSupply")

    def userRewardPerTokenPaid(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self.call_function_autoRpc(
            "userRewardPerTokenPaid", None, Web3.toChecksumAddress(address)
        )

    # get all rewards
    def get_rewards(
        self,
        convert_bint: bool = False,
    ) -> list[dict]:
        """Search for rewards data


        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
        Returns:
            list[dict]:
        """

        rewardRate = self.rewardRate
        rewardToken = self.rewardToken
        totalSupply = self.totalSupply

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

        return [
            {
                "network": self._network,
                "block": self.block,
                "timestamp": self._timestamp,
                "hypervisor_address": self.token.lower(),
                "rewarder_address": self.address.lower(),
                "rewarder_type": rewarderType.THENA_gauge_v2,
                "rewarder_refIds": [],
                "rewardToken": rewardToken.lower(),
                "rewardToken_symbol": rewardToken_symbol,
                "rewardToken_decimals": rewardToken_decimals,
                "rewards_perSecond": str(rewardRate) if convert_bint else rewardRate,
                "total_hypervisorToken_qtty": str(totalSupply)
                if convert_bint
                else totalSupply,
            }
        ]
