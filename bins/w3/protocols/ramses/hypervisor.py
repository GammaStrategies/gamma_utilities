import logging
from web3 import Web3

from bins.errors.general import ProcessingError
from bins.formulas import dex_formulas
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import gamma
from ..general import erc20

from ..ramses.pool import pool, pool_cached
from ..ramses.rewarder import gauge, multiFeeDistribution

WEEK = 60 * 60 * 24 * 7


# Hype v1.3
class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
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
        self._abi_filename = abi_filename or "hypervisor"
        self._abi_path = abi_path or f"{self.abi_root_path}/ramses"

        self._pool: pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

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

    def identify_dex_name(self) -> str:
        return Protocol.RAMSES.database_name

    # PROPERTIES
    @property
    def DOMAIN_SEPARATOR(self) -> str:
        """EIP-712: Typed structured data hashing and signing"""
        return self.call_function_autoRpc("DOMAIN_SEPARATOR")

    @property
    def PRECISION(self) -> int:
        return self.call_function_autoRpc("PRECISION")

    @property
    def pool(self) -> pool:
        if self._pool is None:
            self._pool = pool(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def gauge(self) -> gauge:
        if self._gauge is None:
            self._gauge = gauge(
                address=self.call_function_autoRpc("gauge"),
                network=self._network,
                block=self.block,
            )
        return self._gauge

    @property
    def receiver(self) -> multiFeeDistribution:
        """multiFeeDistribution receiver"""

        if self._multiFeeDistribution is None:
            tmp_address = self.call_function_autoRpc("receiver")
            if (
                tmp_address.lower()
                == "0x8DFF6BbEE7A6E5Fe3413a91dBF305C29e8A0Af5F".lower()
            ):
                raise ProcessingError(
                    chain=text_to_chain(self._network),
                    item={
                        "hypervisor_address": self.address,
                        "block": self.block,
                        "object": "hypervisor.receiver",
                    },
                    identity=error_identity.INVALID_MFD,
                    action="remove",
                    message=f"Invalid MFD detected ({tmp_address.lower()}) from hypervisor {self.address.lower()} at block {self.block}",
                )

            self._multiFeeDistribution = multiFeeDistribution(
                address=tmp_address,
                network=self._network,
                block=self.block,
            )
        return self._multiFeeDistribution

    @property
    def veRamTokenId(self) -> int:
        """The veRam Token Id"""
        return self.call_function_autoRpc("veRamTokenId")

    @property
    def voter(self) -> str:
        """voter address"""
        return self.call_function_autoRpc("voter")

    @property
    def whitelistedAddress(self) -> str:
        return self.call_function_autoRpc("whitelistedAddress")

    # CUSTOM FUNCTIONS

    @property
    def current_period(self) -> int:
        """Get the current period

        Returns:
            int: current period
        """
        return self._timestamp // WEEK

    @property
    def current_period_remaining_seconds(self) -> int:
        """Get the current period remaining seconds

        Returns:
            int: current period remaining seconds
        """
        return ((self.current_period + 1) * WEEK) - self._timestamp

    def get_maximum_rewards(self, period: int, reward_token: str) -> tuple[int, int]:
        """Calculate the maximum base and boosted reward rate

        Args:
            period (int): period to calculate the reward rate for
            reward_token (str): address of the reward token

        Returns:
            dict: {
                    "baseRewards": ,
                    "boostedRewards": ,
                }
        """
        allRewards = self.gauge.tokenTotalSupplyByPeriod(
            var=period, address=reward_token
        )
        boostedRewards = (allRewards * 6) // 10
        baseRewards = allRewards - boostedRewards

        return baseRewards, boostedRewards

    def calculate_rewards(
        self, period: int, reward_token: str, convert_bint: bool = False
    ) -> dict:
        """get rewards data for a given period and token address

        Args:
            period (int):
            reward_token (str): reward token address
            convert_bint (bool): Convert integers to string?

        Returns:
            dict: {
                    "max_baseRewards": (int)
                    "max_boostedRewards": (int)
                    "max_period_seconds": (int)
                    "max_rewards_per_second": (int)
                    "current_baseRewards": (int)
                    "current_boostedRewards": (int)
                    "current_period_seconds": (int)
                    "current_rewards_per_second": (int)
                }
        """
        # get max rewards
        baseRewards, boostedRewards = self.get_maximum_rewards(
            period=period, reward_token=reward_token
        )

        (
            periodSecondsInsideX96_base,
            periodBoostedSecondsInsideX96_base,
        ) = self.pool.positionPeriodSecondsInRange(
            period=period,
            owner=self.address,
            index=0,
            tickLower=self.baseLower,
            tickUpper=self.baseUpper,
        )
        (
            periodSecondsInsideX96_limit,
            periodBoostedSecondsInsideX96_limit,
        ) = self.pool.positionPeriodSecondsInRange(
            period=period,
            owner=self.address,
            index=0,
            tickLower=self.limitLower,
            tickUpper=self.limitUpper,
        )

        # rewards are base rewards plus boosted rewards

        amount_base = 0
        amount_boost = 0
        if periodSecondsInsideX96_base > 0:
            amount_base += (baseRewards * periodSecondsInsideX96_base) / (WEEK << 96)

        elif periodSecondsInsideX96_base < 0:
            logging.getLogger(__name__).warning(
                f"  hype: {self.address} periodSecondsInsideX96_base < 0: {periodSecondsInsideX96_base}"
            )

        if periodBoostedSecondsInsideX96_base > 0:
            amount_boost += (boostedRewards * periodBoostedSecondsInsideX96_base) / (
                WEEK << 96
            )

        elif periodBoostedSecondsInsideX96_base < 0:
            logging.getLogger(__name__).warning(
                f"  hype: {self.address} periodBoostedSecondsInsideX96_base < 0: {periodBoostedSecondsInsideX96_base}"
            )

        if periodSecondsInsideX96_limit > 0:
            amount_base += (baseRewards * periodSecondsInsideX96_limit) / (WEEK << 96)

        elif periodSecondsInsideX96_limit < 0:
            logging.getLogger(__name__).warning(
                f"  hype: {self.address} periodSecondsInsideX96_limit < 0: {periodSecondsInsideX96_limit}"
            )

        if periodBoostedSecondsInsideX96_limit > 0:
            amount_boost += (boostedRewards * periodBoostedSecondsInsideX96_limit) / (
                WEEK << 96
            )

        elif periodBoostedSecondsInsideX96_limit < 0:
            logging.getLogger(__name__).warning(
                f"  hype: {self.address} periodBoostedSecondsInsideX96_limit < 0: {periodBoostedSecondsInsideX96_limit}"
            )

        # convert to integer, if it is
        if amount_boost and amount_boost.is_integer():
            amount_boost = int(amount_boost)
        if amount_base and amount_base.is_integer():
            amount_base = int(amount_base)

        # get rewards per second
        seconds_in_period = WEEK - self.current_period_remaining_seconds

        inside_seconds_base: float = (
            periodSecondsInsideX96_base + periodSecondsInsideX96_limit
        ) >> 96  # / (2**96)
        inside_seconds_boost: float = (
            periodBoostedSecondsInsideX96_base + periodBoostedSecondsInsideX96_limit
        ) >> 96  # / (2**96)

        inside_base_rewards_per_second: float = (
            amount_base / inside_seconds_base if inside_seconds_base else 0
        )
        inside_boosted_rewards_per_second: float = (
            amount_boost / inside_seconds_boost if inside_seconds_boost else 0
        )
        inside_rewards_per_second = (
            inside_base_rewards_per_second + inside_boosted_rewards_per_second
        )

        # set result
        data_result = {
            "max_baseRewards": baseRewards,
            "max_boostedRewards": boostedRewards,
            "max_period_seconds": WEEK,
            "max_rewards_per_second": int((baseRewards + boostedRewards) / WEEK),
            "current_baseRewards": amount_base,
            "current_boostedRewards": amount_boost,
            "current_period_seconds": seconds_in_period,
            "current_rewards_per_second": int(
                (amount_base + amount_boost) / seconds_in_period
            )
            if seconds_in_period
            else 0,
            "inside_baseRewards_per_second": inside_base_rewards_per_second,
            "inside_boostedRewards_per_second": inside_boosted_rewards_per_second,
            "inside_seconds_base": inside_seconds_base,
            "inside_seconds_boost": inside_seconds_boost,
            "inside_rewards_per_second": inside_rewards_per_second,
        }

        # convert to string when specified
        if convert_bint:
            for k in [
                "max_baseRewards",
                "max_boostedRewards",
                "max_rewards_per_second",
                "current_baseRewards",
                "current_boostedRewards",
                "current_rewards_per_second",
                "inside_baseRewards_per_second",
                "inside_boostedRewards_per_second",
                "inside_rewards_per_second",
            ]:
                data_result[k] = str(data_result[k])

        # return result
        return data_result

    def get_already_claimedRewards(self, period: int, reward_token: str) -> int:
        """Get the claimed rewards for an specific period

        Args:
            period (int):
            reward_token (str):

        Returns:
            int: sum of base and limit positions rewards already collected
        """
        _base_position = self.gauge.periodClaimedAmount(
            period=period,
            positionHash=dex_formulas.get_positionKey_ramses(
                ownerAddress=Web3.toChecksumAddress(self.address),
                tickLower=self.baseLower,
                tickUpper=self.baseUpper,
                index=0,
            ),
            address=reward_token,
        )
        _limit_position = self.gauge.periodClaimedAmount(
            period=period,
            positionHash=dex_formulas.get_positionKey_ramses(
                ownerAddress=Web3.toChecksumAddress(self.address),
                tickLower=self.limitLower,
                tickUpper=self.limitUpper,
                index=0,
            ),
            address=reward_token,
        )

        # return result
        return _base_position + _limit_position


class gamma_hypervisor_cached(gamma.hypervisor.gamma_hypervisor_cached):
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
        self._abi_filename = abi_filename or "hypervisor"
        self._abi_path = abi_path or f"{self.abi_root_path}/ramses"

        self._pool: pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

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

    def identify_dex_name(self) -> str:
        return Protocol.RAMSES.database_name

    @property
    def pool(self) -> pool_cached:
        if self._pool is None:
            self._pool = pool_cached(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def gauge(self) -> gauge:
        if self._gauge is None:
            self._gauge = gauge(
                address=self.call_function_autoRpc("gauge"),
                network=self._network,
                block=self.block,
            )
        return self._gauge

    @property
    def receiver(self) -> multiFeeDistribution:
        """multiFeeDistribution receiver"""

        if self._multiFeeDistribution is None:
            tmp_address = self.call_function_autoRpc("receiver")
            if (
                tmp_address.lower()
                == "0x8DFF6BbEE7A6E5Fe3413a91dBF305C29e8A0Af5F".lower()
            ):
                raise ProcessingError(
                    chain=text_to_chain(self._network),
                    item={
                        "hypervisor_address": self.address,
                        "block": self.block,
                        "object": "hypervisor.receiver",
                    },
                    identity=error_identity.INVALID_MFD,
                    action="remove",
                    message=f"Invalid MFD detected ({tmp_address.lower()}) from hypervisor {self.address.lower()} at block {self.block}",
                )

            self._multiFeeDistribution = multiFeeDistribution(
                address=tmp_address,
                network=self._network,
                block=self.block,
            )
        return self._multiFeeDistribution

    @property
    def veRamTokenId(self) -> int:
        prop_name = "veRamTokenId"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def voter(self) -> str:
        prop_name = "voter"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def whitelistedAddress(self) -> str:
        prop_name = "whitelistedAddress"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result
