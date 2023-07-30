import logging
from web3 import Web3
from ....general.enums import Protocol
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
                raise ValueError(
                    f"Invalid MFD detected ({tmp_address.lower()}) from hypervisor {self.address.lower()} at block {self.block}"
                )
                # is not a valid gamma MFD:
                logging.getLogger(__name__).warning(
                    f"Invalid MFD address detected ({tmp_address.lower()}) at hypervisor {self.address.lower()}, changing it to: "
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

    def calculate_rewards(self, period: int, reward_token: str) -> dict:
        """get rewards data for a given period and token address

        Args:
            period (int):
            reward_token (str): reward token address

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
        if periodSecondsInsideX96_base:
            amount_base += (baseRewards * periodSecondsInsideX96_base) / (WEEK << 96)
        if periodBoostedSecondsInsideX96_base:
            amount_boost += (boostedRewards * periodBoostedSecondsInsideX96_base) / (
                WEEK << 96
            )
        if periodSecondsInsideX96_limit:
            amount_base += (baseRewards * periodSecondsInsideX96_limit) / (WEEK << 96)
        if periodBoostedSecondsInsideX96_limit:
            amount_boost += (boostedRewards * periodBoostedSecondsInsideX96_limit) / (
                WEEK << 96
            )

        # get rewards per second
        seconds_in_period = WEEK - self.current_period_remaining_seconds

        return {
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
        }


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
                raise ValueError(
                    f"Invalid MFD detected ({tmp_address.lower()}) from hypervisor {self.address.lower()} at block {self.block}"
                )
                # is not a valid gamma MFD chainge it?:
                logging.getLogger(__name__).warning(
                    f"Invalid MFD address detected ({tmp_address.lower()}) at hypervisor {self.address.lower()}, changing it to: "
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
