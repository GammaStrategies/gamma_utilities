import logging
from web3 import Web3
from bins.config.hardcodes import SPECIAL_HYPERVISOR_ABIS, SPECIAL_POOL_ABIS

from bins.errors.general import ProcessingError
from ....formulas.position import get_positionKey_ramses
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import gamma
from ..general import erc20, erc20_cached, erc20_multicall

from ..ramses.pool import (
    pool,
    pool_cached,
    pool_multicall,
    ABI_FILENAME as POOL_ABI_FILENAME,
    ABI_FOLDERNAME as POOL_ABI_FOLDERNAME,
)
from ..ramses.rewarder import gauge, multiFeeDistribution

WEEK = 60 * 60 * 24 * 7

ABI_FILENAME = "hypervisor"
ABI_FOLDERNAME = "ramses"
DEX_NAME = Protocol.RAMSES.database_name


# Hype v1.3
class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def _initialize_objects(self):
        super()._initialize_objects()
        self._pool: pool = None
        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # PROPERTIES
    @property
    def DOMAIN_SEPARATOR(self) -> str:
        """EIP-712: Typed structured data hashing and signing"""
        return self.call_function_autoRpc("DOMAIN_SEPARATOR")

    @property
    def PRECISION(self) -> int:
        return self.call_function_autoRpc("PRECISION")

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
            period=period, token_address=reward_token
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
            "current_rewards_per_second": (
                int((amount_base + amount_boost) / seconds_in_period)
                if seconds_in_period
                else 0
            ),
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

    def get_already_claimedRewards(
        self, period: int, reward_token: str, position: str | None = None
    ) -> int:
        """Get the claimed rewards for an specific period

        Args:
            period (int):
            reward_token (str):
            position (str):  "base" | "limit" | None

        Returns:
            int: sum of base and limit positions rewards already collected ( or the specified one )
        """
        _base_position = self.gauge.periodClaimedAmount(
            period=period,
            positionHash=get_positionKey_ramses(
                ownerAddress=Web3.toChecksumAddress(self.address),
                tickLower=self.baseLower,
                tickUpper=self.baseUpper,
                index=0,
            ),
            address=reward_token,
        )
        # return base result if specified
        if position and position.lower() == "base":
            return _base_position

        _limit_position = self.gauge.periodClaimedAmount(
            period=period,
            positionHash=get_positionKey_ramses(
                ownerAddress=Web3.toChecksumAddress(self.address),
                tickLower=self.limitLower,
                tickUpper=self.limitUpper,
                index=0,
            ),
            address=reward_token,
        )
        # return limit result if specified
        if position and position.lower() == "limit":
            return _limit_position

        # return result
        return _base_position + _limit_position

    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> pool:
        return pool(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class gamma_hypervisor_cached(
    gamma.hypervisor.gamma_hypervisor_cached, gamma_hypervisor
):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def _initialize_objects(self):
        super()._initialize_objects()
        self._pool: pool_cached = None
        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def gauge(self) -> gauge:
        if self._gauge is None:
            # check if cached
            prop_name = "gauge"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._gauge = gauge(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._gauge

    @property
    def receiver(self) -> multiFeeDistribution:
        """multiFeeDistribution receiver"""

        if self._multiFeeDistribution is None:
            # check if cached
            prop_name = "receiver"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )

            if result.lower() == "0x8DFF6BbEE7A6E5Fe3413a91dBF305C29e8A0Af5F".lower():
                raise ProcessingError(
                    chain=text_to_chain(self._network),
                    item={
                        "hypervisor_address": self.address,
                        "block": self.block,
                        "object": "hypervisor.receiver",
                    },
                    identity=error_identity.INVALID_MFD,
                    action="remove",
                    message=f"Invalid MFD detected ({result.lower()}) from hypervisor {self.address.lower()} at block {self.block}",
                )
            self._multiFeeDistribution = multiFeeDistribution(
                address=result,
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

    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> pool_cached:
        return pool_cached(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class gamma_hypervisor_multicall(
    gamma.hypervisor.gamma_hypervisor_multicall, gamma_hypervisor
):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def _initialize_abi_pool(self, abi_filename: str = "", abi_path: str = ""):
        self._pool_abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or POOL_ABI_FILENAME
        )
        self._pool_abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{POOL_ABI_FOLDERNAME}"
        )

    def _initialize_objects(self):
        super()._initialize_objects()
        self._pool: pool_multicall = None
        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def gauge(self) -> gauge:
        return self._gauge

    @property
    def receiver(self) -> multiFeeDistribution:
        """multiFeeDistribution receiver"""
        return self._receiver

    @property
    def veRamTokenId(self) -> int:
        return self._veRamTokenId

    @property
    def voter(self) -> str:
        return self._voter

    # builds
    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
        processed_calls: list | None = None,
    ) -> pool_multicall:
        return pool_multicall(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
            processed_calls=processed_calls,
        )

    def _fill_from_processed_calls(self, processed_calls: list):
        _this_object_names = ["hypervisor"]
        for _pCall in processed_calls:
            # filter by address
            if _pCall["address"].lower() == self._address.lower():
                # filter by object type
                if _pCall["object"] in _this_object_names:
                    if _pCall["name"] in [
                        "name",
                        "symbol",
                        "decimals",
                        "totalSupply",
                        "baseLower",
                        "baseUpper",
                        "currentTick",
                        "deposit0Max",
                        "deposit1Max",
                        "directDeposit",
                        "fee",
                        "feeRecipient",
                        "limitLower",
                        "limitUpper",
                        "maxTotalSupply",
                        "owner",
                        "tickSpacing",
                        "whitelistedAddress",
                        "veRamTokenId",
                        "voter",
                        "DOMAIN_SEPARATOR",
                        "PRECISION",
                    ]:
                        # one output only
                        if len(_pCall["outputs"]) != 1:
                            raise ValueError(
                                f"Expected only one output for {_pCall['name']}"
                            )
                        # check if value exists
                        if "value" not in _pCall["outputs"][0]:
                            raise ValueError(
                                f"Expected value in output for {_pCall['name']}"
                            )
                        _object_name = f"_{_pCall['name']}"
                        setattr(self, _object_name, _pCall["outputs"][0]["value"])

                    elif _pCall["name"] in ["getBasePosition", "getLimitPosition"]:
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            {
                                "liquidity": _pCall["outputs"][0]["value"],
                                "amount0": _pCall["outputs"][1]["value"],
                                "amount1": _pCall["outputs"][2]["value"],
                            },
                        )
                    elif _pCall["name"] == "getTotalAmounts":
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            {
                                "total0": _pCall["outputs"][0]["value"],
                                "total1": _pCall["outputs"][1]["value"],
                            },
                        )
                    elif _pCall["name"] == "pool":
                        self._pool = self.build_pool(
                            address=_pCall["outputs"][0]["value"],
                            network=self._network,
                            block=self.block,
                            timestamp=self._timestamp,
                            processed_calls=processed_calls,
                        )
                    elif _pCall["name"] in ["token0", "token1"]:
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            self.build_token(
                                address=_pCall["outputs"][0]["value"],
                                network=self._network,
                                block=self.block,
                                timestamp=self._timestamp,
                                processed_calls=processed_calls,
                            ),
                        )
                    elif _pCall["name"] == "gauge":
                        self._gauge = gauge(
                            address=_pCall["outputs"][0]["value"],
                            network=self._network,
                            block=self.block,
                            timestamp=self._timestamp,
                        )
                    elif _pCall["name"] == "receiver":
                        self._receiver = multiFeeDistribution(
                            address=_pCall["outputs"][0]["value"],
                            network=self._network,
                            block=self.block,
                            timestamp=self._timestamp,
                        )
                    else:
                        logging.getLogger(__name__).debug(
                            f" {_pCall['name']} multicall field not defined to be processed. Ignoring"
                        )
