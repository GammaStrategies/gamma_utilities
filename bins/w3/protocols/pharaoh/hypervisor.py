from bins.config.hardcodes import SPECIAL_HYPERVISOR_ABIS, SPECIAL_POOL_ABIS

from bins.errors.general import ProcessingError
from ....formulas.position import get_positionKey_ramses
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import ramses
from .rewarder import gauge, multiFeeDistribution

from .pool import (
    pool,
    pool_cached,
    pool_multicall,
    ABI_FILENAME as POOL_ABI_FILENAME,
    ABI_FOLDERNAME as POOL_ABI_FOLDERNAME,
)

WEEK = 60 * 60 * 24 * 7

ABI_FILENAME = "hypervisor"
ABI_FOLDERNAME = "ramses"
DEX_NAME = Protocol.PHARAOH.database_name


# Pharaoh -> Ramses
class gamma_hypervisor(ramses.hypervisor.gamma_hypervisor):
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

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # PROPERTIES
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

            self._multiFeeDistribution = multiFeeDistribution(
                address=tmp_address,
                network=self._network,
                block=self.block,
            )
        return self._multiFeeDistribution

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


class gamma_hypervisor_cached(ramses.hypervisor.gamma_hypervisor_cached):
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

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # PROPERTIES
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

            self._multiFeeDistribution = multiFeeDistribution(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._multiFeeDistribution

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


class gamma_hypervisor_multicall(ramses.hypervisor.gamma_hypervisor_multicall):
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

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # PROPERTIES
    @property
    def gauge(self) -> gauge:
        return self._gauge

    @property
    def receiver(self) -> multiFeeDistribution:
        """multiFeeDistribution receiver"""
        return self._receiver

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
