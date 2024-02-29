from bins.config.hardcodes import SPECIAL_POOL_ABIS
from ....general.enums import Protocol
from .. import algebra

from .pool import (
    pool,
    pool_cached,
    pool_multicall,
    ABI_FILENAME as POOL_ABI_FILENAME,
    ABI_FOLDERNAME as POOL_ABI_FOLDERNAME,
)

DEX_NAME = Protocol.HERCULES.database_name


class gamma_hypervisor(algebra.hypervisor.gamma_hypervisor):
    def _initialize_objects(self):
        super()._initialize_objects()
        # reset pool to ascent pool
        self._pool: pool = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # builds
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


class gamma_hypervisor_cached(algebra.hypervisor.gamma_hypervisor_cached):
    def _initialize_objects(self):
        super()._initialize_objects()
        # reset pool to ascent pool
        self._pool: pool_cached = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # builds
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


class gamma_hypervisor_multicall(algebra.hypervisor.gamma_hypervisor_multicall):
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
        # reset pool to ascent pool
        self._pool: pool_multicall = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

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
