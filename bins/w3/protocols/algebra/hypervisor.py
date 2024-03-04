from web3 import Web3
from bins.config.hardcodes import SPECIAL_HYPERVISOR_ABIS, SPECIAL_POOL_ABIS

from bins.general.enums import Protocol
from .pool import (
    poolv3,
    poolv3_bep20,
    poolv3_cached,
    poolv3_bep20_cached,
    poolv3_multicall,
    poolv3_bep20_multicall,
    ABI_FILENAME as POOL_ABI_FILENAME,
    ABI_FOLDERNAME as POOL_ABI_FOLDERNAME,
)
from ..general import (
    bep20,
    bep20_multicall,
    bep20_cached,
    erc20,
    erc20_cached,
    erc20_multicall,
)

from .. import gamma


ABI_FILENAME = "algebra_hypervisor_v2"
ABI_FOLDERNAME = "gamma"
DEX_NAME = Protocol.ALGEBRAv3.database_name


class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        # check if this is a special hypervisor and abi_filename is not set
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
        # reset pool to ascent pool
        self._pool: poolv3 = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # builds
    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> poolv3:
        return poolv3(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class gamma_hypervisor_cached(gamma.hypervisor.gamma_hypervisor_cached):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        # check if this is a special hypervisor and abi_filename is not set
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
        # reset pool to ascent pool
        self._pool: poolv3_cached = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # builds
    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> poolv3_cached:
        return poolv3_cached(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class gamma_hypervisor_multicall(gamma.hypervisor.gamma_hypervisor_multicall):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        # check if this is a special hypervisor and abi_filename is not set
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
        # reset pool to ascent pool
        self._pool: poolv3_multicall = None

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
    ) -> poolv3_multicall:
        return poolv3_multicall(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
            processed_calls=processed_calls,
        )


class gamma_hypervisor_bep20(gamma.hypervisor.gamma_hypervisor_bep20):
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
        # reset pool to ascent pool
        self._pool: poolv3_bep20 = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # builds
    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> poolv3_bep20:
        return poolv3_bep20(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class gamma_hypervisor_bep20_cached(gamma.hypervisor.gamma_hypervisor_bep20_cached):
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
        # reset pool to ascent pool
        self._pool: poolv3_bep20_cached = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    # builds
    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> poolv3_bep20_cached:
        return poolv3_bep20_cached(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class gamma_hypervisor_bep20_multicall(
    gamma.hypervisor.gamma_hypervisor_bep20_multicall
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
        self._pool: poolv3_bep20_multicall = None

    def identify_dex_name(self) -> str:
        return DEX_NAME

    def build_pool(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
        processed_calls: list | None = None,
    ) -> poolv3_bep20_multicall:
        return poolv3_bep20_multicall(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
            processed_calls=processed_calls,
        )
