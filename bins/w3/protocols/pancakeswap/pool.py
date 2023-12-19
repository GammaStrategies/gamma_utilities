from web3 import Web3

from bins.config.hardcodes import SPECIAL_POOL_ABIS
from ....general.enums import Protocol
from .. import uniswap

ABI_FILENAME = "pancakeswapv3_pool"
ABI_FOLDERNAME = "pancakeswap"
DEX_NAME = Protocol.PANCAKESWAP.database_name


class pool(uniswap.pool.poolv3):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_cached(uniswap.pool.poolv3_cached):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_multicall(uniswap.pool.poolv3_multicall):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_bep20(uniswap.pool.poolv3_bep20):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_bep20_cached(uniswap.pool.poolv3_bep20_cached):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_bep20_multicall(uniswap.pool.poolv3_bep20_multicall):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME
