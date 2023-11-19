from web3 import Web3
from ....general.enums import Protocol
from .. import algebra

ABI_FILENAME = "thena_pool"
ABI_FOLDERNAME = "thena/binance"
DEX_NAME = Protocol.THENA.database_name


class pool(algebra.pool.poolv3_bep20):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_cached(algebra.pool.poolv3_bep20_cached):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_multicall(algebra.pool.poolv3_bep20_multicall):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

    def identify_dex_name(self) -> str:
        return DEX_NAME
