from web3 import Web3
from ....general.enums import Protocol
from .. import uniswap

ABI_FILENAME = "beamswap_pool"  # same as univ3 but without lmPool related functions
ABI_FOLDERNAME = "beamswap"
DEX_NAME = Protocol.BEAMSWAP.database_name


class pool(uniswap.pool.poolv3):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_cached(uniswap.pool.poolv3_cached):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"


class pool_multicall(uniswap.pool.poolv3_multicall):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

    def identify_dex_name(self) -> str:
        return DEX_NAME
