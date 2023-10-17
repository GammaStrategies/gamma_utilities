import logging
from hexbytes import HexBytes
from web3 import Web3

from bins.w3.protocols.general import erc20_cached

from ....cache import cache_utilities
from ....errors.general import ProcessingError

# from ....formulas.position import get_positionKey_ramses
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import uniswap

# ABI_FILENAME = "fusionx_pool"
# ABI_FOLDERNAME = "fusionx"
ABI_FILENAME = "pancakeswapv3_pool"
ABI_FOLDERNAME = "pancakeswap"
DEX_NAME = Protocol.FUSIONX.database_name


class pool(uniswap.pool.poolv3):
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
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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
        return DEX_NAME

    # PROPERTIES
    @property
    def lmPool(self) -> str:
        return self.call_function_autoRpc("lmPool")


class pool_cached(pool, uniswap.pool.poolv3_cached):
    SAVE2FILE = True

    @property
    def lmPool(self) -> str:
        prop_name = "lmPool"
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
