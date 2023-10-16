import logging
from web3 import Web3

from bins.errors.general import ProcessingError
from ....formulas.position import get_positionKey_ramses
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import gamma
from ..general import erc20, erc20_cached

from .pool import pool, pool_cached


ABI_FILENAME = "fusionx_hypervisor"
ABI_FOLDERNAME = "fusionx"
DEX_NAME = Protocol.FUSIONX.database_name


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
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

        self._pool: pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

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

    @property
    def pool(self) -> pool:
        if self._pool is None:
            self._pool = pool(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_cached(
    gamma_hypervisor, gamma.hypervisor.gamma_hypervisor_cached
):
    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def pool(self) -> pool_cached:
        if self._pool is None:
            # check if cached
            prop_name = "pool"
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
            self._pool = pool_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._pool
