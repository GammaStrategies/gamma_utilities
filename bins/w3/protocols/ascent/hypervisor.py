from web3 import Web3
from ....general.enums import Protocol
from .. import uniswap
from ..general import erc20

from .pool import pool, pool_cached


class gamma_hypervisor(uniswap.hypervisor.gamma_hypervisor):
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
        self._abi_path = abi_path or f"{self.abi_root_path}/gamma"

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
        return Protocol.ASCENT.database_name

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


class gamma_hypervisor_cached(uniswap.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return Protocol.ASCENT.database_name

    @property
    def pool(self) -> pool_cached:
        if self._pool is None:
            self._pool = pool_cached(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool
