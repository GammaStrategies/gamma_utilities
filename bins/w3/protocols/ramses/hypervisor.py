from web3 import Web3
from bins.general.enums import Protocol
from bins.w3.protocols import uniswap
from bins.w3.protocols.general import erc20

from bins.w3.protocols.ramses.pool import pool, pool_cached
from bins.w3.protocols.ramses.rewarder import gauge, multiFeeDistribution


# Hype v1.3
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
        self._abi_path = abi_path or "data/abi/ramses"

        self._pool: pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

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
        return Protocol.RAMSES.database_name

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
            self._multiFeeDistribution = multiFeeDistribution(
                address=self.call_function_autoRpc("receiver"),
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


class gamma_hypervisor_cached(uniswap.hypervisor.gamma_hypervisor_cached):
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
        self._abi_path = abi_path or "data/abi/ramses"

        self._pool: pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        self._gauge: gauge | None = None
        self._multiFeeDistribution: multiFeeDistribution | None = None

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
        return Protocol.RAMSES.database_name

    @property
    def pool(self) -> pool_cached:
        if self._pool is None:
            self._pool = pool_cached(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

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
            self._multiFeeDistribution = multiFeeDistribution(
                address=self.call_function_autoRpc("receiver"),
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
