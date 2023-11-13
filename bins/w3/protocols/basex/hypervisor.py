from web3 import Web3
from .. import gamma
from bins.general.enums import Protocol
from .pool import (
    pool,
    pool_cached,
    pool_multicall,
    ABI_FILENAME as POOL_ABI_FILENAME,
    ABI_FOLDERNAME as POOL_ABI_FOLDERNAME,
)
from ..general import erc20_multicall

DEX_NAME = Protocol.BASEX.database_name


class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
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


class gamma_hypervisor_multicall(gamma.hypervisor.gamma_hypervisor_multicall):
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
        known_data: dict | None = None,
    ):
        self._abi_filename = abi_filename or gamma.hypervisor.ABI_FILENAME
        self._abi_path = (
            abi_path or f"{self.abi_root_path}/{gamma.hypervisor.ABI_FOLDERNAME}"
        )

        self._pool: pool_multicall | None = None
        self._token0: erc20_multicall | None = None
        self._token1: erc20_multicall | None = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            pool_abi_filename=POOL_ABI_FILENAME,
            pool_abi_path=f"{self.abi_root_path}/{POOL_ABI_FOLDERNAME}",
        )

        if known_data:
            self._fill_from_known_data(known_data=known_data)

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def pool(self) -> pool_multicall:
        return self._pool

    def _fill_from_known_data_objects(self, known_data: dict):
        self._pool = pool_multicall(
            address=known_data["pool"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["pool"],
        )
        self._token0 = erc20_multicall(
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token0"],
        )
        self._token1 = erc20_multicall(
            address=known_data["token1"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )
