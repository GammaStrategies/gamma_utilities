from web3 import Web3

from bins.general.enums import Protocol
from bins.w3.helpers.multicaller import execute_multicall
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
    erc20_multicall,
)

from .. import gamma


ABI_FILENAME = "algebra_hypervisor_v2"
ABI_FOLDERNAME = "gamma"
DEX_NAME = Protocol.ALGEBRAv3.database_name


class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
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

    @property
    def pool(self) -> poolv3:
        if self._pool is None:
            self._pool = poolv3(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_cached(gamma.hypervisor.gamma_hypervisor_cached):
    SAVE2FILE = True

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

    @property
    def pool(self) -> poolv3_cached:
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
            self._pool = poolv3_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_multicall(gamma.hypervisor.gamma_hypervisor_multicall):
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
        pool_abi_filename: str = "",
        pool_abi_path: str = "",
    ):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        self._pool_abi_filename = pool_abi_filename or POOL_ABI_FILENAME
        self._pool_abi_path = (
            pool_abi_path or f"{self.abi_root_path}/{POOL_ABI_FOLDERNAME}"
        )

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            pool_abi_filename=self._pool_abi_filename,
            pool_abi_path=self._pool_abi_path,
        )

        if known_data:
            self._fill_from_known_data(known_data)

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def pool(self) -> poolv3_multicall:
        return self._pool

    def fill_with_multicall(
        self,
        pool_address: str | None = None,
        token0_address: str | None = None,
        token1_address: str | None = None,
    ):
        data = execute_multicall(
            network=self._network,
            block=self.block,
            hypervisor_address=self._address,
            pool_address=pool_address,
            token0_address=token0_address,
            token1_address=token1_address,
            hypervisor_abi_filename=self._abi_filename,
            hypervisor_abi_path=self._abi_path,
            pool_abi_filename=self._pool_abi_filename,
            pool_abi_path=self._pool_abi_path,
            convert_bint=False,
        )

        # fill addresses
        if pool_address:
            data["pool"]["address"] = pool_address
        if token0_address:
            data["token0"]["address"] = token0_address
        if token1_address:
            data["token1"]["address"] = token1_address

        self._fill_from_known_data(known_data=data)

    def _fill_from_known_data_objects(self, known_data: dict):
        self._pool = poolv3_multicall(
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


class gamma_hypervisor_bep20(gamma_hypervisor):
    @property
    def pool(self) -> poolv3_bep20:
        if self._pool is None:
            self._pool = poolv3_bep20(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def token0(self) -> bep20:
        if self._token0 is None:
            self._token0 = bep20(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20:
        if self._token1 is None:
            self._token1 = bep20(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1


class gamma_hypervisor_bep20_cached(gamma.hypervisor.gamma_hypervisor_bep20_cached):
    SAVE2FILE = True

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

    @property
    def pool(self) -> poolv3_bep20_cached:
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
            self._pool = poolv3_bep20_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_bep20_multicall(gamma_hypervisor_multicall):
    @property
    def token0(self) -> bep20_multicall:
        return self._token0

    @property
    def token1(self) -> bep20_multicall:
        return self._token1

    def _fill_from_known_data_objects(self, known_data: dict):
        self._pool = poolv3_multicall(
            address=known_data["pool"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["pool"],
        )
        self._token0 = bep20_multicall(
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token0"],
        )
        self._token1 = bep20_multicall(
            address=known_data["token1"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )
