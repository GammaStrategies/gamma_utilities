from .. import gamma
from bins.general.enums import Protocol
from .pool import pool, pool_cached, pool_bep20, pool_bep20_cached

DEX_NAME = Protocol.PANCAKESWAP.database_name


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


class gamma_hypervisor_bep20(gamma.hypervisor.gamma_hypervisor_bep20):
    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def pool(self) -> pool_bep20:
        if self._pool is None:
            self._pool = pool_bep20(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_bep20_cached(
    gamma_hypervisor_bep20, gamma.hypervisor.gamma_hypervisor_bep20_cached
):
    @property
    def pool(self) -> pool_bep20_cached:
        if self._pool is None:
            self._pool = pool_bep20_cached(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool
