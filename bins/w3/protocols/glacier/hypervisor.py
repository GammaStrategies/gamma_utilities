from ....general.enums import Protocol
from .. import algebra

from ..algebra.pool import poolv3, poolv3_cached

DEX_NAME = Protocol.GLACIER.database_name


class gamma_hypervisor(algebra.hypervisor.gamma_hypervisor):
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


class gamma_hypervisor_cached(algebra.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME

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
