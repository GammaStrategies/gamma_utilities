from ....general.enums import Protocol
from .. import algebra

from ..camelot.pool import pool, pool_cached


class gamma_hypervisor(algebra.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return Protocol.CAMELOT.database_name

    @property
    def pool(self) -> pool:
        if self._pool is None:
            self._pool = pool(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool


# TODO: simplify with class inheritance
class gamma_hypervisor_cached(algebra.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return Protocol.CAMELOT.database_name

    @property
    def pool(self) -> pool_cached:
        if self._pool is None:
            self._pool = pool_cached(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool
