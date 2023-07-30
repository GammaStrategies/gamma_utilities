from ....general.enums import Protocol
from .. import algebra


class gamma_hypervisor(algebra.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return Protocol.ZYBERSWAP.database_name


class gamma_hypervisor_cached(algebra.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return Protocol.ZYBERSWAP.database_name
