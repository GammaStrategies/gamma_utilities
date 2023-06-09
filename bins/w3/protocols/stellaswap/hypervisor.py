from bins.general.enums import Protocol
from bins.w3.protocols import algebra


class gamma_hypervisor(algebra.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return Protocol.STELLASWAP.database_name


class gamma_hypervisor_cached(algebra.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return Protocol.STELLASWAP.database_name
