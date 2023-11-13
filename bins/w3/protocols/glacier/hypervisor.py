from ....general.enums import Protocol
from .. import algebra

DEX_NAME = Protocol.GLACIER.database_name


class gamma_hypervisor(algebra.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class gamma_hypervisor_cached(algebra.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class gamma_hypervisor_multicall(algebra.hypervisor.gamma_hypervisor_multicall):
    def identify_dex_name(self) -> str:
        return DEX_NAME
