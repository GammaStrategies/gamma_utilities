from ....general.enums import Protocol
from .. import gamma

DEX_NAME = Protocol.APERTURE.database_name


class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class gamma_hypervisor_cached(gamma.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class gamma_hypervisor_multicall(gamma.hypervisor.gamma_hypervisor_multicall):
    def identify_dex_name(self) -> str:
        return DEX_NAME
