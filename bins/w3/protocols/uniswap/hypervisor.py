from ....general.enums import Protocol
from .. import gamma


DEX_NAME = Protocol.UNISWAPv3.database_name


class gamma_hypervisor(gamma.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return DEX_NAME


# TODO: simplify with inheritance
class gamma_hypervisor_bep20(gamma.hypervisor.gamma_hypervisor_bep20):
    def identify_dex_name(self) -> str:
        return DEX_NAME


# -> Cached version of the hypervisor


class gamma_hypervisor_cached(gamma.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class gamma_hypervisor_bep20_cached(gamma.hypervisor.gamma_hypervisor_bep20_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME
