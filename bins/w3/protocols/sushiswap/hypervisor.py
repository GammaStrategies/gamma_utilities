from ....general.enums import Protocol
from .. import uniswap

DEX_NAME = Protocol.SUSHI.database_name


class gamma_hypervisor(uniswap.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class gamma_hypervisor_cached(uniswap.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME
