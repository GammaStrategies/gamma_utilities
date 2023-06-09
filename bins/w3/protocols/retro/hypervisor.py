from bins.general.enums import Protocol
from bins.w3.protocols import uniswap


class gamma_hypervisor(uniswap.hypervisor.gamma_hypervisor):
    def identify_dex_name(self) -> str:
        return Protocol.RETRO.database_name


class gamma_hypervisor_cached(uniswap.hypervisor.gamma_hypervisor_cached):
    def identify_dex_name(self) -> str:
        return Protocol.RETRO.database_name
