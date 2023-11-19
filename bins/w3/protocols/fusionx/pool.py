from ....general.enums import Protocol
from .. import pancakeswap


DEX_NAME = Protocol.FUSIONX.database_name


class pool(pancakeswap.pool.pool):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_cached(pancakeswap.pool.pool_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_multicall(pancakeswap.pool.pool_multicall):
    def identify_dex_name(self) -> str:
        return DEX_NAME
