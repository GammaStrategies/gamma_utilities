from ....general.enums import Protocol
from .. import pancakeswap

# TODO: https://polygonscan.com/address/0x8486881bcbda4f6f494e9a4e7dfa37f24aa80cb0#readContract


DEX_NAME = Protocol.ASCENT.database_name


class pool(pancakeswap.pool.pool):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_cached(pancakeswap.pool.pool_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_multicall(pancakeswap.pool.pool_multicall):
    def identify_dex_name(self) -> str:
        return DEX_NAME
