from copy import deepcopy
import logging
from web3 import Web3

from bins.errors.general import ProcessingError
from ....general.enums import Protocol, error_identity, text_to_chain
from ..camelot.pool import (
    pool as camelot_pool,
    pool_cached as camelot_pool_cached,
    pool_multicall as camelot_pool_multicall,
    ABI_FILENAME,
    ABI_FOLDERNAME,
)


DEX_NAME = Protocol.HERCULES.database_name


class pool(camelot_pool):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_cached(camelot_pool_cached):
    def identify_dex_name(self) -> str:
        return DEX_NAME


class pool_multicall(camelot_pool_multicall):
    def identify_dex_name(self) -> str:
        return DEX_NAME
