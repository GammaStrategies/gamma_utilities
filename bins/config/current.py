from ..general.enums import Chain

from .loader.all import load_configuration

# load current configuration: from command line + file + database ( first prevail over second, second over third)
# CFG = load_configuration()

WEB3_CHAIN_IDS = {chain.database_name: chain.id for chain in Chain}


# The app will fill this with real data, when no specified here
BLOCKS_PER_SECOND = {}


MULTICALL3_ADDRESSES = {
    "default": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
    # <chain>: <address> to lower
}
