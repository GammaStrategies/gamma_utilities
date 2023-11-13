from ..general.enums import Chain

# from .loader.all import load_configuration

# load current configuratoion
# CFG = load_configuration()


WEB3_CHAIN_IDS = {chain.database_name: chain.id for chain in Chain}


# The app will fill this with real data, when no specified here
BLOCKS_PER_SECOND = {}


