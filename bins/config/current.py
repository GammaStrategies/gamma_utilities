from ..general.enums import Chain

from .loader.all import load_configuration

# load current configuration: from command line + file + database ( first prevail over second, second over third)
# CFG = load_configuration()

WEB3_CHAIN_IDS = {chain.database_name: chain.id for chain in Chain}


# The app will fill this with real data, when no specified here
BLOCKS_PER_SECOND = {}


MULTICALL3_ADDRESSES = {
    "default": {"address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower()},
    Chain.ETHEREUM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 14353601,
    },
    Chain.ARBITRUM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 7654707,
    },
    Chain.OPTIMISM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 4286263,
    },
    Chain.POLYGON: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 25770160,
    },
    Chain.POLYGON_ZKEVM: {
        "address": "0xca11bde05977b3631167028862be2a173976ca11".lower(),
        "block": 57746,
    },
    Chain.AVALANCHE: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 11907934,
    },
    Chain.FANTOM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 33001987,
    },
    Chain.BSC: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 15921452,
    },
    Chain.OPBNB: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 512881,
    },
    Chain.MOONBEAM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 609002,
    },
    Chain.CELO: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 13112599,
    },
    Chain.KAVA: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 3661165,
    },
    Chain.MANTLE: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 304717,
    },
    Chain.BASE: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 5022,
    },
    Chain.LINEA: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 42,
    },
    Chain.METIS: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 2338552,
    },
    Chain.MANTA: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 332890,
    },
    Chain.GNOSIS: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 21022491,
    },
    Chain.ROLLUX: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 119222,
    },
    Chain.ASTAR_ZKEVM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 183817,
    },
    Chain.IMMUTABLE_ZKEVM: {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11".lower(),
        "block": 3680945,
    },
}
