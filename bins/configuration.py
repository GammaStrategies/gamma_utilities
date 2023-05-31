import sys
import os
from bins.general.enums import Chain, Protocol

from bins.general.general_utilities import (
    load_configuration,
    check_configuration_file,
)
from bins.general.command_line import parse_commandLine_args
from bins.log import log_helper

CONFIGURATION = {}

# convert command line arguments to dict variables
cml_parameters = parse_commandLine_args()


# load configuration
CONFIGURATION = (
    load_configuration(cfg_name=cml_parameters.config)
    if cml_parameters.config
    else load_configuration()
)

# check configuration
check_configuration_file(CONFIGURATION)

# add cml_parameters into loaded config ( this is used later on to load again the config file to be able to update on-the-fly vars)
if "_custom_" not in CONFIGURATION.keys():
    CONFIGURATION["_custom_"] = {}
CONFIGURATION["_custom_"]["cml_parameters"] = cml_parameters

# add log subfolder if set
if CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder:
    CONFIGURATION["logs"]["save_path"] = os.path.join(
        CONFIGURATION["logs"]["save_path"],
        CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder,
    )

# setup logging
log_helper.setup_logging(customconf=CONFIGURATION)

# add temporal variables while the app is running so memory is kept
CONFIGURATION["_custom_"]["temporal_memory"] = {}


def add_to_memory(key, value):
    """Add to temporal memory a key and value"""
    if key not in CONFIGURATION["_custom_"]["temporal_memory"]:
        CONFIGURATION["_custom_"]["temporal_memory"][key] = []

    if value not in CONFIGURATION["_custom_"]["temporal_memory"][key]:
        CONFIGURATION["_custom_"]["temporal_memory"][key].append(value)


def get_from_memory(key) -> list:
    """Get value from temporal memory"""
    try:
        return CONFIGURATION["_custom_"]["temporal_memory"][key]
    except KeyError:
        return []


#### ADD STATIC CONFIG HERE ####

WEB3_CHAIN_IDS = {
    "ethereum": 1,
    "polygon": 137,
    "optimism": 10,
    "arbitrum": 42161,
    "celo": 42220,
    "binance": 56,
    "polygon_zkevm": 1101,
    "avalanche": 43114,
}


STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswapv3": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
        },
        "MasterChefV2Registry": {},
        "feeDistributors": [
            "0x07432C021f0A65857a3Ab608600B9FEABF568EA0",
            "0x8451122f06616baff7feb10afc2c4f4132fc4709",
        ],
    },
    "polygon": {
        "hypervisors": {
            "uniswapv3": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
            "quickswap": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd",
        },
        "MasterChefRegistry": "0x135B02F8b110Fe2Dd8B6a5e2892Ee781264c2fbe",
        "MasterChefV2Registry": {
            "uniswapv3": "0x02C8D3FCE5f072688e156F503Bd5C7396328613A",
            "quickswap": "0x62cD3612233B2F918BBf0d17B9Eda3005b84e16f",
        },
    },
    "optimism": {
        "hypervisors": {
            "uniswapv3": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
        },
        "MasterChefV2Registry": {
            "uniswapv3": "0x81d9bF667205662bfa729C790F67D97D54EA391C",
        },
    },
    "arbitrum": {
        "hypervisors": {
            "uniswapv3": "0x66CD859053c458688044d816117D5Bdf42A56813",
            "zyberswap": "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9",
            "camelot": "0xa216C2b6554A0293f69A1555dd22f4b7e60Fe907",
        },
        "MasterChefV2Registry": {
            "camelot": "0x26da8473AaA54e8c7835fA5fdd1599eB4c144d31",
        },
        "zyberswap_v1_masterchefs": [
            "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd",
        ],
    },
    "celo": {
        "hypervisors": {
            "uniswapv3": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
        },
        "MasterChefV2Registry": {},
    },
    "binance": {
        "hypervisors": {
            "uniswapv3": "0x0b4645179C1b668464Df01362fC6219a7ab3234c",
            "thena": "0xd4bcFC023736Db5617E5638748E127581d5929bd",
        },
        "MasterChefV2Registry": {},
    },
    "polygon_zkevm": {
        "hypervisors": {
            "quickswap": "0xD08B593eb3460B7aa5Ce76fFB0A3c5c938fd89b8",
        },
    },
}


KNOWN_VALID_MASTERCHEFS = {
    "polygon": {
        "uniswapv3": ["0x570d60a60baa356d47fda3017a190a48537fcd7d"],
        "quickswap": [
            "0x20ec0d06f447d550fc6edee42121bc8c1817b97d",
            "0x68678cf174695fc2d27bd312df67a3984364ffdd",
        ],
    },
    "optimism": {
        "uniswapv3": ["0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365"],
    },
}


#### STATIC PRICE ORACLES PATH ####


DEX_POOLS = {
    Chain.ETHEREUM: {
        "USDC_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        },
        "WETH_RPL": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xe42318ea3b998e8355a3da364eb9d48ec725eb45",
        },
        "GAMMA_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4006bed7bf103d70a1c6b7f1cef4ad059193dc25",
        },
        "AXL_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x5b0d2536f0c970b8d9cbf3959460fb97ce808ade",
        },
    },
    Chain.OPTIMISM: {
        "WETH_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
        },
        "WETH_OP": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x68f5c0a2de713a54991e01858fd27a3832401849",
        },
    },
    Chain.POLYGON: {
        "WMATIC_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xae81fac689a1b4b1e06e7ef4a2ab4cd8ac0a087d",
        },
        "WMATIC_QI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x5cd94ead61fea43886feec3c95b1e9d7284fdef3",
        },
        "WMATIC_QUICK": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x9f1a8caf3c8e94e43aa64922d67dff4dc3e88a42",
        },
        "WMATIC_DQUICK": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xb8d00c66accdc01e78fd7957bf24050162916ae2",
        },
        "WMATIC_GHST": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x80deece4befd9f27d2df88064cf75f080d3ce1b2",
        },
        "WMATIC_ANKR": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x2f2dd65339226df7441097a710aba0f493879579",
        },
        "USDC_DAVOS": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xfb0bc232cd11dbe804b489860c470b7f9cc80d9f",
        },
        "USDC_GIDDY": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x65c30f39b880bdd9616280450c4b41cc74b438b7",
        },
        "WMATIC_LCD": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xd9c2c978915b907df04972cb3f577126fe55143c",
        },
        "WOMBAT_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xaf835698673655e9910de8398df6c5238f5d3aeb",
        },
        "USDC_FIS": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x2877703a3ba3e712d684d22bd6d60cc0031d84e8",
        },
        "SD_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x5d0acfa39a0fca603147f1c14e53f46be76984bc",
        },
        "USDC_DAI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xe7e0eb9f6bcccfe847fdf62a3628319a092f11a2",
        },
    },
    Chain.POLYGON_ZKEVM: {
        "WETH_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xc44ad482f24fd750caeba387d2726d8653f8c4bb",
        },
        "QUICK_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x1247b70c4b41890e8c1836e88dd0c8e3b23dd60e",
        },
        "WETH_MATIC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xb73abfb5a2c89f4038baa476ff3a7942a021c196",
        },
    },
    Chain.BSC: {
        "THE_WBNB": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x51bd5e6d3da9064d59bcaa5a76776560ab42ceb8",
        },
    },
    Chain.AVALANCHE: {},
    Chain.ARBITRUM: {
        "DAI_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xf0428617433652c9dc6d1093a42adfbf30d29f74",
        },
    },
    Chain.MOONBEAM: {},
}


DEX_POOLS_PRICE_PATHS = {
    Chain.ETHEREUM: {
        # GAMMA
        "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197": [
            (DEX_POOLS[Chain.ETHEREUM]["GAMMA_WETH"], 1),
            (
                DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"],
                0,
            ),
        ],
        # RPL
        "0xd33526068d116ce69f19a9ee46f0bd304f21a51f": [
            (
                DEX_POOLS[Chain.ETHEREUM]["WETH_RPL"],
                0,
            ),
            (
                DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"],
                0,
            ),
        ],
        # AXL
        "0x467719ad09025fcc6cf6f8311755809d45a5e5f3": [
            (DEX_POOLS[Chain.ETHEREUM]["AXL_USDC"], 1)
        ],
    },
    Chain.OPTIMISM: {
        # OP
        "0x4200000000000000000000000000000000000042": [
            (DEX_POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (DEX_POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
        # MOCK-OPT
        "0x601e471de750cdce1d5a2b8e6e671409c8eb2367": [
            (DEX_POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (DEX_POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
    },
    Chain.POLYGON: {
        # USDC
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": [],
        # WMATIC
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1)
        ],
        # QI
        "0x580a84c73811e1839f75d86d75d88cca0c241ff4": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_QI"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # QUICK
        "0xb5c064f955d8e7f38fe0460c556a72987494ee17": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_QUICK"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # dQUICK
        "0x958d208cdf087843e9ad98d23823d32e17d723a1": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_DQUICK"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # GHST
        "0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_GHST"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # ANKR
        "0x101a023270368c0d50bffb62780f4afd4ea79c35": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_ANKR"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # DAVOS
        "0xec38621e72d86775a89c7422746de1f52bba5320": [
            (DEX_POOLS[Chain.POLYGON]["USDC_DAVOS"], 0)
        ],
        # GIDDY
        "0x67eb41a14c0fe5cd701fc9d5a3d6597a72f641a6": [
            (DEX_POOLS[Chain.POLYGON]["USDC_GIDDY"], 0)
        ],
        # LCD
        "0xc2a45fe7d40bcac8369371b08419ddafd3131b4a": [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_LCD"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # WOMBAT
        "0x0c9c7712c83b3c70e7c5e11100d33d9401bdf9dd": [
            (DEX_POOLS[Chain.POLYGON]["WOMBAT_USDC"], 1),
        ],
        # FIS
        "0x7a7b94f18ef6ad056cda648588181cda84800f94": [
            (DEX_POOLS[Chain.POLYGON]["USDC_FIS"], 0),
        ],
        # SD
        "0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94": [
            (DEX_POOLS[Chain.POLYGON]["SD_USDC"], 1),
        ],
        # DAI
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": [
            (DEX_POOLS[Chain.POLYGON]["USDC_DAI"], 0),
        ],
    },
    Chain.POLYGON_ZKEVM: {
        # WMATIC
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_MATIC"], 0),
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # QUICK
        "0x68286607a1d43602d880d349187c3c48c0fd05e6": [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["QUICK_USDC"], 1),
        ],
        # WETH
        "0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9": [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
    },
    Chain.BSC: {
        # # THE
        # "0xf4c8e32eadec4bfe97e0f595add0f4450a863a11":
        # [
        # ],
    },
    Chain.AVALANCHE: {},
    Chain.ARBITRUM: {
        # DAI
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": [
            (DEX_POOLS[Chain.ARBITRUM]["DAI_USDC"], 1),
        ]
    },
    Chain.MOONBEAM: {},
}
