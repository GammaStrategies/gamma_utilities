import sys
import os

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
    "polygon_zk": 1101,
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
        },
        "MasterChefV2Registry": {},
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
            "thena": "0xd4bcFC023736Db5617E5638748E127581d5929bd",
        },
        "MasterChefV2Registry": {},
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
