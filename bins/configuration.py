import sys

from bins.general.general_utilities import (
    convert_commandline_arguments,
    load_configuration,
    check_configuration_file,
)
from bins.general.command_line import parse_commandLine_args
from bins.log import log_helper

CONFIGURATION = dict()

# convert command line arguments to dict variables
cml_parameters = parse_commandLine_args()
# cml_parameters = convert_commandline_arguments(sys.argv[1:])

# load configuration
CONFIGURATION = (
    load_configuration(cfg_name=cml_parameters.config)
    if cml_parameters.config
    else load_configuration()
)

# check configuration
check_configuration_file(CONFIGURATION)

# setup logging
log_helper.setup_logging(customconf=CONFIGURATION)

# add cml_parameters into loaded config ( this is used later on to load again the config file to be able to update on-the-fly vars)
if not "_custom_" in CONFIGURATION.keys():
    CONFIGURATION["_custom_"] = dict()
CONFIGURATION["_custom_"]["cml_parameters"] = cml_parameters


#### ADD STATIC CONFIG HERE ####

HYPERVISOR_REGISTRIES = {
    "uniswapv3": {
        "ethereum": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
        "polygon": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
        "optimism": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
        "arbitrum": "0x66CD859053c458688044d816117D5Bdf42A56813",
        "celo": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
    },
    "quickswap": {
        "polygon": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd",
    },
}


STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswapv3": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
        },
        "rewards": {},
    },
    "polygon": {
        "hypervisors": {
            "uniswapv3": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
            "quickswap": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd",
        },
        "rewards": {},
    },
    "optimism": {
        "hypervisors": {
            "uniswapv3": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
        },
        "rewards": {},
    },
    "arbitrum": {
        "hypervisors": {
            "uniswapv3": "0x66CD859053c458688044d816117D5Bdf42A56813",
        },
        "rewards": {},
    },
    "celo": {
        "hypervisors": {
            "uniswapv3": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
        },
        "rewards": {},
    },
}
