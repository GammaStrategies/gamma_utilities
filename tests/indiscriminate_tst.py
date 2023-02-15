import sys
import os
import datetime as dt
import logging
from web3 import Web3
from pathlib import Path
import tqdm
import concurrent.futures


# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION
from bins.general import general_utilities, file_utilities

from bins.w3.onchain_utilities import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_registry,
)

from bins.log import log_helper


def test_w3_hypervisor_obj(
    protocol: str, network: str, dex: str, hypervisor_address: str, block: int
):
    hypervisor = None
    if dex == "uniswap_v3":
        hypervisor = gamma_hypervisor_cached(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "quickswap":
        hypervisor = gamma_hypervisor_quickswap_cached(
            address=hypervisor_address, network=network, block=block
        )
    else:
        raise NotImplementedError(f" {dex} exchange has not been implemented yet")

    # test fees
    po = hypervisor.as_dict()
    test = ""


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )
    # start time log
    _startime = dt.datetime.utcnow()

    ########
    ########
    protocol = "gamma"
    network = "polygon"
    dex = "uniswap_v3"
    hypervisor_address = "0x02203f2351e7ac6ab5051205172d3f772db7d814"
    block = 38525261

    test_w3_hypervisor_obj(
        protocol=protocol,
        network=network,
        dex=dex,
        hypervisor_address=hypervisor_address,
        block=block,
    )

    # end time log
    _timelapse = dt.datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} minutes to complete".format(_timelapse.total_seconds() / 60)
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
