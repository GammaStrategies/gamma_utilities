import contextlib
import sys
import os
import logging
from datetime import timezone
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pathlib import Path
import tqdm
import concurrent.futures

from datetime import datetime, timedelta

# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION
from bins.general import general_utilities, file_utilities

from bins.converters.onchain import convert_hypervisor_fromDict

from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_algebra,
    gamma_hypervisor_zyberswap,
    gamma_hypervisor_thena,
    gamma_hypervisor_registry,
    masterChef_registry,
    masterchef_v1,
    masterchef_rewarder,
)


def flatten_dict(my_dict: dict, existing_dict: dict = None, add_key: str = ""):
    if existing_dict is None:
        existing_dict = {}
    for k, v in my_dict.items():
        if not isinstance(v, dict):
            existing_dict[f"{add_key}.{k}" if add_key else k] = v
        else:
            flatten_dict(my_dict=v, existing_dict=existing_dict, add_key=k)
    return existing_dict


def construct_hype(
    network: str, dex: str, block: int, hypervisor_address: str
) -> gamma_hypervisor:
    # Setup vars
    web3Provider_url = CONFIGURATION["sources"]["web3Providers"][network]
    # setup onchain dta provider
    web3Provider = Web3(
        Web3.HTTPProvider(web3Provider_url, request_kwargs={"timeout": 120})
    )
    # add middleware as needed
    if network != "ethereum":
        web3Provider.middleware_onion.inject(geth_poa_middleware, layer=0)

    ## WEB3 INFO ##
    # get onchain data and set the block num. on all queries from now on
    if dex == "uniswapv3":
        gamma_web3Helper = gamma_hypervisor(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "quickswap":
        gamma_web3Helper = gamma_hypervisor_quickswap(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "zyberswap":
        gamma_web3Helper = gamma_hypervisor_zyberswap(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "thena":
        gamma_web3Helper = gamma_hypervisor_thena(
            address=hypervisor_address, network=network, block=block
        )
    else:
        raise NotImplementedError(f"unsupported dex {dex}")

    return gamma_web3Helper


def print_values(hype: gamma_hypervisor, toDecimal: bool = True):
    print(" ------------- ")
    print(f"network: {hype._network}")
    print(f"hypervisor_address: {hype.address}")
    print(f"block: {hype.block}")

    hype_dict = convert_hypervisor_fromDict(
        hypervisor=hype.as_dict(), toDecimal=toDecimal
    )

    flat_dict = flatten_dict(my_dict=hype_dict)
    for k, v in flat_dict.items():
        if k not in ["network", "address", "block"]:
            print(f"{k}: {v}")


def print_multiblock_values(
    network: str,
    dex: str,
    blocks: list,
    hypervisor_address: str,
    toDecimal: bool = True,
):
    for block in blocks:
        print_values(
            hype=construct_hype(
                network=network,
                dex=dex,
                block=block,
                hypervisor_address=hypervisor_address,
            ),
            toDecimal=toDecimal,
        )
        print("  ")
        print("  ")


def saveCsv_multiblock_values(
    network: str,
    dex: str,
    blocks: list,
    hypervisor_address: str,
    toDecimal: bool = True,
    folder: str = None,
    filename: str = None,
):
    if folder is None:
        folder = os.path.join(PARENT_FOLDER, "tests")
    if filename is None:
        filename = f"{network}_{dex}_{hypervisor_address}.csv"

    result = []
    for block in tqdm.tqdm(blocks):
        hype = construct_hype(
            network=network,
            dex=dex,
            block=block,
            hypervisor_address=hypervisor_address,
        )
        hype_dict = flatten_dict(
            my_dict=convert_hypervisor_fromDict(
                hypervisor=hype.as_dict(), toDecimal=toDecimal
            )
        )

        result.append(hype_dict)

    result_to_csv(result=result, folder=folder, filename=filename)


def result_to_csv(
    result: list[dict], folder: str, filename: str, csv_columns: list = None
):
    """save data to csv file

    Args:
        result (list[dict]): list of dicts
        folder (str): where to save
        filename (str):
    """

    if csv_columns is None:
        csv_columns = []
    csv_columns.extend([x for x in list(result[-1].keys()) if x not in csv_columns])

    csv_filename = os.path.join(folder, filename)

    # remove file
    with contextlib.suppress(Exception):
        os.remove(csv_filename)
    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=result)


# START ####################################################################################################################

if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    saveCsv_multiblock_values(
        network="ethereum",
        dex="uniswapv3",
        blocks=[16505177, 16719387],
        hypervisor_address="0xa3ecb6e941e773c6568052a509a04cf455a752ae",
        toDecimal=True,
    )
    # print_multiblock_values(
    #     network="ethereum",
    #     dex="uniswapv3",
    #     blocks=[16505177, 16719387],
    #     hypervisor_address="0xa3ecb6e941e773c6568052a509a04cf455a752ae",
    #     toDecimal=True,
    # )

    # end time log
    logging.getLogger(__name__).info(
        f" took {general_utilities.log_time_passed.get_timepassed_string(_startime)} to complete"
    )

    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
