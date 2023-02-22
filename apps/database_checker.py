import os
import sys
import logging
import tqdm
import concurrent.futures
from datetime import datetime
from pathlib import Path
from web3.exceptions import ContractLogicError

if __name__ == "__main__":
    # append parent directory pth
    CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
    PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
    sys.path.append(PARENT_FOLDER)


from bins.configuration import CONFIGURATION, HYPERVISOR_REGISTRIES
from bins.general.general_utilities import (
    convert_string_datetime,
    differences,
    log_time_passed,
)
from bins.w3.onchain_data_helper import onchain_data_helper2
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_registry,
)
from bins.w3.onchain_utilities.basic import erc20_cached

from bins.database.common.db_collections_common import database_local, database_global
from bins.mixed.price_utilities import price_scraper

from bins.formulas.univ3_formulas import sqrtPriceX96_to_price_float


def replace_blocks_to_int(network: str, protocol: str = "gamma"):

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # get all prices
    all_prices = global_db_manager.get_items_from_database(
        collection_name="usd_prices", find={"block": {"$type": "string"}}
    )
    _errors = 0
    with tqdm.tqdm(total=len(all_prices)) as progress_bar:

        def loopme(price):
            global_db_manager.set_price_usd(
                network=price["network"],
                block=price["block"],
                token_address=price["address"],
                price_usd=price["price"],
            )
            return price

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for price in ex.map(loopme, all_prices):
                progress_bar.set_description(
                    f"Updating database {price['network']}'s block {price['block']}"
                )
                # update progress
                progress_bar.update(1)


def check_database():

    # setup global database manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    for protocol, networks in CONFIGURATION["script"]["protocols"].items():
        for network, dexes in networks.items():
            # setup local database manager
            db_name = f"{network}_{protocol}"
            local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

            # check operation blocks are int
            blocks_operatons = local_db_manager.get_items_from_database(
                collection_name="operations",
                find={"blockNumber": {"$not": {"$type": "int"}}},
            )
            # warn
            if len(blocks_operatons) > 0:
                logging.getLogger(__name__).warning(
                    f" Found {len(blocks_operatons)} operations with the block field not being int"
                )

            # check status blocks are int
            blocks_status = local_db_manager.get_items_from_database(
                collection_name="status", find={"block": {"$not": {"$type": "int"}}}
            )
            # warn
            if len(blocks_status) > 0:
                logging.getLogger(__name__).warning(
                    f" Found {len(blocks_status)} hypervisor status with the block field not being int"
                )


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )
    # start time log
    _startime = datetime.utcnow()

    replace_blocks_to_int(network="ethereum")

    # end time log
    # _timelapse = datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {} to complete".format(
            log_time_passed.get_timepassed_string(start_time=_startime)
        )
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
