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


def add_timestamps_to_status(network: str, protocol: str = "gamma"):
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # get a list of timestamps from database
    all_blocks = {
        x["block"]: x["timestamp"]
        for x in global_db_manager.get_items_from_database(collection_name="blocks")
    }

    all_status = local_db_manager.get_items_from_database(collection_name="status")

    _errors = 0
    with tqdm.tqdm(total=len(all_status)) as progress_bar:

        def loopme(status):

            if "timestamp" in status:
                # item already with data
                return status, True

            # control var
            saveit = False
            try:
                # get timestamp from database
                status["timestamp"] = all_blocks[status["block"]]
                status["pool"]["timestamp"] = status["timestamp"]
                status["pool"]["token0"]["timestamp"] = status["timestamp"]
                status["pool"]["token1"]["timestamp"] = status["timestamp"]

                saveit = True
            except:
                pass
            if not saveit:
                try:
                    # get timestamp from web3 call
                    status["timestamp"] = (
                        erc20_cached(
                            address="0x0000000000000000000000000000000000000000",
                            network=network,
                        )
                        ._w3.eth.get_block(status["block"])
                        .timestamp
                    )
                    status["pool"]["timestamp"] = status["timestamp"]
                    status["pool"]["token0"]["timestamp"] = status["timestamp"]
                    status["pool"]["token1"]["timestamp"] = status["timestamp"]

                    saveit = True
                except:
                    pass

            if saveit:
                # save modified status to database
                local_db_manager.set_status(data=status)
                return status, True
            else:
                logging.getLogger(__name__).warning(
                    f" Can't get timestamp for hypervisor {status['address']}   id: {status['id']}"
                )
                return status, False

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for status, result in ex.map(loopme, all_status):
                if not result:
                    _errors += 1

                progress_bar.set_description(
                    f"[{_errors}]  Updating status database {network}'s {status['address']} block {status['block']}"
                )

                # update progress
                progress_bar.update(1)


# TODO: implement tqdm
def check_database():

    # setup global database manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    # check LOCAL
    for protocol, networks in CONFIGURATION["script"]["protocols"].items():
        for network, dexes in networks.items():

            # setup local database manager
            db_name = f"{network}_{protocol}"
            local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

            # check blocks
            chek_localdb_blocks(local_db_manager=local_db_manager)

    # check GLOBAL
    # check blocks
    chek_globaldb_blocks(global_db_manager=global_db_manager)


def chek_localdb_blocks(local_db_manager: database_local):
    """check if blocks are typed correctly

    Args:
        local_db_manager (database_local):
    """

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


def chek_globaldb_blocks(global_db_manager: database_global):
    """check that blocks have the correct type

    Args:
        global_db_manager (database_global):
    """

    blocks_usd_prices = global_db_manager.get_items_from_database(
        collection_name="usd_prices", find={"block": {"$not": {"$type": "int"}}}
    )
    # warn
    if len(blocks_usd_prices) > 0:
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_usd_prices)} usd prices with the block field not being int"
        )


def check_status_prices(
    network: str, local_db_manager: database_local, global_db_manager: database_global
):
    """Check that all status tokens have usd prices

    Args:
        local_db_manager (database_local):
        global_db_manager (database_global):
    """
    # get all prices + address + block
    prices = set(
        [
            x["id"]
            for x in global_db_manager.get_unique_prices_addressBlock(network=network)
        ]
    )

    # get tokens and blocks present in database
    prices_todo = set()
    for x in local_db_manager.get_items_from_database(collection_name=status, find={}):
        for i in [0, 1]:
            db_id = "{}_{}_{}".format(
                network,
                x["pool"][f"token{i}"]["block"],
                x["pool"][f"token{i}"]["address"],
            )
            if not db_id in prices:
                prices_todo.add(db_id)

    if len(prices_todo) > 0:
        logging.getLogger(__name__).warning(
            " Found {} token blocks without price, from a total of {} ({:,.1%})".format(
                len(prices_todo), len(prices), len(prices_todo) / len(prices)
            )
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

    add_timestamps_to_status(network="ethereum")
    # replace_blocks_to_int(network="ethereum")

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
