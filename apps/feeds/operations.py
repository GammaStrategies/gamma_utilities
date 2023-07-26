import logging
from multiprocessing import Pool
import threading
import tqdm
from datetime import datetime
from apps.feeds.queue import build_and_save_queue_from_operation

# from croniter import croniter

from bins.configuration import CONFIGURATION
from bins.general.general_utilities import (
    convert_string_datetime,
    differences,
)
from bins.w3.onchain_data_helper import onchain_data_helper

from bins.database.common.db_collections_common import database_local


### Operations ######################
def feed_operations(
    protocol: str,
    network: str,
    block_ini: int | None = None,
    block_end: int | None = None,
    date_ini: datetime | None = None,
    date_end: datetime | None = None,
    force_back_time: bool = False,
):
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors operations information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    filters = CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})

    # create local database manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # create a web3 protocol helper
    onchain_helper = onchain_data_helper(protocol=protocol)

    try:
        # set timeframe to scrape as dates (used as last option)
        if not date_ini:
            # get configured start date
            date_ini = filters.get("force_timeframe", {}).get(
                "start_time", "2021-03-24T00:00:00"
            )

            date_ini = convert_string_datetime(date_ini)
        if not date_end:
            # get configured end date
            date_end = filters.get("force_timeframe", {}).get("end_time", "now")
            if date_end == "now":
                # set block end to last block number
                # tmp_w3 = onchain_helper.create_erc20_helper(network)
                block_end = (
                    onchain_helper.create_erc20_helper(network)
                    ._getBlockData("latest")
                    .number
                )

            date_end = convert_string_datetime(date_end)

        # apply filters
        hypes_not_included: list = [
            x.lower()
            for x in filters.get("hypervisors_not_included", {}).get(network, [])
        ]
        logging.getLogger(__name__).debug(
            f"   excluding hypervisors: {hypes_not_included}"
        )

        # get hypervisor addresses from static database collection and compare them to current operations distinct addresses
        # to decide whether a full timeback query shall be made
        logging.getLogger(__name__).debug(
            f"   Retrieving {network} hypervisors addresses from database"
        )
        hypervisor_static_in_database = {
            x["address"]: x
            for x in local_db.get_items_from_database(
                collection_name="static",
                find={"address": {"$nin": hypes_not_included}},
                projection={"address": 1, "block": 1, "timestamp": 1},
            )
            if x["address"] not in hypes_not_included
        }
        hypervisor_addresses = list(hypervisor_static_in_database.keys())
        hypervisor_addresses_in_operations = local_db.get_distinct_items_from_database(
            collection_name="operations",
            field="address",
            condition={"address": {"$nin": hypes_not_included}},
        )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"   Unexpected error preparing operations feed for {network}. Can't continue. : {e}"
        )
        # close operations feed
        return

    try:
        # try getting initial block as last found in database
        if not block_ini:
            block_ini = get_db_last_operation_block(protocol=protocol, network=network)
            logging.getLogger(__name__).debug(
                f"   Setting initial block to {block_ini}, being the last block found in operations database collection"
            )

            # check if hypervisors in static collection are diff from operation's
            if (
                force_back_time
                and hypervisor_addresses_in_operations
                and len(hypervisor_addresses) > len(hypervisor_addresses_in_operations)
            ):
                # get different addresses
                diffs = differences(
                    hypervisor_addresses, hypervisor_addresses_in_operations
                )
                # define a new initial block but traveling back time sufficienty to get missed ops
                # get minimum block from the new hypervisors found
                new_block_ini = min(
                    [
                        v["block"]
                        for k, v in hypervisor_static_in_database.items()
                        if k in diffs
                    ]
                )
                new_block_ini = (
                    new_block_ini if new_block_ini < block_ini else block_ini
                )
                # TODO: avoid hardcoded vars ( blocks back in time )
                # new_block_ini = block_ini - int(block_ini * 0.005)
                logging.getLogger(__name__).info(
                    f"   {len(diffs)} new hypervisors found in static but not in operations collections. Force initial block {block_ini} back time at {new_block_ini} [{block_ini-new_block_ini} blocks]"
                )
                logging.getLogger(__name__).info(f"   new hypervisors-->  {diffs}")
                # set initial block
                block_ini = new_block_ini

        # define block to scrape
        if not block_ini and not block_end:
            logging.getLogger(__name__).info(
                "   Calculating {} blocks to be processed using dates from {:%Y-%m-%d %H:%M:%S} to {:%Y-%m-%d %H:%M:%S} ".format(
                    network, date_ini, date_end
                )
            )
            block_ini, block_end = onchain_helper.get_custom_blockBounds(
                date_ini=date_ini,
                date_end=date_end,
                network=network,
                step="day",
            )
        elif not block_ini:
            # if static data exists pick the minimum block from it
            if hypervisor_static_in_database:
                logging.getLogger(__name__).info(
                    f"   Getting {network} initial block from the minimum static hype's collection block found"
                )
                block_ini = min(
                    [v["block"] for k, v in hypervisor_static_in_database.items()]
                )
            else:
                logging.getLogger(__name__).info(
                    "   Calculating {} initial block from date {:%Y-%m-%d %H:%M:%S}".format(
                        network, date_ini
                    )
                )
                block_ini, block_end_notused = onchain_helper.get_custom_blockBounds(
                    date_ini=date_ini,
                    date_end=date_end,
                    network=network,
                    step="day",
                )
        elif not block_end:
            logging.getLogger(__name__).info(
                "   Calculating {} end block from date {:%Y-%m-%d %H:%M:%S}".format(
                    network, date_end
                )
            )
            block_ini_notused, block_end = onchain_helper.get_custom_blockBounds(
                date_ini=date_ini,
                date_end=date_end,
                network=network,
                step="day",
            )

        # check for block range inconsistency
        if block_end < block_ini:
            raise ValueError(
                f" Initial block {block_ini} is higher than end block: {block_end}"
            )

        # feed operations
        feed_operations_hypervisors(
            network=network,
            protocol=protocol,
            hypervisor_addresses=hypervisor_addresses,
            block_ini=block_ini,
            block_end=block_end,
            local_db=local_db,
        )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while searching {network} for operations  .error: {e}"
        )


def feed_operations_hypervisors(
    network: str,
    protocol: str,
    hypervisor_addresses: list,
    block_ini: int,
    block_end: int,
    local_db: database_local,
):
    # set global protocol helper
    onchain_helper = onchain_data_helper(protocol=protocol)

    logging.getLogger(__name__).info(
        "   Feeding database with {}'s {} operations of {} hypervisors from blocks {} to {}".format(
            network, protocol, len(hypervisor_addresses), block_ini, block_end
        )
    )

    # create a task pool to handle operations
    tasks = []

    with tqdm.tqdm(total=100) as progress_bar:
        # create callback progress funtion
        def _update_progress(text=None, remaining=None, total=None):
            # set text
            if text:
                progress_bar.set_description(text)
            # set total
            if total:
                progress_bar.total = total
            # update current
            if remaining:
                progress_bar.update(((total - remaining) - progress_bar.n))
            else:
                progress_bar.update(1)
            # refresh
            progress_bar.refresh()

        # build task handler
        def task_handler():
            while True:
                for task in tasks[:]:
                    if task.ready():
                        tasks.remove(task)
                # update progress
                _update_progress(total=len(tasks))

        # start task handler
        task_thread = threading.Thread(target=task_handler)
        task_thread.start()

        # inside a pool
        with Pool() as p:
            for operation in onchain_helper.operations_generator(
                addresses=hypervisor_addresses,
                network=network,
                block_ini=block_ini,
                block_end=block_end,
                progress_callback=_update_progress,
                max_blocks=1000,
            ):
                # add task
                tasks.append(
                    p.apply_async(
                        task_process_operation,
                        (operation, local_db, network),
                    )
                )

        logging.getLogger(__name__).info(
            f"  Finished finding {network} operations, waiting for {len(tasks)} tasks to finish..."
        )
        # join thread to close
        task_thread.join()


def task_process_operation(operation: dict, local_db: database_local, network: str):
    # set operation id (same hash has multiple operations)
    operation["id"] = f"""{operation["logIndex"]}_{operation["transactionHash"]}"""
    # lower case address ( to ease comparison )
    operation["address"] = operation["address"].lower()
    local_db.set_operation(data=operation)

    # make sure hype is not in status collection already
    if not local_db.get_items_from_database(
        collection_name="status",
        find={"id": f"{operation['address'].lower()}_{operation['blockNumber']}"},
        limit=1,
        projection={"id": 1},
    ):
        # fire scrape event on block regarding hypervisor and rewarders snapshots (status) and token prices
        # build queue events from operation
        build_and_save_queue_from_operation(operation=operation, network=network)
    else:
        logging.getLogger(__name__).debug(
            f"  Not pushing {operation['address']} hypervisor status queue item bcause its already in the database"
        )


def get_db_last_operation_block(protocol: str, network: str) -> int:
    """Get the last operation block from database

    Args:
        protocol (str):
        network (str):

    Returns:
        int: last block number or None if not found or error
    """
    # read last blocks from database
    try:
        # setup database manager
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{network}_{protocol}"
        local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
        batch_size = 100000

        block_list = sorted(
            [
                int(operation["blockNumber"])
                for operation in local_db_manager.get_items_from_database(
                    collection_name="operations",
                    find={},
                    projection={"blockNumber": 1},
                    batch_size=batch_size,
                )
            ],
            reverse=False,
        )

        return block_list[-1]
    except IndexError:
        logging.getLogger(__name__).debug(
            f" Unable to get last operation block bc no operations have been found for {network}'s {protocol} in db"
        )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while quering db operations for latest block  error:{e}"
        )

    return None


# OLD: replaced by feed_operations_hypervisors
# def feed_operations_hypervisors_OLD(
#     network: str,
#     protocol: str,
#     hypervisor_addresses: list,
#     block_ini: int,
#     block_end: int,
#     local_db: database_local,
# ):
#     # set global protocol helper
#     onchain_helper = onchain_data_helper(protocol=protocol)

#     logging.getLogger(__name__).info(
#         "   Feeding database with {}'s {} operations of {} hypervisors from blocks {} to {}".format(
#             network, protocol, len(hypervisor_addresses), block_ini, block_end
#         )
#     )
#     with tqdm.tqdm(total=100) as progress_bar:
#         # create callback progress funtion
#         def _update_progress(text, remaining=None, total=None):
#             progress_bar.set_description(text)
#             # set total
#             if total:
#                 progress_bar.total = total
#             # update current
#             if remaining:
#                 progress_bar.update(((total - remaining) - progress_bar.n))
#             else:
#                 progress_bar.update(1)
#             # refresh
#             progress_bar.refresh()

#         for operation in onchain_helper.operations_generator(
#             addresses=hypervisor_addresses,
#             network=network,
#             block_ini=block_ini,
#             block_end=block_end,
#             progress_callback=_update_progress,
#             max_blocks=1000,
#         ):
#             # set operation id (same hash has multiple operations)
#             operation[
#                 "id"
#             ] = f"""{operation["logIndex"]}_{operation["transactionHash"]}"""
#             # lower case address ( to ease comparison )
#             operation["address"] = operation["address"].lower()
#             local_db.set_operation(data=operation)

#             # make sure hype is not in status collection already
#             if not local_db.get_items_from_database(
#                 collection_name="status",
#                 find={
#                     "id": f"{operation['address'].lower()}_{operation['blockNumber']}"
#                 },
#                 limit=1,
#                 projection={"id": 1},
#             ):
#                 # fire scrape event on block regarding hypervisor and rewarders snapshots (status) and token prices
#                 # build queue events from operation
#                 build_and_save_queue_from_operation(
#                     operation=operation, network=network
#                 )
#             else:
#                 logging.getLogger(__name__).debug(
#                     f"  Not pushing {operation['address']} hypervisor status queue item bcause its already in the database"
#                 )