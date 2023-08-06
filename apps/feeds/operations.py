import logging
from multiprocessing import Pool
import threading
import tqdm
from datetime import datetime

from web3 import Web3
from bins.database.helpers import get_default_localdb, get_from_localdb

from bins.general.enums import Chain, Protocol, queueItemType
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.gamma.collectors import (
    create_data_collector_alternative,
    create_data_collector,
)
from bins.w3.protocols.ramses.collectors import (
    create_multiFeeDistribution_data_collector,
)
from .queue import QueueItem, build_and_save_queue_from_operation

# from croniter import croniter

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import (
    create_id_hypervisor_status,
    create_id_operation,
)
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
):
    # set global protocol helper
    data_collector = create_data_collector(network=network)

    logging.getLogger(__name__).info(
        "   Feeding database with {}'s {} operations of {} hypervisors from blocks {} to {}".format(
            network,
            protocol,
            len(hypervisor_addresses),
            block_ini,
            block_end,
        )
    )

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

        # set progress callback to data collector
        data_collector.progress_callback = _update_progress

        for operations in data_collector.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            contracts=[Web3.toChecksumAddress(x) for x in hypervisor_addresses],
            max_blocks=1000,
        ):
            # process operation
            task_enqueue_operations(
                operations=operations,
                network=network,
                operation_type=queueItemType.OPERATION,
            )


def task_enqueue_operations(
    operations: list[dict], network: str, operation_type: queueItemType
):
    # build a list of operations to be added to the queue
    to_add = [
        QueueItem(
            type=operation_type,
            block=int(operation["blockNumber"]),
            address=operation["address"].lower(),
            data=operation,
        ).as_dict
        for operation in operations
    ]

    if db_return := get_default_localdb(network=network).replace_items_to_database(
        data=to_add, collection_name="queue"
    ):
        logging.getLogger(__name__).debug(
            f"     db return-> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
        )
    else:
        logging.getLogger(__name__).error(
            f"  database did not return anything while saving {operation_type}s to queue"
        )


def get_db_last_operation_block(protocol: str, network: str) -> int:
    """Get the last operation block from database
        using operations collection and queue collection

    Args:
        protocol (str):
        network (str):

    Returns:
        int: last block number or None if not found or error
    """
    # read last blocks from database
    try:
        # setup database manager
        local_db_manager = get_default_localdb(network=network)
        batch_size = 100000

        max_block = 0
        if max_operations_block := local_db_manager.get_items_from_database(
            collection_name="operations",
            aggregate=[
                {"$group": {"_id": "none", "max_block": {"$max": "$blockNumber"}}},
            ],
            batch_size=batch_size,
        ):
            max_block = max(max_block, max_operations_block[0]["max_block"])

        if max_queue_block := local_db_manager.get_items_from_database(
            collection_name="queue",
            aggregate=[
                {"$match": {"type": "operation"}},
                {"$group": {"_id": "none", "max_block": {"$max": "$block"}}},
            ],
            batch_size=batch_size,
        ):
            max_block = max(max_block, max_queue_block[0]["max_block"])

        return max_block

    except IndexError:
        logging.getLogger(__name__).debug(
            f" Unable to get last operation block bc no operations have been found for {network}'s {protocol} in db"
        )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while quering db operations for latest block  error:{e}"
        )

    return None


def feed_mutiFeeDistribution_operations(
    chain: Chain,
    addresses: list[str] | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):
    block_ini_static = 99999999999999999999999999999999999999999999999

    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name} multiFeeDistributor contracts operations information"
    )

    if not addresses:
        addresses = []

        # TODO: solve the 'rewarder_type' manual reference to ramses_v2
        #
        # get addresses to scrape and its minimum block
        for reward in get_from_localdb(
            network=chain.database_name,
            collection="rewards_static",
            find={"rewarder_type": "ramses_v2"},
        ):
            block_ini_static = min([reward["block"], block_ini_static])
            if reward["rewarder_registry"] not in addresses:
                addresses.append(reward["rewarder_registry"])

    if not block_ini:
        # get last block from database
        block_ini = (
            get_latest_multifeedistribution_last_blocks(network=chain.database_name)
            or block_ini_static
        )

    if not block_end:
        # get last block from database
        block_end = (
            build_erc20_helper(chain.database_name)._getBlockData("latest").number
        )

    # proceed to feed
    feed_queue_with_multiFeeDistribution_operations(
        chain=chain,
        addresses=addresses,
        block_ini=block_ini,
        block_end=block_end,
    )


def feed_queue_with_multiFeeDistribution_operations(
    chain: Chain,
    addresses: list[str],
    block_ini: int,
    block_end: int,
):
    """Scrape and process logs from the multiFee distribution contracts ( rewards for liquidity providers when staking )

    Args:
        chain (Chain):
        protocol (Protocol):
        addresses (list[str]): multiFee distribution contract addresses
        block_ini (int ):
        block_end (int ):
    """
    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name}' {len(addresses)} multiFeeDistribution contract addresses operations from block {block_ini} to {block_end}"
    )

    # create collector
    data_collector = create_multiFeeDistribution_data_collector(
        network=chain.database_name
    )

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

        # set progress callback to data collector
        data_collector.progress_callback = _update_progress

        for operations in data_collector.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            contracts=[Web3.toChecksumAddress(x) for x in addresses],
            max_blocks=5000,
        ):
            # process operation
            task_enqueue_operations(
                operations=operations,
                network=chain.database_name,
                operation_type=queueItemType.LATEST_MULTIFEEDISTRIBUTION,
            )


def get_latest_multifeedistribution_last_blocks(network: str) -> int:
    """Get the last block from latest_multifeedistribution collection"""

    # get it from the collection
    if max_block := get_from_localdb(
        network=network,
        collection="latest_multifeedistribution",
        query=[
            {"$group": {"_id": "none", "block": {"$max": "$last_updated_data.block"}}}
        ],
    ):
        max_block = max_block[0]["block"]
    else:
        logging.getLogger(__name__).debug(
            f" there are no latest_multifeedistribution in db for {network} choose last block from"
        )
        max_block = 0

    # get it from the queue
    if max_queue_block := get_from_localdb(
        network=network,
        collection="queue",
        query=[
            {
                "$match": {
                    "type": queueItemType.LATEST_MULTIFEEDISTRIBUTION,
                    "data.is_last_item": True,
                }
            },
            {"$group": {"_id": "none", "block": {"$max": "$block"}}},
        ],
    ):
        max_block = max(max_block, max_queue_block[0]["block"])

    return max_block


################################################################################################
# TO REMOVE - DEPRECATED -< ( check all dependencies)


def feed_operations_hypervisors_classic(
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

        for operation in onchain_helper.operations_generator(
            addresses=hypervisor_addresses,
            network=network,
            block_ini=block_ini,
            block_end=block_end,
            progress_callback=_update_progress,
            max_blocks=1000,
        ):
            # process operation
            task_process_operation(
                operation=operation, local_db=local_db, network=network
            )


# TODO: hangs on some cases
def feed_operations_hypervisors_task1(
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
    running_tasks = []
    queued_tasks = []
    max_paralel_tasks = 5

    # control var
    _waiting = False
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
                # exit
                if len(queued_tasks) + len(running_tasks) == 0:
                    break

                # remove tasks from running
                for task in running_tasks[:]:
                    if task.ready():
                        running_tasks.remove(task)
                        if _waiting:
                            # update progress
                            _update_progress(text=" processing queued items")

                # add tasks from queue
                if len(queued_tasks):
                    # define how many tasks can be added to be ran
                    toadd_tasks = max_paralel_tasks - len(running_tasks)
                    logging.getLogger(__name__).debug(
                        f"  Adding {toadd_tasks} operation tasks to the processing pool "
                    )
                    if toadd_tasks > 0:
                        for i in range(toadd_tasks):
                            running_tasks.append(
                                p.apply_async(
                                    task_process_operation,
                                    queued_tasks.pop(0),
                                )
                            )

            return

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
                # add parameters to taks
                queued_tasks.append((operation, local_db, network))

            # continue progress if there are tasks to complete
            if len(running_tasks):
                logging.getLogger(__name__).info(
                    f"  Finished finding {network} operations, waiting for {len(running_tasks)} tasks to finish..."
                )
                # log

                _update_progress(
                    text=" processing queued items",
                    total=len(running_tasks) + len(queued_tasks),
                    remaining=len(running_tasks) + len(queued_tasks),
                )

            # join thread to close
            task_thread.join()


def feed_operations_hypervisors_task2(
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
    running_tasks = []
    queued_tasks = []
    max_paralel_tasks = 5

    # control var
    _waiting = False
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
                # remove tasks from running
                for task in running_tasks[:]:
                    if task.ready():
                        running_tasks.remove(task)
                        if _waiting:
                            # update progress
                            _update_progress(text=" processing queued items")

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
                # add parameters to taks
                queued_tasks.append((operation, local_db, network))

                # define how many tasks can be added to be ran
                toadd_tasks = max_paralel_tasks - len(running_tasks)

                if toadd_tasks > 0 and len(queued_tasks) > 0:  # add tasks from queue
                    logging.getLogger(__name__).debug(
                        f"  Adding {toadd_tasks} operation tasks to the processing pool "
                    )
                    for i in range(toadd_tasks):
                        try:
                            running_tasks.append(
                                p.apply_async(
                                    task_process_operation,
                                    queued_tasks.pop(0),
                                )
                            )
                        except Exception as e:
                            # manage this in a better way
                            pass

            # update progress
            _update_progress(
                text=" processing queued items",
                total=len(running_tasks) + len(queued_tasks),
                remaining=len(running_tasks) + len(queued_tasks),
            )

            logging.getLogger(__name__).info(
                f"  Finished finding {network} operations, waiting for {len(running_tasks)+len(queued_tasks)} tasks to finish..."
            )
            _waiting = True
            # continue progress if there are tasks to complete
            while len(running_tasks) + len(queued_tasks) > 0:
                # define how many tasks can be added to be ran
                toadd_tasks = max_paralel_tasks - len(running_tasks)
                logging.getLogger(__name__).debug(
                    f"  Adding {toadd_tasks} operation tasks to the processing pool "
                )
                for i in range(toadd_tasks):
                    try:
                        running_tasks.append(
                            p.apply_async(
                                task_process_operation,
                                queued_tasks.pop(0),
                            )
                        )
                    except Exception as e:
                        # manage this in a better way
                        pass
            # join thread to close
            # task_thread.join()


def feed_operations_hypervisors_taskedQueue(
    network: str,
    protocol: str,
    hypervisor_addresses: list,
    block_ini: int,
    block_end: int,
    local_db: database_local,
):
    # split addresses in chunks of 50
    hypervisor_addresses_chunks = [
        hypervisor_addresses[i : i + 50]
        for i in range(0, len(hypervisor_addresses), 50)
    ]
    logging.getLogger(__name__).debug(
        f" Spliting the {network}'s {len(hypervisor_addresses)} total hypervisor addresses in {len(hypervisor_addresses_chunks)} chunks"
    )
    for hype_addresses in hypervisor_addresses_chunks:
        # set global protocol helper
        data_collector = create_data_collector_alternative(network=network)

        logging.getLogger(__name__).info(
            "   Feeding database with {}'s {} operations of {}/{} hypervisors from blocks {} to {}".format(
                network,
                protocol,
                len(hype_addresses),
                len(hypervisor_addresses),
                block_ini,
                block_end,
            )
        )

        # create a task pool to handle operations
        running_tasks = []
        queued_tasks = []
        max_paralel_tasks = 6

        # control var
        _waiting = False
        with tqdm.tqdm(total=block_end - block_ini) as progress_bar:
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

            # set progress callback to data collector
            data_collector.progress_callback = _update_progress

            # build task handler
            def task_handler():
                while True:
                    # remove tasks from running
                    for task in running_tasks[:]:
                        if task.ready():
                            try:
                                running_tasks.remove(task)
                            except Exception as e:
                                logging.getLogger(__name__).error(
                                    f"  Error removing task from running tasks: {e}"
                                )
                            if _waiting:
                                # update progress
                                _update_progress(text=" processing queued items")

            # start task handler
            task_thread = threading.Thread(target=task_handler)
            task_thread.start()

            # inside a pool
            with Pool() as p:
                # loop thru content
                for operation in data_collector.operations_generator(
                    block_ini=block_ini,
                    block_end=block_end,
                    contracts=[Web3.toChecksumAddress(x) for x in hype_addresses],
                    max_blocks=1000,
                ):
                    if isinstance(operation, list):
                        # add parameters to taks
                        for op in operation:
                            queued_tasks.append((op, local_db, network))
                    else:
                        pass
                        # add parameters to taks
                        queued_tasks.append((operation, local_db, network))

                    # define how many tasks can be added to be ran
                    toadd_tasks = max_paralel_tasks - len(running_tasks)
                    added_tasks = 0

                    if (
                        toadd_tasks > 0 and len(queued_tasks) > 0
                    ):  # add tasks from queue
                        for i in range(toadd_tasks):
                            try:
                                running_tasks.append(
                                    p.apply_async(
                                        task_enqueue_operation,
                                        queued_tasks.pop(0),
                                    )
                                )
                                added_tasks += 1
                            except Exception as e:
                                # manage this in a better way
                                pass
                            logging.getLogger(__name__).debug(
                                f" added {added_tasks} queued tasks"
                            )

                # update progress
                _update_progress(
                    text=" processing queued items",
                    total=len(running_tasks) + len(queued_tasks),
                    remaining=len(running_tasks) + len(queued_tasks),
                )

                if len(running_tasks) + len(queued_tasks) > 0:
                    logging.getLogger(__name__).info(
                        f"  Finished finding {network} operations, waiting for {len(running_tasks)+len(queued_tasks)} tasks to finish..."
                    )

                _waiting = True
                # continue progress if there are tasks to complete
                while len(running_tasks) + len(queued_tasks) > 0:
                    # define how many tasks can be added to be ran
                    toadd_tasks = max_paralel_tasks - len(running_tasks)
                    added_tasks = 0
                    for i in range(toadd_tasks):
                        try:
                            running_tasks.append(
                                p.apply_async(
                                    task_enqueue_operation,
                                    queued_tasks.pop(0),
                                )
                            )
                            added_tasks += 1
                        except Exception as e:
                            # manage this in a better way
                            pass


def task_process_operation(operation: dict, local_db: database_local, network: str):
    # set operation id (same hash has multiple operations)
    operation["id"] = create_id_operation(
        logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
    )

    # log
    logging.getLogger(__name__).debug(
        f"  -> Processing {network}'s operation {operation['id']}"
    )

    # lower case address ( to ease comparison )
    operation["address"] = operation["address"].lower()
    # save operation to database
    if db_return := local_db.set_operation(data=operation):
        logging.getLogger(__name__).debug(f" Saved operation {operation['id']}")

    # make sure hype is not in status collection already
    if not local_db.get_items_from_database(
        collection_name="status",
        find={
            "id": create_id_hypervisor_status(
                hypervisor_address=operation["address"], block=operation["blockNumber"]
            )
        },
        projection={"id": 1},
    ):
        # fire scrape event on block regarding hypervisor and rewarders snapshots (status) and token prices
        # build queue events from operation
        build_and_save_queue_from_operation(operation=operation, network=network)
    else:
        logging.getLogger(__name__).debug(
            f"  Not pushing {operation['address']} hypervisor status queue item bcause its already in the database"
        )

    # log
    logging.getLogger(__name__).debug(
        f"  <- Done processing {network}'s operation {operation['id']}"
    )


def task_enqueue_operation(operation: dict, local_db: database_local, network: str):
    # lower op address just in case ..
    operation["address"] = operation["address"].lower()

    queueItem = QueueItem(
        type=queueItemType.OPERATION,
        block=int(operation["blockNumber"]),
        address=operation["address"].lower(),
        data=operation,
    )
    # item should not be already in the queue
    if not local_db.get_items_from_database(
        collection_name="queue", find={"id": queueItem.id}
    ):
        db_return = local_db.set_queue_item(data=queueItem.as_dict)
        logging.getLogger(__name__).debug(
            f" operation queue item {queueItem.id} added to the queue-> db: {db_return.raw_result}"
        )
    else:
        logging.getLogger(__name__).debug(
            f"  operation queue item {queueItem.id} already in queue"
        )


############################################################################################################
