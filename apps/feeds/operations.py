import logging
from multiprocessing import Pool
import threading
import tqdm
from datetime import datetime

from web3 import Web3
from apps.feeds.queue.push import build_and_save_queue_from_operation
from bins.config.hardcodes import HYPERVISOR_NO_OPERATIONS_BEFORE
from bins.database.helpers import get_default_localdb, get_from_localdb

from bins.general.enums import Chain, Protocol, queueItemType, text_to_chain
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.gamma.collectors import (
    create_data_collector,
)
from bins.w3.protocols.ramses.collectors import (
    create_multiFeeDistribution_data_collector,
)

from .queue.queue_item import QueueItem

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
            if date_end == "now" and not block_end:
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

            # force initial block mannualy if set so.
            if HYPERVISOR_NO_OPERATIONS_BEFORE.get(text_to_chain(network), None):
                block_ini = HYPERVISOR_NO_OPERATIONS_BEFORE[network]

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
    hypervisor_addresses: list,
    block_ini: int,
    block_end: int,
    max_blocks_step: int = 1000,
):
    """Scrape and process logs from the hypervisors and save em to the database

    Args:
        network (str): Network to scrape
        hypervisor_addresses (list): Addresses of the hypervisors to scrape
        block_ini (int): Initial block to scrape
        block_end (int): End block to scrape
        max_blocks_step (int, optional): Maximum blocks to scrape at once in one query (seee operations_generator) Careful bc some RPCs do not like respond well to high values. Defaults to 1000.
    """

    # set global protocol helper
    data_collector = create_data_collector(network=network)

    logging.getLogger(__name__).info(
        "   Feeding database {}'s operations of {} hypervisors from blocks {} to {}".format(
            network,
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
            max_blocks=max_blocks_step,
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
        block_end = build_erc20_helper(chain)._getBlockData("latest").number

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

        # control var
        items_to_queue = {}

        for operations in data_collector.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            contracts=[Web3.toChecksumAddress(x) for x in addresses],
            max_blocks=5000,
        ):
            # process operation

            for operation in operations:
                if operation["address"].lower() not in items_to_queue:
                    items_to_queue[operation["address"].lower()] = operation

                # select the latest block number
                if (
                    items_to_queue[operation["address"].lower()]["blockNumber"]
                    < operation["blockNumber"]
                ):
                    items_to_queue[operation["address"].lower()] = operation

    logging.getLogger(__name__).info(
        f" Adding {len(items_to_queue)} mfd operations items to {chain.database_name} queue"
    )
    # add to queue
    task_enqueue_operations(
        operations=list(items_to_queue.values()),
        network=chain.database_name,
        operation_type=queueItemType.LATEST_MULTIFEEDISTRIBUTION,
    )


def get_latest_multifeedistribution_last_blocks(network: str) -> int:
    """Get the last block from latest_multifeedistribution collection"""

    try:
        # get it from the collection
        if max_block := get_from_localdb(
            network=network,
            collection="latest_multifeedistribution",
            aggregate=[
                {
                    "$group": {
                        "_id": "none",
                        "block": {"$max": "$last_updated_data.block"},
                    }
                }
            ],
        ):
            max_block = max_block[0]["block"]
        else:
            logging.getLogger(__name__).debug(
                f" there are no latest_multifeedistribution in db for {network} choose last block from"
            )
            max_block = 0
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while quering db for latest_multifeedistribution block  error:{e}"
        )
        max_block = 0

    # get it from the queue
    if max_queue_block := get_from_localdb(
        network=network,
        collection="queue",
        aggregate=[
            {
                "$match": {
                    "type": queueItemType.LATEST_MULTIFEEDISTRIBUTION,
                    "data.is_last_item": True,
                }
            },
            {"$group": {"_id": "none", "block": {"$max": "$block"}}},
        ],
    ):
        if not max_block is None and not max_queue_block[0]["block"] is None:
            max_block = max(max_block, max_queue_block[0]["block"])

    return max_block
