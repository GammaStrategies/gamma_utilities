import logging
import random
import tqdm
from apps.database_reScrape import manual_reScrape, reScrape_loopWork_hypervisor_status
from apps.feeds.queue.queue_item import QueueItem
from apps.repair.prices.logs import get_all_logfiles, get_failed_prices_from_log
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.general.enums import queueItemType, text_to_chain
from bins.general.general_utilities import differences
from bins.w3.builders import (
    build_db_hypervisor,
    build_hypervisor,
    check_erc20_fields,
    convert_dex_protocol,
)


def repair_hypervisor_status():
    # from user_status debug log
    repair_hype_status_from_user()

    # missing hypes
    repair_missing_hype_status()

    # humongous values, missing fiels, wrong types ( TODO: add more)
    repar_multiple_wrongs()

    # binance
    repair_binance_hypervisor_status()
    repair_binance_queue_hype_status()


def repair_hype_status_from_user(min_count: int = 1):
    protocol = "gamma"

    network_token_blocks = {}
    for log_file in get_all_logfiles():
        network_token_blocks.update(get_failed_prices_from_log(log_file=log_file))

    # for log_file in get_all_logfiles():
    # hypervisor status not found while scrpaing user data
    # network_token_blocks = get_failed_status_from_log(log_file)

    try:
        with tqdm.tqdm(total=len(network_token_blocks)) as progress_bar:
            for network, addresses in network_token_blocks.items():
                # set local database name and create manager
                db_name = f"{network}_{protocol}"
                local_db = database_local(
                    mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
                    db_name=db_name,
                )
                logging.getLogger(__name__).info(
                    f" > Trying to repair {len(addresses)} hypervisors status from {network}"
                )

                for address, blocks_data in addresses.items():
                    for block, counter in blocks_data.items():
                        # block is string
                        block = int(block)

                        # make sure hypervisor status is not in db
                        if local_db.get_items(
                            collection_name="status",
                            find={"address": address.lower(), "block": block},
                            projection={"dex": 1},
                        ):
                            logging.getLogger(__name__).debug(
                                f" Status for {network}'s {address} at block {block} is already in database..."
                            )
                            continue

                        progress_bar.set_description(
                            f" Repair {network}'s hype status not found log entries for {address} at block {block}"
                        )
                        progress_bar.update(0)

                        # counter = number of times found in logs
                        if counter >= min_count:
                            # need dex to be able to build hype

                            if dex := local_db.get_items(
                                collection_name="static",
                                find={"address": address.lower()},
                                projection={"dex": 1},
                            ):
                                dex = dex[0]["dex"]
                            else:
                                logging.getLogger(__name__).error(
                                    f"{protocol}'s {network} hyperivisor {address} not fount in static db collection. May not be present in registry. (cant solve err.)"
                                )
                                # loop to next address
                                continue

                            # scrape hypervisor status at block
                            hype_status = build_db_hypervisor(
                                address=address,
                                network=network,
                                block=block,
                                dex=dex,
                                cached=True,
                            )
                            if hype_status:
                                # add hypervisor status to database
                                local_db.set_status(data=hype_status)

                                logging.getLogger(__name__).info(
                                    f" Added status for {network}'s {address} at block {block}  (found {counter} times in log)"
                                )
                            else:
                                logging.getLogger(__name__).debug(
                                    f" Could not find status for {network}'s {address} at block {block}  (found {counter} times in log)"
                                )
                        else:
                            logging.getLogger(__name__).debug(
                                f" Not procesing status for {network}'s {address} at block {block} bc it has been found only {counter} times in log."
                            )

                # update progress
                progress_bar.update(1)
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Error repairing hypervisor status not found {e}"
        )


def repair_missing_hype_status():
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            repair_missing_hypervisor_status(
                protocol=protocol,
                network=network,
                max_repair=CONFIGURATION["_custom_"]["cml_parameters"].maximum,
            )


def repair_missing_hypervisor_status(
    protocol: str, network: str, cache: bool = True, max_repair: int = None
):
    """Creates hypervisor status at all operations block and block-1 not already present in database,
        using the difference between operations and status blocks
        Important -> transfer operations will not be accounted for repair.

    Args:
        protocol (str):
        network (str):
        rewrite (bool): rewrite all status
        threaded: (bool):
    """
    batch_size = 100000
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors status information using the difference between operations and status blocks"
    )
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    # loop thru all hypervisors in database
    for hype in tqdm.tqdm(
        database_local(mongo_url=mongo_url, db_name=db_name).get_items_from_database(
            collection_name="static", find={}
        )
    ):
        # get all status blocks
        hype_status_blocks = [
            x["block"]
            for x in database_local(
                mongo_url=mongo_url, db_name=db_name
            ).get_items_from_database(
                collection_name="status",
                find={"address": hype["address"]},
                projection={"block": 1},
                batch_size=batch_size,
            )
        ]

        # get all operations blocks with the topic=["deposit", "withdraw", "zeroBurn", "rebalance"]
        operation_blocks = []

        # add all topic blocks and block-1
        for operation in database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="operations",
            find={
                "address": hype["address"],
                "topic": {"$in": ["deposit", "withdraw", "zeroBurn", "rebalance"]},
            },
            batch_size=batch_size,
            sort=[("blockNumber", 1)],
        ):
            operation_blocks.append(int(operation["blockNumber"]))
            operation_blocks.append(int(operation["blockNumber"]) - 1)

        # add transfer opertçation blocks
        for operation in database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="operations",
            find={
                "address": hype["address"],
                "topic": {"$in": ["transfer"]},
            },
            batch_size=batch_size,
            sort=[("blockNumber", 1)],
        ):
            operation_blocks.append(int(operation["blockNumber"]))

        # get differences
        if difference_blocks := differences(operation_blocks, hype_status_blocks):
            logging.getLogger(__name__).info(
                f" Found {len(difference_blocks)} missing status blocks for {network}'s {hype['address']}"
            )
            if max_repair and len(difference_blocks) > max_repair:
                logging.getLogger(__name__).info(
                    f"  Selecting a random sample of {max_repair} hypervisor status missing due to max_repair limit set."
                )
                difference_blocks = random.sample(difference_blocks, max_repair)

            # define added counter
            total_added_counter = 0

            for block in difference_blocks:
                # create queue item
                queue_item = QueueItem(
                    type=queueItemType.HYPERVISOR_STATUS,
                    block=block,
                    address=operation["address"],
                    data=operation,
                )

                # check if id is already in queue
                if not database_local(
                    mongo_url=mongo_url, db_name=db_name
                ).get_items_from_database(
                    collection_name="queue", find={"id": queue_item.id}
                ):
                    # insert
                    database_local(
                        mongo_url=mongo_url, db_name=db_name
                    ).insert_if_not_exists(
                        data=queue_item.as_dict,
                        collection_name="queue",
                    )
                    # add counter
                    total_added_counter += 1

            # add hype status queueItems to the database queue collection to be processed
            if total_added_counter:
                logging.getLogger(__name__).info(
                    f"  Added {total_added_counter} blocks for {network}'s {hype['address']} to the queue"
                )
            else:
                logging.getLogger(__name__).info(
                    f"  No blocks for {network}'s {hype['address']} have been added to the queue as they were already present"
                )
        else:
            logging.getLogger(__name__).debug(
                f" No missing status blocks found for {network}'s {hype['address']}"
            )


def repar_multiple_wrongs():
    """humongous values, missing fields  ( wrong types are disabled for now)"""
    chains = [
        text_to_chain(x)
        for x in (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
        )
    ]

    # build humongous values query
    query_humongous = [
        {
            "$addFields": {
                "_data": {
                    "uncollected_qtty_token0": {
                        "$divide": [
                            {"$toDecimal": "$fees_uncollected.qtty_token0"},
                            {"$pow": [10, "$pool.token0.decimals"]},
                        ]
                    },
                    "uncollected_qtty_token1": {
                        "$divide": [
                            {"$toDecimal": "$fees_uncollected.qtty_token1"},
                            {"$pow": [10, "$pool.token1.decimals"]},
                        ]
                    },
                    "qtty_tvl_token0": {
                        "$divide": [
                            {"$toDecimal": "$tvl.tvl_token0"},
                            {"$pow": [10, "$pool.token0.decimals"]},
                        ]
                    },
                    "qtty_tvl_token1": {
                        "$divide": [
                            {"$toDecimal": "$tvl.tvl_token1"},
                            {"$pow": [10, "$pool.token1.decimals"]},
                        ]
                    },
                }
            }
        },
        {
            "$match": {
                "$or": [
                    {"_data.uncollected_qtty_token0": {"$gte": 10000000}},
                    {"_data.uncollected_qtty_token1": {"$gte": 10000000}},
                    {"_data.qtty_tvl_token0": {"$gte": 100000000000}},
                    {"_data.qtty_tvl_token1": {"$gte": 100000000000}},
                ]
            }
        },
    ]
    # build missing fields query
    missing_fields = [
        "basePosition_data",
        "limitPosition_data",
        "basePosition_ticksLower",
        "limitPosition_ticksLower",
        "basePosition_ticksUpper",
        "limitPosition_ticksUpper",
        "fees_uncollected.gamma_qtty_token0",
        "fees_collected",
    ]
    query_missing_fields = [
        {"$match": {"$or": [{x: {"$exists": False}} for x in missing_fields]}},
    ]
    # build wrong types query
    wrong_types = [
        {"$match": {"pool.globalState": {"$exists": True}}},
        {"$addFields": {"deleteme": {"$type": "pool.globalState.unlocked"}}},
        {"$match": {"$or": [{"deleteme": {"$ne": {"$type": "bool"}}}]}},
        {"$unset": "deleteme"},
    ]

    for chain in chains:
        names = ["humongous values", "missing fields", "wrong types"]
        for idx, query in enumerate(
            [query_humongous, query_missing_fields]  # , wrong_types]
        ):
            logging.getLogger(__name__).info(
                f" {chain.database_name} repairing {names[idx]}"
            )
            manual_reScrape(
                chain=chain,
                loop_work=reScrape_loopWork_hypervisor_status,
                aggregate=query,
                sort=[("timestamp", 1)],
                db_collection="status",
                rewrite=True,
            )


def repair_binance_hypervisor_status():
    batch_size = 100000
    network = "binance"
    dex = "thena"
    logging.getLogger(__name__).info(f">Repairing {network} hypervisors status ")
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"

    wrong_values = [None, "none", "None", "null", "Null", "NaN", "nan"]

    # get all wrong erc20 values from status
    for hype_status in tqdm.tqdm(
        database_local(mongo_url=mongo_url, db_name=db_name).get_items_from_database(
            collection_name="status",
            find={
                "$or": [
                    {"decimals": {"$in": wrong_values}},
                    {"totalSupply": {"$in": wrong_values}},
                    {"symbol": {"$in": wrong_values}},
                    {"pool.token0.symbol": {"$in": wrong_values}},
                    {"pool.token1.symbol": {"$in": wrong_values}},
                    {"pool.token0.decimals": {"$in": wrong_values}},
                    {"pool.token1.decimals": {"$in": wrong_values}},
                    {"baseLower": {"$in": wrong_values}},
                    {"baseUpper": {"$in": wrong_values}},
                    {"limitUpper": {"$in": wrong_values}},
                    {"limitLower": {"$in": wrong_values}},
                    {"totalAmounts.total0": {"$in": wrong_values}},
                    {"totalAmounts.total1": {"$in": wrong_values}},
                ],
            },
        )
    ):
        # build hypervisor
        hypervisor = build_hypervisor(
            network=network,
            protocol=convert_dex_protocol(dex),
            block=hype_status["block"],
            hypervisor_address=hype_status["address"],
            cached=True,
        )
        # check fields
        if check_erc20_fields(
            hypervisor=hypervisor,
            hype=hype_status,
            convert_bint=True,
            wrong_values=wrong_values,
        ):
            # save it to database
            if save_result := database_local(
                mongo_url=mongo_url, db_name=db_name
            ).set_status(data=hype_status):
                logging.getLogger(__name__).debug(
                    f" Database modified: {save_result.modified_count} "
                )


def repair_binance_queue_hype_status():
    batch_size = 100000
    network = "binance"
    dex = "thena"
    logging.getLogger(__name__).info(f">Repairing {network} queue (hype status) ")
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"

    # get all wrong erc20 values from status
    wrong_values = [None, "none", "None", "null", "Null", "NaN", "nan"]
    for queue_item in tqdm.tqdm(
        database_local(mongo_url=mongo_url, db_name=db_name).get_items_from_database(
            collection_name="queue",
            find={
                "data.hypervisor_status": {"$exists": True},
                "$or": [
                    {"data.hypervisor_status.decimals": {"$in": wrong_values}},
                    {"data.hypervisor_status.totalSupply": {"$in": wrong_values}},
                    {"data.hypervisor_status.symbol": {"$in": wrong_values}},
                    {
                        "data.hypervisor_status.pool.token0.symbol": {
                            "$in": wrong_values
                        }
                    },
                    {
                        "data.hypervisor_status.pool.token1.symbol": {
                            "$in": wrong_values
                        }
                    },
                    {
                        "data.hypervisor_status.pool.token0.decimals": {
                            "$in": wrong_values
                        }
                    },
                    {
                        "data.hypervisor_status.pool.token1.decimals": {
                            "$in": wrong_values
                        }
                    },
                ],
            },
        )
    ):
        # build hypervisor
        hypervisor = build_hypervisor(
            network=network,
            protocol=convert_dex_protocol(dex),
            block=queue_item["data"]["hypervisor_status"]["block"],
            hypervisor_address=queue_item["data"]["hypervisor_status"]["address"],
            cached=True,
        )
        # check fields
        if check_erc20_fields(
            hypervisor=hypervisor,
            hype=queue_item["data"]["hypervisor_status"],
            convert_bint=True,
            wrong_values=wrong_values,
        ):
            # reset count
            queue_item["count"] = 0

            # save it to database
            if save_result := database_local(
                mongo_url=mongo_url, db_name=db_name
            ).set_queue_item(data=queue_item):
                logging.getLogger(__name__).debug(f" {save_result['raw_result']} ")
