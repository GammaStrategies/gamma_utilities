# one time utils
import logging
import concurrent.futures
import tqdm

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_block
from bins.database.common.db_collections_common import database_global, database_local


def repair_blocks():
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            repair_missing_blocks(protocol=protocol, network=network)


def repair_missing_blocks(protocol: str, network: str, batch_size: int = 100000):
    logging.getLogger(__name__).info(f">Repair blocks for {network} {protocol}...")

    # get a list of blocks from global database
    database_blocks = [
        x["block"]
        for x in database_global(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        ).get_items_from_database(
            collection_name="blocks",
            find={"network": network},
            projection={"block": 1},
            batch_size=batch_size,
        )
    ]

    # get a list of status blocks from local database
    todo_blocks = {
        x["block"]: {
            "id": create_id_block(network=network, block=x["block"]),
            "network": network,
            "block": x["block"],
            "timestamp": x["timestamp"],
        }
        for x in database_local(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
            db_name=f"{network}_{protocol}",
        ).get_items_from_database(
            collection_name="status",
            find={"block": {"$nin": database_blocks}},
            projection={"block": 1, "timestamp": 1},
            batch_size=batch_size,
        )
    }

    # get a list of status rewards from local database
    todo_blocks.update(
        {
            x["block"]: {
                "id": create_id_block(network=network, block=x["block"]),
                "network": network,
                "block": x["block"],
                "timestamp": x["timestamp"],
            }
            for x in database_local(
                mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
                db_name=f"{network}_{protocol}",
            ).get_items_from_database(
                collection_name="rewards_status",
                find={"block": {"$nin": database_blocks}},
                projection={"block": 1, "timestamp": 1},
                batch_size=batch_size,
            )
        }
    )

    if todo_blocks:
        logging.getLogger(__name__).info(
            f" Found {len(todo_blocks)} missing blocks in {network}. Adding to global database..."
        )
        # add missing blocks to global database
        database_global(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        ).replace_items_to_database(data=todo_blocks.values(), collection_name="blocks")
    else:
        logging.getLogger(__name__).info(f" No missing blocks found in {network}.")


def replace_blocks_to_int():
    logging.getLogger(__name__).debug("    Converting non int blocks to int")

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # db_name = f"{network}_{protocol}"
    # local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # get all prices
    all_prices = global_db_manager.get_items_from_database(
        collection_name="usd_prices", find={"block": {"$not": {"$type": "int"}}}
    )
    _errors = 0
    with tqdm.tqdm(total=len(all_prices)) as progress_bar:

        def loopme(price):
            global_db_manager.set_price_usd(
                network=price["network"],
                block=price["block"],
                token_address=price["address"],
                price_usd=price["price"],
                source=price["source"],
            )
            return price

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for price in ex.map(loopme, all_prices):
                progress_bar.set_description(
                    f"Updating database {price['network']}'s block {price['block']}"
                )
                # update progress
                progress_bar.update(1)
