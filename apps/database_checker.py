import os
import random
import logging
import time
import tqdm
import concurrent.futures
import contextlib
import re
from apps.feeds.operations import feed_operations

from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)

from apps.feeds.queue.queue_item import QueueItem
from apps.feeds.queue.pull import process_queue_item_type
from apps.feeds.queue.push import build_and_save_queue_from_hypervisor_status

from bins.configuration import CONFIGURATION, TOKEN_ADDRESS_EXCLUDE
from bins.database.common.database_ids import create_id_block, create_id_price
from bins.general.enums import (
    Chain,
    Protocol,
    databaseSource,
    queueItemType,
    text_to_chain,
)
from bins.general.general_utilities import differences

from bins.w3.protocols.general import erc20_cached

from bins.database.common.db_collections_common import database_local, database_global
from bins.mixed.price_utilities import price_scraper

from bins.w3.builders import (
    build_db_hypervisor,
    build_hypervisor,
    check_erc20_fields,
    convert_dex_protocol,
)

from apps.feeds.prices import (
    create_tokenBlocks_all,
    feed_prices,
)


# repair apps
def repair_all():
    """Repair all errors found in logs"""

    # repair queue
    repair_queue_locked_items()

    # repair blocks
    repair_blocks()

    # repair hypervisors status
    repair_hypervisor_status()

    # repair prices not found in logs
    repair_prices()

    # repair missing rewards status
    # TODO: this is too time intensive right now. Need to find a better way to do it
    # repair_rewards_status()


def repair_prices(min_count: int = 1):
    repair_prices_from_logs(min_count=min_count, add_to_queue=True)

    repair_prices_from_status(
        max_repair_per_network=CONFIGURATION["_custom_"]["cml_parameters"].maximum
        or 500
    )

    repair_prices_from_database(
        max_repair_per_network=CONFIGURATION["_custom_"]["cml_parameters"].maximum or 50
    )


def repair_prices_from_logs(min_count: int = 1, add_to_queue: bool = False):
    """Check price errors from debug and price logs and try to scrape again"""

    logging.getLogger(__name__).info(
        f">Check all errors found in debug and price logs and try to scrape 'em again"
    )
    try:
        network_token_blocks = {}
        for log_file in get_all_logfiles():
            network_token_blocks.update(get_failed_prices_from_log(log_file=log_file))

        # setup database managers
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        global_db_manager = database_global(mongo_url=mongo_url)
        # create ids to query database with
        price_ids_to_search = [
            create_id_price(network, block, address)
            for network, addresses in network_token_blocks.items()
            for address, blocks_data in addresses.items()
            for block, counter in blocks_data.items()
        ]
        # get those same prices from database
        price_ids_in_database = global_db_manager.get_items_from_database(
            collection_name="usd_prices",
            find={"id": {"$in": price_ids_to_search}},
            batch_size=100000,
            projection={"id": 1},
        )

        # remove those prices from network_token_blocks
        for price_id in price_ids_in_database:
            try:
                if "polygon_zkevm" in price_id:
                    network, network2, block, address = price_id["id"].split("_")
                    network = f"{network}_{network2}"
                else:
                    network, block, address = price_id["id"].split("_")

                if address in network_token_blocks[network]:
                    if len(network_token_blocks[network][address]) == 0:
                        network_token_blocks[network].pop(address)
                    else:
                        network_token_blocks[network][address].pop(block)
            except Exception as e:
                pass

        with tqdm.tqdm(total=len(network_token_blocks)) as progress_bar:
            for network, addresses in network_token_blocks.items():
                # create a queue item list to add to queue when enabled
                to_queue_items = []

                logging.getLogger(__name__).info(
                    f" > Trying to repair {len(addresses)} tokens price from {network}"
                )
                progress_bar.total += len(addresses)

                for address, blocks_data in addresses.items():
                    progress_bar.total += len(addresses)
                    for block, counter in blocks_data.items():
                        # block is string
                        block = int(block)

                        progress_bar.set_description(
                            f" Check & solve {network}'s price error log entries for {address[-4:]} at block {block}"
                        )
                        progress_bar.update(0)

                        # counter = number of times found in logs
                        if counter >= min_count:
                            # add to queue
                            if add_to_queue:
                                to_queue_items.append(
                                    QueueItem(
                                        type=queueItemType.PRICE,
                                        block=block,
                                        address=address,
                                        data={},
                                    ).as_dict
                                )
                            else:
                                price, source = get_price(
                                    network=network, token_address=address, block=block
                                )
                                if price:
                                    logging.getLogger(__name__).debug(
                                        f" Added {price} as price for {network}'s {address} at block {block}  (found {counter} times in log) source: {source}"
                                    )
                                    add_price_to_token(
                                        network=network,
                                        token_address=address,
                                        block=block,
                                        price=price,
                                        source=source,
                                    )
                                else:
                                    logging.getLogger(__name__).debug(
                                        f" Could not find price for {network}'s {address} at block {block}  (found {counter} times in log)"
                                    )

                        else:
                            logging.getLogger(__name__).debug(
                                f" Not procesing price for {network}'s {address} at block {block} bc it has been found only {counter} times in log."
                            )

                        # update progress
                        progress_bar.update(1)

                    # update progress
                    progress_bar.update(1)

                # update progress
                progress_bar.update(1)

    except Exception as e:
        logging.getLogger(__name__).exception(
            " unexpected error checking prices from log"
        )

    # add all items to queue, when enabled
    if add_to_queue and len(to_queue_items):
        if db_return := get_default_localdb(network).replace_items_to_database(
            data=to_queue_items, collection_name="queue"
        ):
            if (
                db_return.inserted_count
                or db_return.upserted_count
                or db_return.modified_count
            ):
                logging.getLogger(__name__).debug(
                    f" Added {len(to_queue_items)} prices to the queue for {network}."
                )
            else:
                logging.getLogger(__name__).warning(
                    f" No prices added to the queue for {network}. Database returned:  {db_return.bulk_api_result}"
                )
        else:
            logging.getLogger(__name__).error(
                f" No database return while adding prices to the queue for {network}."
            )


def repair_prices_from_status(
    batch_size: int = 100000, max_repair_per_network: int | None = None
):
    """Check prices not present in database but present in hypervisors and rewards status and add them to the QUEUE to be processed"""

    logging.getLogger(__name__).info(
        f">Check prices not present in database but present in hypervisors and rewards status and add them to the queue to be processed"
    )
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        with tqdm.tqdm(total=len(networks)) as progress_bar:
            for network in networks:
                try:
                    # database name
                    db_name = f"{network}_{protocol}"

                    # database helper
                    def _db():
                        return database_local(mongo_url=mongo_url, db_name=db_name)

                    logging.getLogger(__name__).info(
                        f" Building a price id list of addresses and blocks that should be present in the database for {network}"
                    )
                    # prices to get = all token0 and token1 addresses from hypervisor status + rewarder status blocks
                    # price id = network_block_address
                    (
                        price_ids_shouldBe,
                        blocks_shouldBe,
                    ) = shouldBe_price_ids_from_status_hypervisors(
                        network=network
                    )  # set()
                    # add rewards
                    price_ids_shouldBe.update(
                        shouldBe_price_ids_from_status_rewards(network=network)
                    )  # =  set()
                    # progress
                    progress_bar.set_description(
                        f" {network} should be prices: {len(price_ids_shouldBe)}"
                    )
                    progress_bar.update(0)

                    logging.getLogger(__name__).info(
                        f" Checking if there are {len(price_ids_shouldBe)} prices for {network} in the price database"
                    )

                    if price_ids_diffs := price_ids_shouldBe - set(
                        [
                            id["id"]
                            for id in database_global(
                                mongo_url=mongo_url
                            ).get_items_from_database(
                                collection_name="usd_prices",
                                find={"network": network},
                                batch_size=batch_size,
                                projection={"_id": 0, "id": 1},
                            )
                        ]
                    ):
                        logging.getLogger(__name__).info(
                            f" Found {len(price_ids_diffs)} missing prices for {network}"
                        )

                        try:
                            # check if those prices are already in the queue to be processed
                            # price id : create_id_price(network, block, address)
                            if price_ids_in_queue := set(
                                [
                                    create_id_price(network, x["block"], x["address"])
                                    for x in _db().get_items_from_database(
                                        collection_name="queue",
                                        find={"type": queueItemType.PRICE},
                                        batch_size=batch_size,
                                        projection={
                                            "_id": 0,
                                            "type": 1,
                                            "block": 1,
                                            "address": 1,
                                        },
                                    )
                                ]
                            ):
                                # remove prices already in queue from price_ids_diffs
                                logging.getLogger(__name__).info(
                                    f" Found {len(price_ids_in_queue)} prices already in queue for {network}. Removing them from the process list"
                                )
                                price_ids_diffs = price_ids_diffs - price_ids_in_queue

                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error getting queued prices for {network}"
                            )

                        # do not repair more than max_repair_per_network prices at once to avoid being too much time in the same network
                        if (
                            max_repair_per_network
                            and len(price_ids_diffs) > max_repair_per_network
                        ):
                            logging.getLogger(__name__).info(
                                f" Selecting a random sample of {max_repair_per_network} prices due to maximum repair limit set."
                            )
                            # choose to repair the most recent ones first
                            price_ids_diffs = sorted(price_ids_diffs, reverse=True)[
                                :max_repair_per_network
                            ]
                            # choose to repair the first max_repair_per_network
                            # price_ids_diffs = random.sample(
                            #     price_ids_diffs, max_repair_per_network
                            # )

                        # progress_bar.total += len(price_ids_diffs)

                        def create_queue_item(price_id):
                            network, block, address = price_id.split("_")
                            return QueueItem(
                                type=queueItemType.PRICE,
                                block=block,
                                address=address,
                                data={},
                            ).as_dict

                        # create a list of queue items tobe added to database
                        to_queue_items = [
                            create_queue_item(price_id) for price_id in price_ids_diffs
                        ]

                        # TODO: create a list of blocks to be added to database
                        # to_queue_items += [
                        #   QueueItem(
                        #        type=queueItemType.BLOCK,
                        #        block=block,
                        #        address="0x0000000000000000000000000000000000000000",
                        #        data={},
                        #    ).as_dict for block in blocks_shouldBe
                        # ]

                        # add to queue
                        if result := _db().replace_items_to_database(
                            data=to_queue_items, collection_name="queue"
                        ):
                            if (
                                result.inserted_count
                                or result.upserted_count
                                or result.modified_count
                            ):
                                logging.getLogger(__name__).debug(
                                    f" Added {len(to_queue_items)} prices to the queue for {network}."
                                )
                            else:
                                logging.getLogger(__name__).warning(
                                    f" No prices added to the queue for {network}. Database returned:  {result.bulk_api_result}"
                                )
                        else:
                            logging.getLogger(__name__).error(
                                f" No database return while adding prices to the queue for {network}."
                            )
                        progress_bar.update(1)

                    else:
                        logging.getLogger(__name__).info(
                            f" No missing prices found for {network}"
                        )

                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" error in {network} while repairing prices from hype status  {e} "
                    )

                # progress
                progress_bar.update(1)


def shouldBe_price_ids_from_status_rewards(
    network: str, batch_size: int = 100000
) -> set[str]:
    """List of price ids that should be present in the database, built using hypervisor status linked to static rewards

    Args:
        network (str):

    Returns:
        set[str]:
    """

    logging.getLogger(__name__).debug(
        f" Building a 'should be' price id list from rewards for {network}"
    )

    # create a result price ids
    price_ids = set()

    # special query to get the list
    query = [
        {
            "$project": {
                "hypervisor_address": "$hypervisor_address",
                "rewardToken": "$rewardToken",
            }
        },
        {
            "$lookup": {
                "from": "status",
                "localField": "hypervisor_address",
                "foreignField": "address",
                "as": "hypervisor_status",
                "pipeline": [
                    {"$project": {"block": "$block"}},
                ],
            }
        },
        {"$unwind": "$hypervisor_status"},
        {
            "$project": {
                "rewardToken": "$rewardToken",
                "block": "$hypervisor_status.block",
            }
        },
        {"$sort": {"block": 1}},
    ]

    # build a list of token addresses to exclude from this network
    addresses_to_exclude = list(TOKEN_ADDRESS_EXCLUDE.get(network, {}).keys())

    price_ids.update(
        [
            create_id_price(network, item["block"], item["rewardToken"])
            for item in get_from_localdb(
                network=network,
                collection="rewards_static",
                aggregate=query,
                batch_size=batch_size,
            )
            if item["rewardToken"] not in addresses_to_exclude
        ]
    )

    return price_ids


def shouldBe_price_ids_from_status_hypervisors(
    network: str, batch_size: int = 100000
) -> tuple[set[str], set]:
    """List of price ids that should be present in the database, built using hypervisor status
    Args:
        network (str):

    Returns:
        list[str]:  list of price ids and list of blocks
    """

    logging.getLogger(__name__).debug(
        f" Building a 'should be' price id list from hypervisor status for {network}"
    )

    # create a result price ids
    price_ids = set()
    block_ids = set()

    # database helper
    local_db = database_local(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
        db_name=f"{network}_gamma",
    )

    for hype_status in get_from_localdb(
        network=network,
        collection="status",
        find={},
        batch_size=batch_size,
        projection={
            "block": 1,
            "pool.token0.address": 1,
            "pool.token1.address": 1,
            "address": 1,
        },
    ):
        # get all hypervisor status blocks and build a price id for each one
        price_ids.add(
            create_id_price(
                network, hype_status["block"], hype_status["pool"]["token0"]["address"]
            )
        )
        price_ids.add(
            create_id_price(
                network, hype_status["block"], hype_status["pool"]["token1"]["address"]
            )
        )
        # add block to block_ids
        block_ids.add(hype_status["block"])

    return price_ids, block_ids


def repair_prices_from_database(
    batch_size: int = 100000, max_repair_per_network: int | None = None
):
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(
                f" > Trying to repair {network}'s prices from database (old prices)"
            )
            feed_prices(
                network=network,
                price_ids=create_tokenBlocks_all(network=network),
            )


def repair_prices_from_status_OLD(
    batch_size: int = 100000, max_repair_per_network: int | None = None
):
    """Check prices not present in database but present in hypervisors and rewards status and try to scrape again"""

    logging.getLogger(__name__).info(
        f">Check prices not present in database but present in hypervisors and rewards status and try to scrape again"
    )
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        with tqdm.tqdm(total=len(networks)) as progress_bar:
            for network in networks:
                # database name
                db_name = f"{network}_{protocol}"

                # database helper
                def _db():
                    return database_local(mongo_url=mongo_url, db_name=db_name)

                # prices to get = all token0 and token1 addresses from hypervisor status + rewarder status blocks
                # price id = network_block_address
                price_ids_shouldBe = set()
                blocks_shouldBe = set()
                # progress
                progress_bar.set_description(
                    f" {network} should be prices: {len(price_ids_shouldBe)}"
                )
                progress_bar.update(0)

                # get all token addressess + block from status hypervisors
                logging.getLogger(__name__).info(
                    f" Getting hypervisor status token addresses and blocks for {network}"
                )
                for hype_status in _db().get_items_from_database(
                    collection_name="status",
                    find={},
                    batch_size=batch_size,
                    projection={"pool": 1, "block": 1},
                ):
                    # add token addresses
                    price_ids_shouldBe.add(
                        create_id_price(
                            network=network,
                            block=hype_status["block"],
                            token_address=hype_status["pool"]["token0"]["address"],
                        )
                    )
                    price_ids_shouldBe.add(
                        create_id_price(
                            network=network,
                            block=hype_status["block"],
                            token_address=hype_status["pool"]["token1"]["address"],
                        )
                    )
                    # add block
                    blocks_shouldBe.add(hype_status["block"])

                    # progress
                    progress_bar.set_description(
                        f" {network} should be prices: {len(price_ids_shouldBe)}"
                    )
                    progress_bar.update(0)

                logging.getLogger(__name__).info(
                    f" Getting rewarder status token addresses and blocks for {network}"
                )
                for rewarder_status in _db().get_items_from_database(
                    collection_name="rewards_status",
                    find={"blocks": {"$nin": list(blocks_shouldBe)}},
                    batch_size=batch_size,
                    projection={"rewardToken": 1, "block": 1},
                ):
                    # add token addresses
                    price_ids_shouldBe.add(
                        create_id_price(
                            network=network,
                            block=rewarder_status["block"],
                            token_address=rewarder_status["rewardToken"],
                        )
                    )

                    # add block
                    blocks_shouldBe.add(rewarder_status["block"])

                    # progress
                    progress_bar.set_description(
                        f" {network} should be prices: {len(price_ids_shouldBe)}"
                    )
                    progress_bar.update(0)

                logging.getLogger(__name__).info(
                    f" Checking if there are {len(price_ids_shouldBe)} prices for {network} in the price database"
                )

                if price_ids_diffs := price_ids_shouldBe - set(
                    [
                        id["id"]
                        for id in database_global(
                            mongo_url=mongo_url
                        ).get_items_from_database(
                            collection_name="usd_prices",
                            find={"network": network},
                            batch_size=batch_size,
                            projection={"_id": 0, "id": 1},
                        )
                    ]
                ):
                    logging.getLogger(__name__).info(
                        f" Found {len(price_ids_diffs)} missing prices for {network}"
                    )

                    try:
                        # check if those prices are already in the queue to be processed
                        # price id : create_id_price(network=network, block=x['block'], token_address=x['address'])
                        if price_ids_in_queue := set(
                            [
                                create_id_price(
                                    network=network,
                                    block=x["block"],
                                    token_address=x["address"],
                                )
                                for x in _db().get_items_from_database(
                                    collection_name="queue",
                                    find={"type": queueItemType.PRICE},
                                    batch_size=batch_size,
                                    projection={
                                        "_id": 0,
                                        "type": 1,
                                        "block": 1,
                                        "address": 1,
                                    },
                                )
                            ]
                        ):
                            # remove prices already in queue from price_ids_diffs
                            logging.getLogger(__name__).info(
                                f" Found {len(price_ids_in_queue)} prices already in queue for {network}. Removing them from the process list"
                            )
                            price_ids_diffs = price_ids_diffs - price_ids_in_queue

                    except Exception as e:
                        logging.getLogger(__name__).exception(
                            f" Error getting queued prices for {network}"
                        )

                    # do not repair more than max_repair_per_network prices at once to avoid being too much time in the same network
                    if (
                        max_repair_per_network
                        and len(price_ids_diffs) > max_repair_per_network
                    ):
                        logging.getLogger(__name__).info(
                            f" Selecting a random sample of {max_repair_per_network} prices due to maximum repair limit set. Next loop will repair the next ones."
                        )
                        # choose to repair the first max_repair_per_network
                        price_ids_diffs = random.sample(
                            price_ids_diffs, max_repair_per_network
                        )

                    progress_bar.total += len(price_ids_diffs)
                    # get prices
                    for price_id in price_ids_diffs:
                        network, block, address = price_id.split("_")
                        logging.getLogger(__name__).debug(
                            f" Getting price for {network}'s {address} at block {block}"
                        )

                        price, source = get_price(
                            network=network, token_address=address, block=block
                        )
                        if price:
                            logging.getLogger(__name__).debug(
                                f" Added {price} as price for {network}'s {address} at block {block} source {source}"
                            )
                            add_price_to_token(
                                network=network,
                                token_address=address,
                                block=block,
                                price=price,
                                source=source,
                            )
                        else:
                            logging.getLogger(__name__).debug(
                                f" Could not find price for {network}'s {address} at block {block}"
                            )

                        # progress
                        progress_bar.set_description(f" {network} {address} {block}")
                        progress_bar.update(1)

                else:
                    logging.getLogger(__name__).info(
                        f" No missing prices found for {network}"
                    )

                # progress
                progress_bar.update(1)


def reScrape_database_prices(
    batch_size=100000, protocol="gamma", network_limit: int | None = None
):
    """Rescrape thegraph database prices

    Args:
        batch_size (int, optional): . Defaults to 100000.
        protocol (str, optional): . Defaults to "gamma".
        network_limit(int, optional): Maximum number of prices to process. Defaults to 5000.
    """
    logging.getLogger(__name__).info(f">Re scrape thegraph prices, in reverse order ")
    networks = (
        CONFIGURATION["_custom_"]["cml_parameters"].networks
        or CONFIGURATION["script"]["protocols"][protocol]["networks"]
    )

    for network in networks:
        # get reversed list of prices and rescrape them
        database_items = database_global(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        ).get_items_from_database(
            collection_name="usd_prices",
            find={
                "network": network,
                "source": {"$in": ["thegraph"]},
            },
            sort=[("block", -1)],
            batch_size=batch_size,
        )
        # limit number of prices to process
        if network_limit:
            logging.getLogger(__name__).info(
                f" Found {len(database_items):,.0f} prices for {network} -> limiting them to {network_limit:,.0f} "
            )
            database_items = database_items[:network_limit]
        else:
            logging.getLogger(__name__).info(
                f" Found {len(database_items):,.0f} prices to update for {network}"
            )

        different = 0
        with tqdm.tqdm(total=len(database_items)) as progress_bar:
            for db_price_item in database_items:
                try:
                    # progress
                    progress_bar.set_description(
                        f" {network} processing 0x..{db_price_item['address'][:-4]}. Total diff prices found: {different}"
                    )
                    progress_bar.update(0)

                    price, source = get_price(
                        network=network,
                        token_address=db_price_item["address"],
                        block=int(db_price_item["block"]),
                    )
                    # get price
                    if price:
                        if price != db_price_item["price"] and price != 0:
                            different += 1

                            logging.getLogger(__name__).debug(
                                f" Different price found for {network}'s {db_price_item['address']} at block {db_price_item['block']}. Updating price {db_price_item['price']} to {price}"
                            )
                            # update price
                            database_global(
                                mongo_url=CONFIGURATION["sources"]["database"][
                                    "mongo_server_url"
                                ]
                            ).set_price_usd(
                                network=network,
                                token_address=db_price_item["address"],
                                block=int(db_price_item["block"]),
                                price_usd=price,
                                source=source,
                            )

                    else:
                        logging.getLogger(__name__).debug(
                            f" No price found for {network}'s {db_price_item['address']} at block {db_price_item['block']}."
                        )
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f"  while processing prices --> {e}"
                    )
                # update progress
                progress_bar.update(1)


def repair_hypervisor_status():
    # from user_status debug log
    repair_hype_status_from_user()

    # missing hypes
    repair_missing_hype_status()

    # binance
    repair_binance_hypervisor_status()
    repair_binance_queue_hype_status()


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


def repair_missing_hypervisor_status_OLD(
    protocol: str, network: str, cache: bool = True, max_repair: int = None
):
    """Creates hypervisor status at all operations block and block-1 not already present in database,
        using the difference between operations and status blocks

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
    for hype in database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_items_from_database(collection_name="static", find={}):
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

            logging.getLogger(__name__).info(
                f"  Feeding hypervisor status collection with {len(difference_blocks)} blocks for {network}'s {hype['address']}"
            )

            # prepare arguments for paralel scraping
            args = (
                (
                    hype["address"],
                    network,
                    block,
                    hype["dex"],
                    False,
                    None,
                    None,
                    cache,
                    "private",
                )
                for block in difference_blocks
            )
            # scrape missing status
            _errors = 0
            with tqdm.tqdm(total=len(difference_blocks)) as progress_bar:
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    for result in ex.map(lambda p: build_db_hypervisor(*p), args):
                        if result is None:
                            # error found
                            _errors += 1

                        else:
                            # add hypervisor status to database
                            database_local(
                                mongo_url=mongo_url, db_name=db_name
                            ).set_status(data=result)

                        progress_bar.set_description(
                            f" Found {_errors} errors while snapshooting hypervisor status"
                        )
                        # update progress
                        progress_bar.update(1)


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


def repair_rewards_status():
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            repair_missing_rewards_status(
                chain=text_to_chain(network),
                max_repair=CONFIGURATION["_custom_"]["cml_parameters"].maximum,
            )


def repair_missing_rewards_status(
    chain: Chain, max_repair: int = None, hypervisor_addresses: list[str] | None = None
):
    """ """
    batch_size = 100000
    logging.getLogger(__name__).info(
        f"> Finding missing {chain.database_name}'s rewards status in database"
    )
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{chain.database_name}_gamma"

    # loop thru all static rewards in database
    for reward_static in tqdm.tqdm(
        database_local(mongo_url=mongo_url, db_name=db_name).get_items_from_database(
            collection_name="rewards_static",
            find={"hypervisor_address": {"$in": hypervisor_addresses}}
            if hypervisor_addresses
            else {},
            batch_size=batch_size,
            sort=[("_id", -1)],
        )
    ):
        # do not process excluded tokens
        if reward_static["rewardToken"] in TOKEN_ADDRESS_EXCLUDE.get(chain, {}):
            continue

        # get rewards_status ids and blocks from database
        rewards_status_ids = []
        rewards_status_blocks = []
        for item in database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="rewards_status",
            find={
                "hypervisor_address": reward_static["hypervisor_address"],
                "rewardToken": reward_static["rewardToken"],
            },
            projection={"id": 1, "_id": 0, "block": 1},
            batch_size=batch_size,
        ):
            rewards_status_ids.append(item["id"])
            rewards_status_blocks.append(int(item["block"]))

        # sort by block desc so that newer items can be seen first
        hypervisors_status_lits = database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="status",
            find={
                "address": reward_static["hypervisor_address"],
                "$and": [
                    {"block": {"$gte": reward_static["block"]}},
                    {"block": {"$nin": rewards_status_blocks}},
                ],
                "totalSupply": {"$ne": "0"},
            },
            sort=[("block", -1)],
            batch_size=batch_size,
        )

        if hypervisors_status_lits:
            logging.getLogger(__name__).debug(
                f" Found {len(hypervisors_status_lits)} missing rewards status blocks for {chain.database_name}'s {reward_static['hypervisor_address']}"
            )
            if max_repair and len(hypervisors_status_lits) > max_repair:
                # select the most recent
                logging.getLogger(__name__).info(
                    f"  Selecting the most recent {max_repair} rewards status missing due to max_repair limit set."
                )
                hypervisors_status_lits = hypervisors_status_lits[:max_repair]
                # make a random selection
                # logging.getLogger(__name__).info(
                #     f"  Selecting a random sample of {max_repair} rewards status missing due to max_repair limit set."
                # )
                # hypervisors_status_lits = random.sample(
                #     hypervisors_status_lits, max_repair
                # )

            logging.getLogger(__name__).info(
                f" A total of {len(hypervisors_status_lits)} rewards status will be added to the queue (prices may also get queued when needed)"
            )

            # prepare arguments for treaded function
            args = (
                (
                    hype_status,
                    chain.database_name,
                )
                for hype_status in hypervisors_status_lits
            )

            # get hypervisor status gte reward_static block
            with concurrent.futures.ThreadPoolExecutor() as ex:
                for result in ex.map(
                    lambda p: build_and_save_queue_from_hypervisor_status(*p), args
                ):
                    # progress_bar.update(0)
                    pass

        else:
            logging.getLogger(__name__).debug(
                f" No missing rewards status found for {chain.database_name}'s {reward_static['hypervisor_address']}"
            )


def repair_hype_status_from_user(min_count: int = 1):
    protocol = "gamma"

    network_token_blocks = {}
    for log_file in get_all_logfiles():
        network_token_blocks.update(get_failed_status_from_log(log_file=log_file))

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
                                cached=False,
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


def repair_queue():
    # locked items
    repair_queue_locked_items()

    # TODO: replace with non fixed list lenght solution like the "manual_scrape_from_queue" from  tests
    # try process failed items with count > 10
    # repair_queue_failed_items(
    #    count_gte=CONFIGURATION["_custom_"]["cml_parameters"].queue_count or 10
    # )


def repair_queue_locked_items():
    """
    Reset queue items that are locked for more than 10 minutes
    No queue item should be running for more than 2 minutes
    items with the field count =>10 will not be unlocked
    """

    logging.getLogger(__name__).info(
        f">Repair queue items that are locked for more than 10 minutes..."
    )
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(f"          processing {network} ...")
            # get a list of queue items with processing >0
            db_name = f"{network}_{protocol}"
            for queue_item in tqdm.tqdm(
                database_local(
                    mongo_url=mongo_url, db_name=db_name
                ).get_items_from_database(
                    collection_name="queue",
                    find={"processing": {"$gt": 0}, "count": {"$lt": 10}},
                )
            ):
                # check seconds passed since processing
                minutes_passed = (time.time() - queue_item["processing"]) / 60
                if minutes_passed > 10:
                    # free locked processing
                    database_local(
                        mongo_url=mongo_url, db_name=db_name
                    ).free_queue_item(queue_item)
                    logging.getLogger(__name__).debug(
                        f" {network}'s queue item {queue_item['id']} has been in the processing state for {minutes_passed} minutes. It probably halted. Freeing it..."
                    )


def repair_queue_failed_items(
    queue_item_type: queueItemType | None = None,
    count_gte: int | None = None,
):
    logging.getLogger(__name__).info(
        f">Repair queue items that have failed more than 10 times ..."
    )

    # get all 1st rewarder status from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    protocol = "gamma"
    db_collection = "queue"
    batch_size = 100000

    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(f"          processing {network} ...")
            # get a list of queue items with count > 10
            db_name = f"{network}_{protocol}"
            find = {}
            # construct query
            if count_gte:
                find["count"] = {"$gte": count_gte}
            if queue_item_type:
                find["type"] = queue_item_type.value

            # get queue items
            for db_queue_item in tqdm.tqdm(
                database_local(
                    mongo_url=mongo_url, db_name=db_name
                ).get_items_from_database(
                    collection_name=db_collection, find=find, batch_size=batch_size
                )
            ):
                # convert database queue item to class
                queue_item = QueueItem(**db_queue_item)

                # modify queue item count so that can be processed
                queue_item.count = 9

                # process queue item
                if process_queue_item_type(network=network, queue_item=queue_item):
                    logging.getLogger(__name__).debug(
                        f"Queue item {queue_item.id} processed"
                    )

                else:
                    logging.getLogger(__name__).warning(
                        f"Queue item {queue_item.id} could not be processed"
                    )


def repair_operations():
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(f"  Repairing {network} operations ")
            repair_operations_chain(chain=text_to_chain(network))


def repair_operations_chain(
    chain: Chain,
    dex: Protocol | None = None,
    block_ini: int = None,
    use_last_block: bool = False,
    blocks_back: float = 0,
):
    """scrape operations from the specified blocks and save them to database

    Args:
        chain (Chain):
        dex (Protocol | None, optional): . Defaults to None.
        block_ini (int, optional): Force an initial block . Defaults to the first static block found in database.
        use_last_block: block_ini will beguin from the last block found in database. Defaults to False.
        blocks_back (float, optional): percentage of blocks to substract to the last block found in database. Defaults to 0.
    """

    batch_size = 100000

    ########## CONFIG #############
    protocol = "gamma"
    network = chain.database_name
    dex = dex.database_name if dex else None  # can be None
    force_back_time = True
    ###############################

    find = {}
    # filter by dex
    if dex:
        find["dex"] = dex

    # get static hypervisor blocks ( creation)
    hypervisor_list = get_from_localdb(
        network=network,
        collection="static",
        find=find,
        projection={"block": 1},
        batch_size=batch_size,
    )
    # set initial block
    if use_last_block:
        # get the max block from static hypervisor info
        block_ini = block_ini or max([h["block"] for h in hypervisor_list])
    else:
        # get the min block from static hypervisor info
        block_ini = block_ini or min([h["block"] for h in hypervisor_list])

    # remove blocks back when specified
    block_ini = block_ini - int(block_ini * blocks_back)

    # feed operations
    feed_operations(
        protocol=protocol,
        network=network,
        block_ini=block_ini,
        force_back_time=force_back_time,
    )


# def repair_hypervisor_static():
#     logging.getLogger(__name__).info(
#         f">Repair hypervisor static database ... remove disabled hypes and add missing hypes"
#     )

#     # get all 1st rewarder status from database
#     mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
#     db_collection = "static"
#     batch_size = 100000

#     # override networks if specified in cml
#     networks = (
#         CONFIGURATION["_custom_"]["cml_parameters"].networks
#         or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
#     )
#     for network in networks:
#         logging.getLogger(__name__).info(f"          processing {network} ...")
#         # get a list of queue items with count > 10
#         db_name = f"{network}_gamma"
#         find = {}

#         # get queue items
#         for db_hypervisor_static in tqdm.tqdm(
#             database_local(
#                 mongo_url=mongo_url, db_name=db_name
#             ).get_items_from_database(
#                 collection_name=db_collection, find=find, batch_size=batch_size
#             )
#         ):
#             # convert database queue item to class
#             hypervisor_static = HypervisorStatic(**db_hypervisor_static)

#             # get hypervisor
#             hypervisor = get_hypervisor(
#                 network=network, hypervisor_address=hypervisor_static.address
#             )

#             # get hypervisor static
#             hypervisor_static = hypervisor.get_hypervisor_static()

#             # update hypervisor static
#             database_local(
#                 mongo_url=mongo_url, db_name=db_name
#             ).replace_items_to_database(
#                 data=[hypervisor_static.dict()],
#                 collection_name=db_collection,
#                 key="address",
#             )


# one time utils
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


def replace_quickswap_pool_dex_to_algebra(network: str, protocol: str = "gamma"):
    logging.getLogger(__name__).debug("    Convert quickswap pool dex to algebra")

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    # get all status to be modded
    status_to_modify = local_db_manager.get_items_from_database(
        collection_name="status", find={"pool.dex": "quickswap"}
    )
    _errors = 0
    with tqdm.tqdm(total=len(status_to_modify)) as progress_bar:

        def loopme(status):
            status["pool"]["dex"] = "algebrav3"
            local_db_manager.set_status(data=status)
            return status

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for status in ex.map(loopme, status_to_modify):
                progress_bar.set_description(
                    f" Convert {network}'s status quickswap pool dex to algebra  id: {status['id']}"
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
            with contextlib.suppress(Exception):
                # get timestamp from database
                status["timestamp"] = all_blocks[status["block"]]
                status["pool"]["timestamp"] = status["timestamp"]
                status["pool"]["token0"]["timestamp"] = status["timestamp"]
                status["pool"]["token1"]["timestamp"] = status["timestamp"]

                saveit = True
            if not saveit:
                with contextlib.suppress(Exception):
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


# helpers
def add_price_to_token(
    network: str, token_address: str, block: int, price: float, source: databaseSource
):
    """force special price add to database:
     will create a field called "origin" with "manual" as value to be ableto identify at db

    Args:
        network (str):
        token_address (str):
        block (int):
        price (float):
    """

    # setup database managers
    global_db_manager = database_global(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
    )

    global_db_manager.set_price_usd(
        network=network,
        block=block,
        token_address=token_address,
        price_usd=price,
        source=source,
    )


def get_price_of_token(network: str, token_address: str, block: int) -> float:
    """get price of token at block

    Args:
        network (str):
        token_address (str):
        block (int):

    Returns:
        float:
    """

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    # get price from database
    price = global_db_manager.get_price_usd(
        network=network, block=block, address=token_address
    )

    if price:
        return price[0]["price"]
    else:
        return 0.0


def get_price(
    network: str, token_address: str, block: int
) -> tuple[float, databaseSource]:
    """get price of token at block
    Will return a tuple with price and source
    """
    return price_scraper(
        cache=False,
        thegraph=False,
        geckoterminal_sleepNretry=True,
        source_order=[
            databaseSource.ONCHAIN,
            databaseSource.GECKOTERMINAL,
            databaseSource.COINGECKO,
        ],
    ).get_price(network=network, token_id=token_address, block=block)


def auto_get_prices():
    # set prices to get
    address_block_list = {
        # "ethereum": {
        #     "0xf4dc48d260c93ad6a96c5ce563e70ca578987c74": [14982409],
        #     "0x0642026e7f0b6ccac5925b4e7fa61384250e1701": [15171687],
        #     "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": [16701232],
        #     "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": [13047429],
        #     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": [14953317, 12825206],
        #     "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": [12957386],
        #     "0x77fba179c79de5b7653f68b5039af940ada60ce0": [12996888],
        #     "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0": [12948766],
        # },
        # "polygon": {
        #     "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": [
        #         39745459,
        #         39745460,
        #         39745491,
        #         39745492,
        #         39745534,
        #         39745535,
        #         39745541,
        #         39745542,
        #         39746053,
        #         39746054,
        #         39746062,
        #         39746063,
        #         39068569,
        #         39423640,
        #         39613083,
        #         39616413,
        #     ]
        # }
    }

    address_block_list["ethereum"] = {
        "0xb41f289d699c5e79a51cb29595c203cfae85f32a": [
            13856873,
            13856900,
            13864770,
            13856874,
            13856901,
            13864769,
        ]
    }

    # loop query n save
    for network, data in address_block_list.items():
        for address, blocks in data.items():
            for block in blocks:
                price, source = get_price(
                    network=network, token_address=address, block=block
                )
                if price != 0:
                    logging.getLogger(__name__).debug(
                        f" Added price for {network}'s {address} at block {block} from source: {source}"
                    )
                    add_price_to_token(
                        network=network, token_address=address, block=block, price=price
                    )
                else:
                    logging.getLogger(__name__).debug(
                        f" Could not add price for {network}'s {address} at block {block}"
                    )


def get_all_logfiles(log_names: list = ["error", "debug", "price"]) -> list:
    """get all logfiles from config or default"""

    logfiles = []

    for logPath in CONFIGURATION["_custom_"]["cml_parameters"].check_logs or [
        CONFIGURATION["logs"]["save_path"]
    ]:
        if os.path.isfile(logPath):
            logfiles.append(logPath)
        elif os.path.isdir(logPath):
            for root, dirs, files in os.walk(logPath):
                # avoid to load "check" related logs ( current app log)
                if (
                    # CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder
                    # or
                    "check"
                    not in root.lower()
                ):
                    for file in files:
                        if file.endswith(".log") and (
                            any([x.lower() in file.lower() for x in log_names])
                            or len(log_names) == 0
                        ):
                            #     "debug" in file.lower() or "price" in file.lower()
                            # ):
                            logfiles.append(os.path.join(root, file))

    return logfiles


def load_logFile(logfile: str) -> str:
    """load logfile and return list of lines"""

    # load file
    result = ""
    if os.path.isfile(logfile):
        with open(logfile, mode="r", encoding="utf8") as f:
            result = f.read()
    else:
        logging.getLogger(__name__).error(f"Error: File not found {logfile}")

    return result


# checks
def check_database():
    # setup global database manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    with tqdm.tqdm(total=len(CONFIGURATION["script"]["protocols"])) as progress_bar:
        # checks
        for protocol, networks in CONFIGURATION["script"]["protocols"].items():
            for network, dexes in networks["networks"].items():
                # setup local database manager
                db_name = f"{network}_{protocol}"
                local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

                # progress
                progress_bar.set_description(
                    f" Checking {network}'s blocks from operations and status"
                )
                progress_bar.update(0)
                # check blocks
                chek_localdb_blocks(local_db_manager=local_db_manager)

                # progress
                progress_bar.set_description(
                    f" Checking {network}'s token stables usd prices"
                )
                progress_bar.update(0)
                # check stable prices
                check_stable_prices(
                    network=network,
                    local_db_manager=local_db_manager,
                    global_db_manager=global_db_manager,
                )

                # update progress
                progress_bar.update(1)

        # check GLOBAL
        # progress
        progress_bar.set_description(" Checking global blocks collection")
        progress_bar.update(0)
        # check blocks
        chek_globaldb_blocks(global_db_manager=global_db_manager)


def chek_localdb_blocks(local_db_manager: database_local):
    """check if blocks are typed correctly

    Args:
        local_db_manager (database_local):
    """

    if blocks_operatons := local_db_manager.get_items_from_database(
        collection_name="operations",
        find={"blockNumber": {"$not": {"$type": "int"}}},
    ):
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_operatons)} operations with the block field not being int"
        )

    if blocks_status := local_db_manager.get_items_from_database(
        collection_name="status", find={"block": {"$not": {"$type": "int"}}}
    ):
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_status)} hypervisor status with the block field not being int"
        )


def chek_globaldb_blocks(global_db_manager: database_global):
    """check that blocks have the correct type

    Args:
        global_db_manager (database_global):
    """

    if blocks_usd_prices := global_db_manager.get_items_from_database(
        collection_name="usd_prices", find={"block": {"$not": {"$type": "int"}}}
    ):
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_usd_prices)} usd prices with the block field not being int: database '{global_db_manager._db_name}' collection 'usd_prices'   ids-> {[x['_id'] for x in blocks_usd_prices]}"
        )
        # try replacing those found non int block prices to int
        replace_blocks_to_int()


def check_status_prices(
    network: str, local_db_manager: database_local, global_db_manager: database_global
):
    """Check that all status tokens have usd prices

    Args:
        local_db_manager (database_local):
        global_db_manager (database_global):
    """
    # get all prices + address + block
    prices = {
        x["id"]
        for x in global_db_manager.get_unique_prices_addressBlock(network=network)
    }

    # get tokens and blocks present in database
    prices_todo = set()

    for x in local_db_manager.get_items_from_database(collection_name="status"):
        for i in [0, 1]:
            db_id = create_id_price(
                network=network,
                block=x["pool"][f"token{i}"]["block"],
                token_address=x["pool"][f"token{i}"]["address"],
            )

            if db_id not in prices:
                prices_todo.add(db_id)

    if prices_todo:
        logging.getLogger(__name__).warning(
            " Found {} token blocks without price, from a total of {} ({:,.1%})".format(
                len(prices_todo), len(prices), len(prices_todo) / len(prices)
            )
        )


def check_stable_prices(
    network: str, local_db_manager: database_local, global_db_manager: database_global
):
    """Search database for predefined stable tokens usd price devisations from 1
        and log it

    Args:
        network (str): _description_
        local_db_manager (database_local):
        global_db_manager (database_global):
    """
    logging.getLogger(__name__).debug(
        f" Seek deviations of {network}'s stable token usd prices from 1 usd"
    )

    stables_symbol_list = ["USDC", "USDT", "LUSD", "DAI"]
    stables = {
        x["pool"]["token0"]["symbol"]: x["pool"]["token0"]["address"]
        for x in local_db_manager.get_items_from_database(
            collection_name="static",
            find={"pool.token0.symbol": {"$in": stables_symbol_list}},
        )
    } | {
        x["pool"]["token1"]["symbol"]: x["pool"]["token1"]["address"]
        for x in local_db_manager.get_items_from_database(
            collection_name="static",
            find={"pool.token1.symbol": {"$in": stables_symbol_list}},
        )
    }

    # database ids var
    db_ids = []

    for x in global_db_manager.get_items_from_database(
        collection_name="usd_prices",
        find={"address": {"$in": list(stables.values())}, "network": network},
    ):
        # check if deviation from 1 is significative
        if abs(x["price"] - 1) > 0.3:
            logging.getLogger(__name__).warning(
                f" Stable {x['network']}'s {x['address']} usd price is {x['price']} at block {x['block']}"
            )
            # add id
            db_ids.append(x["_id"])

    if db_ids:
        logging.getLogger(__name__).warning(
            f" Error found in database '{global_db_manager._db_name}' collection 'usd_prices'  ids: {db_ids}"
        )


def get_failed_prices_from_log(log_file: str) -> dict:
    """Search repeated network + address + block in logs

    Return: {  <network>: {<address>: {<block>:<counter>}}}

    """
    pricelog_regx = "\-\s\s(?P<network>.*)'s\stoken\s(?P<address>.*)\sprice\sat\sblock\s(?P<block>\d*)\snot\sfound"
    debug_regx = "No\sprice\sfor\s(?P<address>.*)\sat\sblock\s(?P<block>\d*).*\[(?P<network>.*)\s(?P<dex>.*)\]"
    debug_regx2 = "No\sprice\sfor\s(?P<network>.*)'s\s(?P<symbol>.*)\s\((?P<address>.*)\).*at\sblock\s(?P<block>\d*)"
    debug_regx3 = "No\sprice\sfor\s(?P<address>.*)\son\s(?P<network>.*)\sat\sblocks\s(?P<block>\d*)"
    user_status_regx = "Can't\sfind\s(?P<network>.*?)'s\s(?P<hype_address>.*?)\susd\sprice\sfor\s(?P<address>.*?)\sat\sblock\s(?P<block>\d*?)\.\sReturn\sZero"
    # groups->  network, symbol, address, block

    # load file
    log_file_content = load_logFile(logfile=log_file)

    # set a var
    network_token_blocks = {}

    for regx_txt in [
        debug_regx,
        pricelog_regx,
        debug_regx2,
        debug_regx3,
        user_status_regx,
    ]:
        if matches := re.finditer(regx_txt, log_file_content):
            for match in matches:
                network = match.group("network")
                address = match.group("address")
                block = match.group("block")

                # network
                if network not in network_token_blocks:
                    network_token_blocks[network] = {}
                # address
                if address not in network_token_blocks[network]:
                    network_token_blocks[network][address] = {}
                # block
                if block not in network_token_blocks[network][address]:
                    network_token_blocks[network][address][block] = 0

                # counter ( times encountered)
                network_token_blocks[network][address][block] += 1

    return network_token_blocks


def get_failed_status_from_log(log_file: str) -> dict:
    # load file
    log_file_content = load_logFile(logfile=log_file)

    regx_txt = "No\shypervisor\sstatus\sfound\sfor\s(?P<network>.*)'s\s(?P<address>.*)\sat\sblock\s(?P<block>\d*)"
    # set a var
    network_token_blocks = {}

    # find hypervisor status not found
    if matches := re.finditer(regx_txt, log_file_content):
        for match in matches:
            network = match.group("network")
            address = match.group("address")
            block = match.group("block")

            # network
            if network not in network_token_blocks:
                network_token_blocks[network] = {}
            # address
            if address not in network_token_blocks[network]:
                network_token_blocks[network][address] = {}
            # block
            if block not in network_token_blocks[network][address]:
                network_token_blocks[network][address][block] = 0

            # counter ( times encountered)
            network_token_blocks[network][address][block] += 1

    return network_token_blocks


def main(option: str, **kwargs):
    if option == "prices":
        repair_prices()
    if option == "database":
        check_database()
    if option == "hypervisor_status":
        repair_hypervisor_status()
    if option == "repair":
        repair_all()
    if option == "queue":
        repair_queue()
    if option == "reward_status":
        repair_rewards_status()
    if option == "operations":
        repair_operations()
    if option == "special":
        # used to check for special cases
        reScrape_database_prices(
            network_limit=CONFIGURATION["_custom_"]["cml_parameters"].maximum
        )
    # else:
    #     raise NotImplementedError(
    #         f" Can't find any action to be taken from {option} checks option"
    #     )
