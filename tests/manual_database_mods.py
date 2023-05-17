import copy
import sys
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import tqdm
import concurrent.futures


# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global, database_local

from apps.database_checker import (
    get_failed_prices_from_log,
    get_price,
    add_price_to_token,
    get_all_logfiles,
    repair_blocks,
)
from apps.database_feeder import feed_operations_hypervisors
from bins.database.db_user_status import user_status_hypervisor_builder
from bins.general.general_utilities import log_execution_time

from bins.w3.onchain_data_helper import onchain_data_helper2


def manual_set_prices_by_log(log_file: str | None = None):
    # define prices manually
    address_block_list = {}
    address_block_list["optimism"] = {
        "0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a": 0.99
    }
    # define qtty threshold to cosider manual price: how many number of blocks+address has been found in log
    qtty_threshold = 100

    network_token_blocks = read_logfile_regx(log_file)

    # loop query n save
    for network, data in network_token_blocks.items():
        # check if network is in list
        if network in address_block_list:
            for address, block_qtty_data in data.items():
                # check if address is in list
                if address in address_block_list[network]:
                    with tqdm.tqdm(total=len(block_qtty_data.keys())) as progress_bar:
                        for block, qtty in block_qtty_data.items():
                            # progress
                            progress_bar.set_description(
                                f" Processing {network} 0x..{address[:-4]}'s {block} block price"
                            )
                            progress_bar.update(0)

                            # check if block qtty threshold is reached
                            if qtty >= qtty_threshold:
                                manual_price = address_block_list[network][address]

                                # try get price from 3rd party
                                if price := get_price(
                                    network=network,
                                    token_address=address,
                                    block=int(block),
                                ):
                                    logging.getLogger(__name__).debug(
                                        f" Found price for {network}'s {address} at block {block}"
                                    )
                                else:
                                    logging.getLogger(__name__).debug(
                                        f" NOT found price for {network}'s {address} at block {block}. Adding manual price {manual_price}"
                                    )
                                    price = manual_price

                                # add price to database
                                add_price_to_token(
                                    network=network,
                                    token_address=address,
                                    block=block,
                                    price=price,
                                )

                            # update progress
                            progress_bar.update(1)


def manual_set_price_average_between_blocks():
    address_block_list = {}
    address_block_list["arbitrum"] = {
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(): [
            70182518,
            91006567,
            70164588,
        ],
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(): [70182518],
        "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(): [90714723, 90362324],
        "0xfa5ed56a203466cbbc2430a43c66b9d8723528e7".lower(): [90714723],
        "0x1a5b0aaf478bf1fda7b934c76e7692d722982a6d".lower(): [91006567],
        "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f".lower(): [90362324],
    }

    # find closest prices known to block
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    for network, data in address_block_list.items():
        for token_address, blocks in data.items():
            with tqdm.tqdm(total=len(blocks)) as progress_bar:
                for block in blocks:
                    # try get price from 3rd party
                    if price := get_price(
                        network=network,
                        token_address=token_address,
                        block=int(block),
                    ):
                        logging.getLogger(__name__).debug(
                            f" Found price for {network}'s {token_address} at block {block}"
                        )
                    else:
                        # get prices for address in database, close to block
                        closeprices = global_db_manager.get_price_usd_closestBlock(
                            network=network, block=block, address=token_address
                        )
                        # get average price
                        price = sum([p["price"] for p in closeprices]) / len(
                            closeprices
                        )
                        logging.getLogger(__name__).debug(
                            f" Found price {price} for {network}'s {token_address} at block {block} using average of {closeprices}"
                        )

                    # add price to database
                    add_price_to_token(
                        network=network,
                        token_address=token_address,
                        block=block,
                        price=price,
                    )

                # update progress
                progress_bar.update(1)


def manual_set_price_by_block():
    # TODO: parallel process

    # define prices manually
    address_block_list = {}
    address_block_list["arbitrum"] = {
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(): [
            {"block": 70182518, "price": 1800},
        ],
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(): [
            {"block": 41011508, "price": 0.99},
        ],
    }

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    for network, data in address_block_list.items():
        for token_address, block_price_data in data.items():
            # get all prices for address in database

            with tqdm.tqdm(total=len(block_price_data)) as progress_bar:
                for bpdata in block_price_data:
                    # progress
                    progress_bar.set_description(
                        f" Processing {network} 0x..{token_address[:-4]}'s {bpdata['block']} block price {bpdata['price']}"
                    )
                    progress_bar.update(0)
                    # update price to manual price
                    add_price_to_token(
                        network=network,
                        block=bpdata["block"],
                        token_address=token_address,
                        price=bpdata["price"],
                    )
                    # update progress
                    progress_bar.update(1)


def manual_set_price_all():
    # TODO: parallel process

    # define prices manually
    address_block_list = {}
    address_block_list["optimism"] = {
        "0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a".lower(): 0.99
    }

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    for network, data in address_block_list.items():
        for address, manual_price in data.items():
            # get all prices for address in database
            database_address_prices = global_db_manager.get_items_from_database(
                collection_name="usd_prices",
                find={"network": network, "address": address},
            )
            with tqdm.tqdm(total=len(database_address_prices)) as progress_bar:
                for price_dbObject in database_address_prices:
                    # progress
                    progress_bar.set_description(
                        f" Processing {network} 0x..{address[:-4]}'s {price_dbObject['block']} block price"
                    )
                    progress_bar.update(0)

                    # check % deviation from manual price
                    deviation = (
                        abs(price_dbObject["price"] - manual_price) / manual_price
                    )
                    if deviation > 0.15:
                        logging.getLogger(__name__).debug(
                            f" Found price for {network}'s {address} at block {price_dbObject['block']}. Price {price_dbObject['price']} has a deviation of more than 15% versus the manual price {manual_price}. Updating price to manual price"
                        )

                        # update price to manual price
                        add_price_to_token(
                            network=network,
                            block=price_dbObject["block"],
                            token_address=price_dbObject["address"],
                            price=manual_price,
                        )

                    # update progress
                    progress_bar.update(1)


@log_execution_time
def manual_get_missing_prices():
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    batch_size = 100000

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
                    return database_global(mongo_url=mongo_url, db_name=db_name)

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
                    collection_name="status", find={}, batch_size=batch_size
                ):
                    # add token addresses
                    price_ids_shouldBe.add(
                        f"{network}_{hype_status['block']}_{hype_status['pool']['token0']['address']}"
                    )
                    price_ids_shouldBe.add(
                        f"{network}_{hype_status['block']}_{hype_status['pool']['token1']['address']}"
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
                ):
                    # add token addresses
                    price_ids_shouldBe.add(
                        f"{network}_{rewarder_status['block']}_{rewarder_status['rewardToken']}"
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
                for id in database_global(mongo_url=mongo_url).get_items_from_database(
                    collection_name="usd_prices",
                    find={"network": network},
                    batch_size=batch_size,
                ):
                    # add token addresses
                    if id["id"] in price_ids_shouldBe:
                        price_ids_shouldBe.remove(id["id"])

                    # progress
                    progress_bar.set_description(
                        f" {network} should be prices: {len(price_ids_shouldBe)}"
                    )
                    progress_bar.update(0)

                if price_ids_shouldBe:
                    logging.getLogger(__name__).info(
                        f" Found {len(price_ids_shouldBe)} missing prices for {network}"
                    )
                else:
                    logging.getLogger(__name__).info(
                        f" No missing prices found for {network}"
                    )

                # progress
                progress_bar.update(1)


def read_logfile_regx(log_file: str | None = None):
    network_token_blocks = {}
    for log_file in [log_file] or get_all_logfiles():
        network_token_blocks.update(get_failed_prices_from_log(log_file=log_file))

    return network_token_blocks


def manual_set_rewarder_static_block_timestamp():
    """
    This function is used to manually set the block and timestamp for static rewarders using status rewarders first object found in the database
    """

    network = "arbitrum"
    protocol = "gamma"

    logging.getLogger(__name__).info(
        f"Setting {protocol}'s {network} rewarders static block and timestamp mannually to first known in rewarders status collection"
    )
    # get all 1st rewarder status from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    # get all rewarders
    for static_rewarder in local_db_manager.get_items_from_database(
        collection_name="rewards_static"
    ):
        # get first known block and timestamp from the reward status collection
        if reward_status := local_db_manager.get_items_from_database(
            collection_name="rewards_status",
            find={
                "hypervisor_address": static_rewarder["hypervisor_address"],
                "rewarder_address": static_rewarder["rewarder_address"],
            },
            sort=[("block", 1)],
            projection={"block": 1, "timestamp": 1},
            limit=1,
        ):
            # update static rewarder with block and timestamp
            static_rewarder["block"] = reward_status[0]["block"]
            static_rewarder["timestamp"] = reward_status[0]["timestamp"]

            # update static rewarder in database
            local_db_manager.set_rewards_static(data=static_rewarder)


def manual_set_database_field():
    # variables
    network = "binance"
    protocol = "gamma"
    db_collection = "rewards_status"
    find = {"rewarder_registry": {"$exists": False}}
    field = "rewarder_registry"
    field_value = "0x3a1d0952809f4948d15ebce8d345962a282c4fcb".lower()

    #########
    logging.getLogger(__name__).info(
        f"Setting {protocol}'s {network} {field} database field from {db_collection} collection to {field_value}"
    )
    # control
    if field == "id":
        raise ValueError("id field is not allowed to be set manually")

    # get all 1st rewarder status from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    def construct_result(dbItem):
        try:
            # update rewarder field
            dbItem[field] = field_value

            # deleteme custom
            dbItem["hypervisor_address"] = dbItem["hypervisor_address"].lower()
            dbItem["rewardToken"] = dbItem["rewardToken"].lower()
            dbItem["rewarder_address"] = dbItem["rewarder_address"].lower()

            # update static rewarder in database ( new instance of database manager to avoid pymongo cursor error)
            database_local(mongo_url=mongo_url, db_name=db_name).save_item_to_database(
                data=dbItem, collection_name=db_collection
            )
            result = True
        except Exception as e:
            logging.getLogger(__name__).error(
                f"Failed to update {dbItem['hypervisor_address']} {dbItem['rewardToken']} {dbItem['rewarder_address']} {field} to {field_value}. Error: {e}"
            )
            result = False
        return result, dbItem

    _errors = 0
    # get all items from database
    if database_items := database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_items_from_database(
        collection_name=db_collection, find=find, projection={"_id": 0}
    ):
        with tqdm.tqdm(total=len(database_items)) as progress_bar:
            with concurrent.futures.ThreadPoolExecutor() as ex:
                for result, dbItem in ex.map(construct_result, database_items):
                    if not result:
                        _errors += 1

                    progress_bar.set_description(
                        f"""[er:{_errors}]  item block {dbItem['block']} """
                    )
                    progress_bar.refresh()
                    progress_bar.update(1)


def manual_feed_user_status():
    network = "polygon"
    protocol = "gamma"
    hypervisor_address = "0x3cc20a6795c4b57d9817399f68e83e71c8626580"

    hype_new = user_status_hypervisor_builder(
        hypervisor_address=hypervisor_address, network=network, protocol=protocol
    )
    try:
        hype_new._process_operations()
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while manually feeding user status of {network}'s  {hypervisor_address} -> error {e}"
        )


def manual_sync_databases(rewrite: bool = False):
    origin_mongo_url = "mongodb://localhost:27072"
    origin_db_name = "global"
    origin_collection_name = "usd_prices"
    destination_mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    destination_db_name = origin_db_name
    destination_collection_name = origin_collection_name

    batch_size = 100000

    def origin_db():
        return database_global(mongo_url=origin_mongo_url, db_name=origin_db_name)

    def destination_db():
        return database_global(
            mongo_url=destination_mongo_url, db_name=destination_db_name
        )

    # create a list of items not to be synced
    if origin_data := origin_db().get_items_from_database(
        collection_name=destination_collection_name, find={}, batch_size=batch_size
    ):
        # set items to be saved database
        origin_data_toSave = origin_data

        if not rewrite:
            origin_ids = [x["id"] for x in origin_data]
            # get destination ids
            if destination_ids := destination_db().get_items_from_database(
                collection_name=destination_collection_name,
                find={"id": {"$in": origin_ids}},
                projection={"id": 1, "_id": 0},
                batch_size=batch_size,
            ):
                destination_ids = [x["id"] for x in destination_ids]

                if difference := set(origin_ids) - set(destination_ids):
                    logging.getLogger(__name__).info(
                        f"Found {len(difference)} items to be synced"
                    )

                    # set items to be saved database
                    origin_data_toSave = [
                        x for x in origin_data if x["id"] in difference
                    ]
                else:
                    logging.getLogger(__name__).info(f" No items found to be synced")
                    origin_data_toSave = None
        else:
            logging.getLogger(__name__).debug(f" Rewriting {len(difference)} items")

        if origin_data_toSave:
            destination_db().replace_items_to_database(
                collection_name=destination_collection_name, data=origin_data_toSave
            )


def manual_set_lowercase_addresses_rewards_static(
    network: str, batch_size: int = 100000
):
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"

    # rewards static
    collection_name = "rewards_static"
    items_to_process = database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_items_from_database(
        collection_name=collection_name,
        find={},
        projection={"_id": 0},
        batch_size=batch_size,
    )
    _errors = 0
    logging.getLogger(__name__).info(
        f"Found {len(items_to_process)} items to process at database collection {collection_name}"
    )

    def loop(item) -> bool:
        try:
            # copy item to new item
            new_item = copy.deepcopy(item)

            new_item["hypervisor_address"] = item["hypervisor_address"].lower()
            new_item["rewardToken"] = item["rewardToken"].lower()
            new_item["rewarder_address"] = item["rewarder_address"].lower()
            new_item["rewarder_registry"] = item["rewarder_registry"].lower()
            new_item["id"] = item["id"].lower()

            # remove old item
            database_local(mongo_url=mongo_url, db_name=db_name).delete_item(
                collection_name=collection_name, item_id=item["id"]
            )
            # save new item
            database_local(mongo_url=mongo_url, db_name=db_name).save_item_to_database(
                collection_name=collection_name, data=new_item
            )

            return True

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error while processing item {item['id']} -> error {e}"
            )

            return False

    with tqdm.tqdm(total=len(items_to_process)) as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for result in ex.map(loop, items_to_process):
                if not result:
                    _errors += 1
                progress_bar.set_description(f"""[er:{_errors}]  """)
                progress_bar.update(1)


def manual_set_lowercase_addresses_rewards_status(
    network: str, batch_size: int = 100000
):
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"

    # rewards static
    collection_name = "rewards_status"
    items_to_process = database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_items_from_database(
        collection_name=collection_name,
        find={},
        projection={"_id": 0},
        batch_size=batch_size,
    )
    _errors = 0
    logging.getLogger(__name__).info(
        f"Found {len(items_to_process)} items to process at database collection {collection_name}"
    )

    def loop(item) -> bool:
        try:
            # copy item to new item
            new_item = copy.deepcopy(item)

            new_item["hypervisor_address"] = item["hypervisor_address"].lower()
            new_item["rewardToken"] = item["rewardToken"].lower()
            new_item["rewarder_address"] = item["rewarder_address"].lower()
            new_item["rewarder_registry"] = item["rewarder_registry"].lower()
            new_item["id"] = item["id"].lower()

            # remove old item
            database_local(mongo_url=mongo_url, db_name=db_name).delete_item(
                collection_name=collection_name, item_id=item["id"]
            )
            # save new item
            database_local(mongo_url=mongo_url, db_name=db_name).save_item_to_database(
                collection_name=collection_name, data=new_item
            )

            return True

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error while processing item {item['id']} -> error {e}"
            )

            return False

    with tqdm.tqdm(total=len(items_to_process)) as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for result in ex.map(loop, items_to_process):
                if not result:
                    _errors += 1
                progress_bar.set_description(f"""[er:{_errors}]  """)
                progress_bar.update(1)


if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    manual_set_lowercase_addresses_rewards_status(network="arbitrum")

    # end time log
    _timelapse = datetime.now(timezone.utc) - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} minutes to complete".format(_timelapse.total_seconds() / 60)
    )
    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
