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
)
from apps.database_feeder import feed_operations_hypervisors


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


def manual_set_price_by_block():
    # TODO: parallel process

    # define prices manually
    address_block_list = {}
    address_block_list["ethereum"] = {
        "0x967da4048cd07ab37855c090aaf366e4ce1b9f48".lower(): [
            {"block": 17202552, "price": 1213},
        ]
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


def read_logfile_regx(log_file: str | None = None):
    network_token_blocks = {}
    for log_file in [log_file] or get_all_logfiles():
        network_token_blocks.update(get_failed_prices_from_log(log_file=log_file))

    return network_token_blocks


def manual_set_rewarder_static_block_timestamp():
    """
    This function is used to manually set the block and timestamp for static rewarders using status rewarders first object found in the database
    """

    network = "binance"
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
    network = "arbitrum"
    protocol = "gamma"
    db_collection = "rewards_status"
    find = {}
    field = "rewarder_registry"
    field_value = "0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower()

    #########
    logging.getLogger(__name__).info(
        f"Setting {protocol}'s {network} {field} database field  to {field_value} mannually"
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


# def manual_feed_operations():

#     data_operations_to_feed = {
#         "polygon":{
#             "0xcbb7fae80e4f5c0cbfe1af7bb1f19692f9532cfa": 41151107,
#         },
#         "binance":
#         {
#             "0x31257f40e65585cc45fdabeb12002c25bc95ee80":27955595,
#             "0x69e8c26050daecf8e3b3334e3f612b70f8d40a4f":27955436,
#             "0x0f51bcc98bdb85141692310a4e3abf2e0c552eb4":27430815,
#             '0xb83f87ff5629c7e5ac9631095fbc9d06587b0f2c':27430778,
#             '0xb84b03a6a02246ef71bcf3dde343b0a7e693e2b4':27955490,
#             '0xb1a0e5fee652348a206d935985ae1e8a9182a245': ,
#             '0x9c3e0445559e6de1fe6391e8e018dca02b480836': ,
#             '0x87a4db5bcb99b73d6bf16e74374d292caf2bfcb3': ,
#             '0xc9e88650db57e409371052abe7248aa854013613': ,
#             '0xf2ba5122a1f2692c8785e0d3b10a99ac62475420': ,
#             '0x1d6b56dada36ff58a454a3f5cca3a3631f17e405': ,
#             '0xf937145b516cff1ea501cf1210832a5b7ea42c3a': ,
#             '0xa4d37759a64df0e2b246945e81b50af7628a275e': ,
#             '0x3bc5650d2afe11aeb805e230968018293befd561': ,
#             '0xc3f6f60b6c26925b64a6ee77d331a7d4c3fed08f': ,
#             '0x3513292a2e0e0c6fb0a82196d7ed8eb499fe5772': ,
#             '0xfb50f3240aab04c8e3634a1e1074709fb56b2762': ,
#             '0x60d0d9f18203745087806b69ac948b8be37cbe72': ,
#             '0x5c15842fcc12313c4f94dfb6fad1af3f989d33e9': ,
#             '0x3f8f3caeff393b1994a9968e835fd38ecba6c1be': ,
#             '0xa15c281339aecdec3a79f44254d7dfcc811ea310': ,
#             '0xdfba9e5af368bbf7ab92e68b09e05af3116f7fcf': ,
#             '0x0087ca4844cae94b1c51dec0f9434a6f92006af9': ,
#             '0xfaeaa34ef2102520e2854721a4b00136c1fdead0': ,

#         },
#     }

#     feed_operations_hypervisors()

if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    manual_set_database_field()

    # end time log
    _timelapse = datetime.now(timezone.utc) - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} minutes to complete".format(_timelapse.total_seconds() / 60)
    )
    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
