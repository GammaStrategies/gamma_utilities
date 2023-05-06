import sys
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import tqdm


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


if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    manual_set_prices_by_log("logs/price.log")

    # end time log
    _timelapse = datetime.now(timezone.utc) - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} minutes to complete".format(_timelapse.total_seconds() / 60)
    )
    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
