import os
import sys
import logging
import tqdm
import concurrent.futures
import re

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


# mod apps
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


def check_prices():

    try:
        # load log file
        log_file = logging.getLogger("price").handlers[0].baseFilename
        network_token_blocks = get_failed_prices_from_log(log_file=log_file)
        for network, addresses in network_token_blocks.items():
            for address, blocks in addresses.items():
                for block, counter in blocks.items():
                    # block is string
                    block = int(block)
                    # counter = number of times found in logs
                    price = get_price(
                        network=network, token_address=address, block=block
                    )
                    if price != 0:
                        logging.getLogger(__name__).debug(
                            f" Added price for {network}'s {address} at block {block}"
                        )
                        add_price_to_token(
                            network=network,
                            token_address=address,
                            block=block,
                            price=price,
                        )
                    else:
                        logging.getLogger(__name__).debug(
                            f" Could not find price for {network}'s {address} at block {block}"
                        )
    except:
        logging.getLogger(__name__).exception(
            " unexpected error checking prices from log"
        )


# helpers
def add_price_to_token(network: str, token_address: str, block: int, price: float):
    """force special price add to database:
     will create a field called "origin" with "manual" as value to be ableto identify at db

    Args:
        network (str):
        token_address (str):
        block (int):
        price (float):
    """

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    data = {
        "id": f"{network}_{block}_{token_address}",
        "network": network,
        "block": int(block),
        "address": token_address,
        "price": float(price),
        "origin": "manual",
    }

    global_db_manager.save_item_to_database(data=data, collection_name="usd_prices")


def get_price(network: str, token_address: str, block: int) -> float:

    price_helper = price_scraper(cache=False)

    return price_helper.get_price(network=network, token_id=token_address, block=block)


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

    # address_block_list[] = []

    # loop query n save
    for network, data in address_block_list.items():
        for address, blocks in data.items():
            for block in blocks:
                price = get_price(network=network, token_address=address, block=block)
                if price != 0:
                    logging.getLogger(__name__).debug(
                        f" Added price for {network}'s {address} at block {block}"
                    )
                    add_price_to_token(
                        network=network, token_address=address, block=block, price=price
                    )
                else:
                    logging.getLogger(__name__).debug(
                        f" Could not add price for {network}'s {address} at block {block}"
                    )


# checks
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
    for x in local_db_manager.get_items_from_database(collection_name=status):
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


def get_failed_prices_from_log(log_file: str) -> dict:
    """Search repeated network + address + block in logs

    Return: {  <network>: {<address>: {<block>:<counter>}}}

    """
    pricelog_regx = "\-\s\s(?P<network>.*)'s\stoken\s(?P<address>.*)\sprice\sat\sblock\s(?P<block>\d*)\snot\sfound"
    debug_regx = "No\sprice\sfor\s(?P<address>.*)\sat\sblock\s(?P<block>\d*).*\[(?P<network>.*)\s(?P<dex>.*)\]"
    # groups->  network, symbol, address, block

    # load file
    log_file_content = ""
    with open(log_file, encoding="utf8") as f:
        log_file_content = f.read()

    # set a var
    network_token_blocks = dict()

    for regx_txt in [pricelog_regx, debug_regx]:

        # search pattern
        matches = re.finditer(regx_txt, log_file_content)

        if matches:
            for match in matches:
                network = match.group("network")
                address = match.group("address")
                block = match.group("block")

                # network
                if not network in network_token_blocks.keys():
                    network_token_blocks[network] = dict()
                # address
                if not address in network_token_blocks[network].keys():
                    network_token_blocks[network][address] = dict()
                # block
                if not block in network_token_blocks[network][address].keys():
                    network_token_blocks[network][address][block] = 0

                # counter ( times encountered)
                network_token_blocks[network][address][block] += 1

    return network_token_blocks


def main(option: str, **kwargs):

    if option == "prices":
        check_prices()
    else:
        raise NotImplementedError(
            f" Can't find any action to be taken from {option} checks option"
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

    check_prices()
    auto_get_prices()

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
