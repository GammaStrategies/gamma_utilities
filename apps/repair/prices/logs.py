import logging
import os
import re

import tqdm
from apps.feeds.queue.queue_item import QueueItem
from apps.repair.prices.helpers import add_price_to_token, get_price

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_price
from bins.database.common.db_collections_common import database_global
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import queueItemType


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

        # how do we know the log specified address-network is correct?
        # we get hypervisor static and reward static tokens from database, per network
        valid_tokens_networks = {}
        for network in network_token_blocks.keys():
            if network not in valid_tokens_networks:
                valid_tokens_networks[network] = []
            # get hypervisor static tokens
            query = [
                {
                    "$project": {
                        "address": "$address",
                        "token0": "$pool.token0.address",
                        "token1": "$pool.token1.address",
                    }
                },
                {
                    "$lookup": {
                        "from": "rewards_static",
                        "let": {"parent_address": "$address"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$eq": [
                                            "$hypervisor_address",
                                            "$$parent_address",
                                        ],
                                    }
                                }
                            },
                            {"$project": {"token": "$rewardToken"}},
                        ],
                        "as": "reward_tokens",
                    }
                },
            ]
            for item in get_from_localdb(
                network=network, collection="static", aggregate=query
            ):
                # add to valid tokens if not already
                if item["token0"].lower() not in valid_tokens_networks[network]:
                    valid_tokens_networks[network].append(item["token0"].lower())
                if item["token1"].lower() not in valid_tokens_networks[network]:
                    valid_tokens_networks[network].append(item["token1"].lower())
                for reward_token in item["reward_tokens"]:
                    if (
                        reward_token["token"].lower()
                        not in valid_tokens_networks[network]
                    ):
                        valid_tokens_networks[network].append(
                            reward_token["token"].lower()
                        )

        # create ids to query database with, and filter out incorrect ones
        price_ids_to_search = [
            create_id_price(network, block, address)
            for network, addresses in network_token_blocks.items()
            for address, blocks_data in addresses.items()
            for block, counter in blocks_data.items()
            if address.lower() in valid_tokens_networks[network]
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

        # create a network/queue item list to add to queue when enabled
        to_queue_network_items = {}

        with tqdm.tqdm(total=len(network_token_blocks)) as progress_bar:
            for network, addresses in network_token_blocks.items():
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
                                # add network
                                if network not in to_queue_network_items:
                                    to_queue_network_items[network] = []
                                # add item
                                to_queue_network_items[network].append(
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
    if add_to_queue and len(to_queue_network_items):
        for network, to_queue_items in to_queue_network_items.items():
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


def get_all_logfiles(
    log_names: list = ["error", "debug", "price"],
    avoid_path_names: list = ["check", "analysis"],
) -> list:
    """get all logfiles from config or default"""

    logfiles = []

    check_logs = CONFIGURATION["_custom_"]["cml_parameters"].check_logs
    if not check_logs:
        # we shall use the logs directory saved in configuration file
        # one directory up from the current file
        if os.path.isdir(CONFIGURATION["logs"]["save_path"]):
            # get the root directory from path
            check_logs = [os.path.dirname(CONFIGURATION["logs"]["save_path"])]

    for logPath in check_logs:
        if os.path.isfile(logPath):
            logfiles.append(logPath)
        elif os.path.isdir(logPath):
            for root, dirs, files in os.walk(logPath):
                # avoid to load "check" related logs ( current app log)
                if any(name in root for name in avoid_path_names):
                    # loop to next
                    continue

                for file in files:
                    if file.endswith(".log") and (
                        any([x.lower() in file.lower() for x in log_names])
                        or len(log_names) == 0
                    ):
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
