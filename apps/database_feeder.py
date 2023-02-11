import os
import sys
import logging
import tqdm
import concurrent.futures
from datetime import datetime
from pathlib import Path

if __name__ == "__main__":
    # append parent directory pth
    CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
    PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
    sys.path.append(PARENT_FOLDER)


from bins.configuration import CONFIGURATION, HYPERVISOR_REGISTRIES
from bins.general.general_utilities import convert_string_datetime, differences
from bins.w3.onchain_data_helper import onchain_data_helper2
from bins.w3.onchain_utilities import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_registry,
)
from bins.database.common.db_collections_common import database_local, database_global
from bins.mixed.price_utilities import price_scraper


### Static ######################
def feed_hypervisor_static(
    protocol: str, network: str, dex: str, threaded: bool = True
):
    """

    Args:
        protocol (str):
        network (str):
    """

    logging.getLogger(__name__).info(
        f" Feeding {protocol}'s {network} hypervisors static information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # create global database manager
    global_db = database_global(mongo_url=mongo_url)

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # get hyp addresses from database
    logging.getLogger(__name__).debug(
        "   Retrieving {} hypervisors addresses from database".format(network)
    )
    hypervisor_addresses_db = local_db.get_distinct_items_from_database(
        collection_name="static", field="address"
    )

    # define a registry to pull data from
    gamma_registry_address = HYPERVISOR_REGISTRIES[dex][network]
    gamma_registry = gamma_hypervisor_registry(
        address=gamma_registry_address,
        network=network,
    )

    # get hyp addresses from chain
    logging.getLogger(__name__).info(
        "   Retrieving {} hypervisors addresses from registry".format(network)
    )
    hypervisor_addresses_registry = gamma_registry.get_hypervisors_addresses()

    # ini hyp addresses to process var
    hypervisor_addresses = list()

    # filter already scraped hypervisors
    for address in hypervisor_addresses_registry:
        if address.lower() in hypervisor_addresses_db:
            logging.getLogger(__name__).debug(
                f" 0x..{address[-4:]} hypervisor static info already in db"
            )
        else:
            hypervisor_addresses.append(address)

    with tqdm.tqdm(total=len(hypervisor_addresses), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    address,
                    network,
                    0,
                )
                for address in hypervisor_addresses
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(lambda p: create_db_hypervisor(*p), args):
                    if result:
                        # progress
                        progress_bar.set_description(
                            " {} processed ".format(result["address"])
                        )
                        # add hypervisor status to database
                        local_db.set_static(data=result)
                        # update progress
                        progress_bar.update(1)
        else:
            # get operations from database
            for address in hypervisor_addresses:
                progress_bar.set_description(" 0x..{} to be processed".format())
                result = create_db_hypervisor(
                    address=address,
                    network=network,
                    block=0,
                )

                if result:
                    # add hypervisor static data to database
                    local_db.set_static(data=result)

                # update progress
                progress_bar.update(1)


### Operations ######################
def feed_operations(
    protocol: str,
    network: str,
    dex: str,
    block_ini: int = None,
    block_end: int = None,
    date_ini: datetime = None,
    date_end: datetime = None,
):

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    filters = CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})

    # create global and local database managers
    global_db = database_global(mongo_url=mongo_url)
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # create a web3 protocol helper
    onchain_helper = onchain_data_helper2(protocol=protocol)

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
            tmp_w3 = onchain_helper.create_web3_provider(network)
            block_end = tmp_w3.eth.get_block("latest").number

        date_end = convert_string_datetime(date_end)

    # get hypervisor addresses from static database collection and compare them to current operations distinct addresses
    # to decide whether a full timeback query shall be made
    logging.getLogger(__name__).debug(
        "   Retrieving {} hypervisors addresses from database".format(network)
    )
    hypervisor_addresses = local_db.get_distinct_items_from_database(
        collection_name="static", field="address"
    )
    hypervisor_addresses_in_operations = local_db.get_distinct_items_from_database(
        collection_name="operations", field="address"
    )

    try:
        # try getting initial block as last found in database
        if not block_ini:
            block_ini = get_db_last_operation_block(protocol=protocol, network=network)
            logging.getLogger(__name__).debug(
                "   Setting initial block to {}, being the last block found in operations".format(
                    block_ini
                )
            )

            if len(hypervisor_addresses) > len(hypervisor_addresses_in_operations):
                diffs = differences(
                    hypervisor_addresses, hypervisor_addresses_in_operations
                )
                new_block_ini = block_ini - int(
                    block_ini * 0.01
                )  # 1% of blocks ini back time?
                logging.getLogger(__name__).debug(
                    f" {len(diffs)} new hypervisors found in static but not in operations collections. Force initial block {block_ini} back time at {new_block_ini}"
                )
                block_ini = new_block_ini

        # define block to scrape
        if not block_ini and not block_end:
            logging.getLogger(__name__).info(
                " Calculating {} blocks to be processed using dates from {:%Y-%m-%d %H:%M:%S} to {:%Y-%m-%d %H:%M:%S} ".format(
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

        if block_end < block_ini:
            raise ValueError(
                f" Initial block {block_ini} is higher than end block: {block_end}"
            )

        # OPERATIONS
        feed_operations_hypervisors(
            network=network,
            protocol=protocol,
            hypervisor_addresses=hypervisor_addresses,
            block_ini=block_ini,
            block_end=block_end,
            local_db=local_db,
        )

    except:
        logging.getLogger(__name__).exception(
            " Unexpected error while looping    .error: {}".format(sys.exc_info()[0])
        )


def feed_operations_hypervisors(
    network: str,
    protocol: str,
    hypervisor_addresses: list,
    block_ini: int,
    block_end: int,
    local_db: database_local,
):

    # set global protocol helper
    onchain_helper = onchain_data_helper2(protocol=protocol)

    logging.getLogger(__name__).info(
        "Feeding database with {}'s {} operations of {} hypervisors from blocks {} to {}".format(
            network, protocol, len(hypervisor_addresses), block_ini, block_end
        )
    )
    with tqdm.tqdm(total=100) as progress_bar:
        # create callback progress funtion
        def _update_progress(text, remaining=None, total=None):
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
            # set operation id (same hash has multiple operations)
            operation[
                "id"
            ] = f"""{operation["logIndex"]}_{operation["transactionHash"]}"""
            # lower case address ( to ease comparison )
            operation["address"] = operation["address"].lower()
            local_db.set_operation(data=operation)


def get_db_last_operation_block(protocol: str, network: str) -> int:
    """Get the last operation block from database

    Args:
        protocol (str):
        network (str):

    Returns:
        int: last block number or None if not found or error
    """
    # read last blocks from database
    try:
        # setup database manager
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{network}_{protocol}"
        local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

        block_list = sorted(
            local_db_manager.get_distinct_items_from_database(
                collection_name="operations", field="blockNumber"
            ),
            reverse=False,
        )

        return block_list[-1]
    except IndexError:
        logging.getLogger(__name__).debug(
            f" Unable to get last operation block bc no operations have been found for {network}'s {protocol} in db"
        )

    except:
        logging.getLogger(__name__).exception(
            " Unexpected error while quering db operations for latest block  error:{}".format(
                sys.exc_info()[0]
            )
        )
    return None


### Status ######################
def feed_hypervisor_status_db(protocol: str, network: str, threaded: bool = True):
    """Scrapes all operations block and block-1  hypervisor information

    Args:
        protocol (str):
        network (str):
    """

    logging.getLogger(__name__).info(
        f" Feeding {protocol}'s {network} hypervisors status information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # create global database manager
    global_db = database_global(mongo_url=mongo_url)

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # create a unique list of blocks addresses from database
    toProcess_block_address = dict()
    for x in local_db.get_unique_operations_blockAddress():
        # add operation addressBlock to be processed
        toProcess_block_address[f"""{x["address"]}_{x["block"]}"""] = x
        # add block -1
        toProcess_block_address[f"""{x["address"]}_{x["block"]-1}"""] = {
            "address": x["address"],
            "block": x["block"] - 1,
        }

    # get a list of blocks already processed
    processed_blocks = {
        f"""{x["address"]}_{x["block"]}""": x
        for x in local_db.get_unique_status_blockAddress()
    }

    logging.getLogger(__name__).debug(
        " Total address blocks {} ->  Already processed {} [{:,.0%}]".format(
            len(toProcess_block_address),
            len(processed_blocks),
            len(processed_blocks.keys()) / len(toProcess_block_address),
        )
    )
    # remove already processed blocks
    for k in processed_blocks.keys():
        toProcess_block_address.pop(k)

    with tqdm.tqdm(
        total=len(toProcess_block_address.keys()), leave=False
    ) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    item["address"],
                    network,
                    item["block"],
                )
                for item in toProcess_block_address.values()
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(lambda p: create_db_hypervisor(*p), args):
                    if result:
                        # progress
                        progress_bar.set_description(
                            " {} processed ".format(result["address"])
                        )
                        # add hypervisor status to database
                        local_db.set_status(data=result)
                        # update progress
                        progress_bar.update(1)
        else:
            # get operations from database
            for item in toProcess_block_address.values():
                progress_bar.set_description(
                    " 0x..{} at block {} to be processed".format(
                        item["address"][-4:], item["block"]
                    )
                )
                result = create_db_hypervisor(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                )
                if result:
                    # add hypervisor status to database
                    local_db.set_status(data=result)
                # update progress
                progress_bar.update(1)


def create_db_hypervisor(address: str, network: str, block: int) -> dict():

    try:
        hypervisor = gamma_hypervisor_cached(
            address=address, network=network, block=block
        )
        return hypervisor.as_dict()
    except:
        logging.getLogger(__name__).exception(
            f" Unexpected error while creating {network}'s hypervisor {address} at block {block}->    error:{sys.exc_info()[0]}"
        )

    return None


### Prices ######################
def feed_prices(protocol: str, network: str):
    """Feed database with prices of tokens found in status collection

    Args:
        protocol (str):
        network (str):
    """
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # create unique token list using db data from hypervisor status
    token_list = [x["_id"] for x in local_db_manager.get_unique_tokens()]
    # create unique block list using data from hypervisor status
    blocks_list = sorted(
        local_db_manager.get_distinct_items_from_database(
            collection_name="status", field="block"
        ),
        reverse=True,
    )
    # combine token block lists
    token_blocks = {token: blocks_list for token in token_list}

    # check already processed
    # TODO: checkalready processed token blocks prices and pop em from token_blocks var

    # create price helper
    logging.getLogger(__name__).info(
        "Get {}'s prices using {} database".format(network, db_name)
    )
    price_helper = price_scraper(
        cache=CONFIGURATION["cache"]["enabled"],
        cache_filename="uniswapv3_price_cache",
    )
    try:
        total_items_to_process = len(token_blocks) * len(
            token_blocks[next(iter(token_blocks))]
        )
    except:
        total_items_to_process = len(token_blocks)

    with tqdm.tqdm(total=total_items_to_process) as progress_bar:
        # loop blocks to gather info
        for token, blocks in token_blocks.items():
            # get prices
            for block in blocks:

                progress_bar.set_description(
                    f" Retrieving USD price of 0x..{token[-3:]} at block {block}"
                )
                progress_bar.refresh()
                try:
                    # get price
                    price_usd = price_helper.get_price(
                        network=network, token_id=token, block=block, of="USD"
                    )
                    # save price to database
                    global_db_manager.set_price_usd(
                        network=network,
                        block=block,
                        token_address=token,
                        price_usd=price_usd,
                    )
                except:
                    logging.getLogger(__name__).exception(
                        f"Unexpected error while geting {token} usd price at block {block}"
                    )

                # add one
                progress_bar.update(1)


def main(option="operations"):

    dex = "uniswap_v3"

    for protocol in CONFIGURATION["script"]["protocols"].keys():

        for network in CONFIGURATION["script"]["protocols"][protocol][
            "networks"
        ].keys():

            if option == "static":
                # feed database with static hypervisor info
                feed_hypervisor_static(protocol=protocol, network=network, dex=dex)

            elif option == "operations":
                # first feed static operations
                feed_hypervisor_static(protocol=protocol, network=network, dex=dex)
                # feed database with all operations from static hyprervisor addresses
                feed_operations(protocol=protocol, network=network, dex=dex)

            elif option == "status":
                # feed database with statuss from all operations
                feed_hypervisor_status_db(
                    protocol=protocol, network=network, threaded=True
                )
            elif option == "prices":
                # feed database with prices from all status
                feed_prices(protocol=protocol, network=network)


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

    main("status")

    # end time log
    _timelapse = datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} seconds to complete".format(_timelapse.total_seconds())
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
