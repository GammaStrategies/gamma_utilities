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
from bins.general.general_utilities import convert_string_datetime
from bins.w3.onchain_data_helper import onchain_data_helper2
from bins.w3.onchain_utilities import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_registry,
)
from apps.database_analysis import database_local, database_global


log = logging.getLogger(__name__)


def feed_operations(
    protocol: str,
    network: str,
    dex: str,
    date_ini: datetime = None,
    date_end: datetime = None,
):

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    filters = CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
    if not date_ini:
        date_ini = filters.get("force_timeframe", {}).get(
            "start_time", "2021-03-24T00:00:00"
        )

        date_ini = convert_string_datetime(date_ini)
    if not date_end:
        date_end = filters.get("force_timeframe", {}).get("end_time", "now")
        date_end = convert_string_datetime(date_end)

    log.info(
        " Feeding {}'s {} {} hypervisor operations from {:%Y-%m-%d %H:%M:%S} to {:%Y-%m-%d %H:%M:%S} ".format(
            network, protocol, dex, date_ini, date_end
        )
    )

    # create global database manager
    global_db = database_global(mongo_url=mongo_url)

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # set global web3 protocol helper
    onchain_helper = onchain_data_helper2(protocol=protocol)

    # define a registry to pull data from
    gamma_registry_address = HYPERVISOR_REGISTRIES[dex][network]
    gamma_registry = gamma_hypervisor_registry(
        address=gamma_registry_address,
        network=network,
    )
    try:
        log.info("   Retrieving {} hypervisors addresses from registry".format(network))
        hypervisor_addresses = gamma_registry.get_hypervisors_addresses()

        log.info("   Calculating {} blocks to be processed".format(network))
        block_ini, block_end = onchain_helper.get_custom_blockBounds(
            date_ini=date_ini,
            date_end=date_end,
            network=network,
            step="day",
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
        log.exception(
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

    log.info("Processing {} operations".format(network))
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


def feed_hypervisor_status_db(protocol: str, network: str, threaded: bool = True):
    """Scrapes all operations block and block-1  hypervisor information

    Args:
        protocol (str):
        network (str):
    """

    log.info(f" Feeding {protocol}'s {network} hypervisors status information")

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

    log.debug(
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
                for result in ex.map(lambda p: create_hypervisor(*p), args):
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
                result = create_hypervisor(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                )
                if result:
                    # add hypervisor status to database
                    local_db.set_status(data=result)
                # update progress
                progress_bar.update(1)


def create_hypervisor(address: str, network: str, block: int) -> dict():

    try:
        hypervisor = gamma_hypervisor_cached(
            address=address, network=network, block=block
        )
        return hypervisor.as_dict()
    except:
        log.exception(
            f" Unexpected error while creating {network}'s hypervisor {address} at block {block}->    error:{sys.exc_info()[0]}"
        )

    return None


def process_prices(
    configuration: dict,
    network: str,
    hypervisor_addresses: list,
    unique_blocks: list,
    unique_token_list: list,
):
    # create price helper
    log.info("Get {} prices".format(network))
    price_helper = price_utilities.price_scraper(
        cache=configuration["cache"]["enabled"],
        cache_filename="uniswapv3_price_cache",
        cache_folderName=configuration["cache"]["save_path"],
    )
    with tqdm.tqdm(total=len(unique_blocks)) as progress_bar:
        # loop blocks to gather info
        for block in unique_blocks:
            # get prices
            for token in unique_token_list:
                progress_bar.set_description(
                    f" Retrieving USD price of 0x..{token[-3:]}"
                )
                progress_bar.refresh()

                # get price
                price_usd = price_helper.get_price(
                    network=network, token_id=token, block=block, of="USD"
                )
                # save price to database
                global_db.set_price_usd(
                    network=network,
                    block=block,
                    token_address=token,
                    price_usd=price_usd,
                )

            # scrape status
            for hypervisor_id in hypervisor_addresses:
                progress_bar.set_description(
                    f" Saving  hypervisor {hypervisor_id[-4:]} status at block {block}"
                )
                progress_bar.refresh()
                # create hypervisor w3 obj
                hypervisor = gamma_hypervisor_cached(
                    address=hypervisor_id,
                    web3Provider=onchain_helper.create_web3_provider(network=network),
                    block=block,
                )
                hyp_dict = hypervisor.as_dict()
                hyp_dict["id"] = f"{block}_{hypervisor_id}"
                local_db.set_status(data=hyp_dict)

            # add one
            progress_bar.update(1)


def status(configuration: dict):

    # debug variables
    protocol = "gamma"
    dex = "uniswap_v3"
    mongo_url = configuration["sources"]["database"]["mongo_server_url"]

    # create global database
    global_db = database_global(mongo_url=mongo_url)

    # set global protocol helper
    onchain_helper = onchain_data_helper2(
        configuration=configuration, protocol=protocol
    )

    for network in configuration["script"]["protocols"][protocol]["networks"].keys():
        log.info("Processing {} status".format(network))

        # set local database helper
        db_name = f"{network}_{protocol}"
        local_db = database_local(mongo_url=mongo_url, db_name=db_name)

        # define a registry to pull data from
        gamma_registry_address = HYPERVISOR_REGISTRIES[dex][network]
        gamma_registry = gamma_hypervisor_registry(
            address=gamma_registry_address,
            web3Provider=onchain_helper.create_web3_provider(network=network),
        )

        for hypervisor in gamma_registry.get_hypervisors_generator():
            # save static data
            local_db.set_status(data=hypervisor.as_dict())


def main(options="operations"):

    dex = "uniswap_v3"

    for protocol in CONFIGURATION["script"]["protocols"].keys():

        for network in CONFIGURATION["script"]["protocols"][protocol][
            "networks"
        ].keys():

            if options == "operations":
                # feed database
                feed_operations(protocol=protocol, network=network, dex=dex)
            elif options == "status":

                feed_hypervisor_status_db(
                    protocol=protocol, network=network, threaded=True
                )


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    log.info(" Start {}   ----------------------> ".format(__module_name))
    # start time log
    _startime = datetime.utcnow()

    main("status")

    # end time log
    _timelapse = datetime.utcnow() - _startime
    log.info(" took {:,.2f} seconds to complete".format(_timelapse.total_seconds()))
    log.info(" Exit {}    <----------------------".format(__module_name))
