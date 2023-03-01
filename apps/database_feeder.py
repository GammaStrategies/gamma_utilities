import os
import sys
import logging
import tqdm
import concurrent.futures
from datetime import datetime
from datetime import date
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

### Static ######################
def feed_hypervisor_static(
    protocol: str, network: str, dex: str, rewrite: bool = False, threaded: bool = True
):
    """Save hypervisor static data using web3 calls from a hypervisor's registry

    Args:
        protocol (str):
        network (str):
        dex (str):
        rewrite (bool): Force rewrite all hypervisors found
        threaded (bool):
    """

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors static information"
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
        "   Retrieving {}'s {} {} hypervisors addresses from registry".format(
            network, protocol, dex
        )
    )
    try:
        # get hypes
        hypervisor_addresses_registry: list = gamma_registry.get_hypervisors_addresses()
        # apply filters
        filters: dict = (
            CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
        )
        hypes_not_included: list = [
            x.lower()
            for x in filters.get("hypervisors_not_included", {}).get(network, list())
        ]

        logging.getLogger(__name__).debug(
            f"   excluding hypervisors: {hypes_not_included}"
        )
        hypervisor_addresses_registry = [
            x for x in hypervisor_addresses_registry if x not in hypes_not_included
        ]

    except ValueError as err:
        if "message" in err:
            logging.getLogger(__name__).error(
                f" Unexpected error while fetching hypes from {network} registry  error: {err['message']}"
            )
        else:
            logging.getLogger(__name__).error(
                f" Unexpected error while fetching hypes from {network} registry  error: {sys.exc_info()[0]}"
            )
        # return an empty hype address list
        hypervisor_addresses_registry = list()

    # ini hyp addresses to process var
    hypervisor_addresses = list()

    # rewrite all static info?
    if not rewrite:
        # filter already scraped hypervisors
        for address in hypervisor_addresses_registry:
            if address.lower() in hypervisor_addresses_db:
                logging.getLogger(__name__).debug(
                    f"   0x..{address[-4:]} hypervisor static info already in db"
                )
            else:
                hypervisor_addresses.append(address)
    else:
        hypervisor_addresses = hypervisor_addresses_registry
        logging.getLogger(__name__).debug(
            "   Rewriting all hypervisors static information of {}'s {} {} ".format(
                network, protocol, dex
            )
        )

    # set log list of hypervisors with errors
    _errors = 0
    with tqdm.tqdm(total=len(hypervisor_addresses), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (address, network, 0, dex, True) for address in hypervisor_addresses
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(lambda p: create_db_hypervisor(*p), args):
                    if result:
                        # progress
                        progress_bar.set_description(
                            " 0x..{} processed ".format(result["address"][-4:])
                        )
                        progress_bar.refresh()
                        # add hypervisor status to database
                        local_db.set_static(data=result)
                        # update progress
                        progress_bar.update(1)
                    else:
                        # error found
                        _errors += 1
        else:
            # get operations from database
            for address in hypervisor_addresses:
                progress_bar.set_description(
                    " 0x..{} to be processed".format(address[-4:])
                )
                progress_bar.refresh()
                result = create_db_hypervisor(
                    address=address,
                    network=network,
                    block=0,
                    dex=dex,
                    static_mode=True,
                )

                if result:
                    # add hypervisor static data to database
                    local_db.set_static(data=result)
                else:
                    # error found
                    _errors += 1

                # update progress
                progress_bar.update(1)

    try:
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisors could not be scraped due to errors".format(
                    _errors,
                    len(hypervisor_addresses),
                    _errors / len(hypervisor_addresses),
                )
            )
    except:
        pass


### Operations ######################
def feed_operations(
    protocol: str,
    network: str,
    block_ini: int = None,
    block_end: int = None,
    date_ini: datetime = None,
    date_end: datetime = None,
):

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors operations information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    filters = CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})

    # create global and local database managers
    global_db = database_global(mongo_url=mongo_url)
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # create a web3 protocol helper
    onchain_helper = onchain_data_helper2(protocol=protocol)

    # set timeframe to scrape as dates (used as last option)
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
    # apply filters
    filters: dict = (
        CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
    )
    hypes_not_included: list = [
        x.lower()
        for x in filters.get("hypervisors_not_included", {}).get(network, list())
    ]
    logging.getLogger(__name__).debug(f"   excluding hypervisors: {hypes_not_included}")
    hypervisor_addresses = [
        x for x in hypervisor_addresses if x not in hypes_not_included
    ]
    hypervisor_addresses_in_operations = [
        x for x in hypervisor_addresses_in_operations if x not in hypes_not_included
    ]

    try:
        # try getting initial block as last found in database
        if not block_ini:
            block_ini = get_db_last_operation_block(protocol=protocol, network=network)
            logging.getLogger(__name__).debug(
                "   Setting initial block to {}, being the last block found in operations".format(
                    block_ini
                )
            )

            # check if hypervisors in static collection are diff from operation's
            if len(hypervisor_addresses_in_operations) > 0 and len(
                hypervisor_addresses
            ) > len(hypervisor_addresses_in_operations):
                # get different addresses
                diffs = differences(
                    hypervisor_addresses, hypervisor_addresses_in_operations
                )
                # define a new initial block but traveling back time sufficienty to get missed ops
                # TODO: avoid hardcoded vars ( blocks back in time )
                new_block_ini = block_ini - int(block_ini * 0.005)
                logging.getLogger(__name__).debug(
                    f"   {len(diffs)} new hypervisors found in static but not in operations collections. Force initial block {block_ini} back time at {new_block_ini} [{block_ini-new_block_ini} blocks]"
                )
                logging.getLogger(__name__).debug(f"   new hypervisors-->  {diffs}")
                # set initial block
                block_ini = new_block_ini

        # define block to scrape
        if not block_ini and not block_end:
            logging.getLogger(__name__).info(
                "   Calculating {} blocks to be processed using dates from {:%Y-%m-%d %H:%M:%S} to {:%Y-%m-%d %H:%M:%S} ".format(
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

        # check for block range inconsistency
        if block_end < block_ini:
            raise ValueError(
                f" Initial block {block_ini} is higher than end block: {block_end}"
            )

        # feed operations
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
        "   Feeding database with {}'s {} operations of {} hypervisors from blocks {} to {}".format(
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
def feed_hypervisor_status(
    protocol: str, network: str, rewrite: bool = False, threaded: bool = True
):
    """Scrapes all operations block and block-1  hypervisor information

    Args:
        protocol (str):
        network (str):
        rewrite (bool): rewrite all status
        threaded: (bool):
    """

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors status information {'[rewriting all]' if rewrite else ''}"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # create global database manager
    global_db = database_global(mongo_url=mongo_url)

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # get all static hypervisor info and convert it to dict
    static_info = {
        x["address"]: x for x in local_db.get_items(collection_name="static")
    }

    # create a unique list of blocks addresses from database to be processed including:
    #       operation blocks and their block-1 relatives
    #       block every 20 min
    toProcess_block_address = dict()
    for x in local_db.get_unique_operations_addressBlock():
        # add operation addressBlock to be processed
        toProcess_block_address[f"""{x["address"]}_{x["block"]}"""] = x
        # add block -1
        toProcess_block_address[f"""{x["address"]}_{x["block"]-1}"""] = {
            "address": x["address"],
            "block": x["block"] - 1,
        }

    # add latest block to all hypervisors every 20 min
    try:
        if datetime.utcnow().timestamp() - local_db.get_max_field(
            collection="status", field="timestamp"
        )[0]["max"] > (60 * 20):

            latest_block = (
                erc20_cached(
                    address="0x0000000000000000000000000000000000000000",
                    network=network,
                )
                ._w3.eth.get_block("latest")
                .number
            )

            for address in static_info.keys():
                toProcess_block_address[f"""{address}_{latest_block}"""] = {
                    "address": address,
                    "block": latest_block,
                }
    except:
        logging.getLogger(__name__).exception(
            " unexpected error while adding new blocks to status scrape process "
        )

    if rewrite:
        # rewrite all address blocks
        processed_blocks = {}
    else:
        # get a list of blocks already processed
        processed_blocks = {
            f"""{x["address"]}_{x["block"]}""": x
            for x in local_db.get_unique_status_addressBlock()
        }

    logging.getLogger(__name__).debug(
        "   Total address blocks {} ->  Already processed {} [{:,.0%}]".format(
            len(toProcess_block_address),
            len(processed_blocks),
            (len(processed_blocks.keys()) / len(toProcess_block_address))
            if len(toProcess_block_address) > 0
            else 0,
        )
    )
    # remove already processed blocks
    for k in processed_blocks.keys():
        try:
            toProcess_block_address.pop(k)
        except KeyError as err:
            # there are many more status blocks than operations ...
            # not to worry
            # logging.getLogger(__name__).warning(
            #     f" Could not find status block address key {k} in operations"
            # )
            pass
        except:
            logging.getLogger(__name__).exception(
                f" Unexpected error found while construction block addresses to feed db with hype status.  err-> {sys.exc_info()[0]}"
            )

    # set log list of hypervisors with errors
    _errors = 0

    with tqdm.tqdm(total=len(toProcess_block_address), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    item["address"],
                    network,
                    item["block"],
                    static_info[item["address"]]["dex"],
                    False,
                )
                for item in toProcess_block_address.values()
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(lambda p: create_db_hypervisor(*p), args):
                    if result != None:
                        # progress
                        progress_bar.set_description(
                            " {} processed ".format(result.get("address", " "))
                        )
                        progress_bar.refresh()
                        # add hypervisor status to database
                        local_db.set_status(data=result)
                    else:
                        # error found
                        _errors += 1

                    # update progress
                    progress_bar.update(1)
        else:
            # get operations from database
            for item in toProcess_block_address.values():
                progress_bar.set_description(
                    " 0x..{} at block {} to be processed".format(
                        item.get("address", "    ")[-4:], item.get("block", "")
                    )
                )
                progress_bar.refresh()
                result = create_db_hypervisor(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                    dex=static_info[item["address"]]["dex"],
                    static_mode=False,
                )
                if result != None:
                    # add hypervisor status to database
                    local_db.set_status(data=result)
                else:
                    # error found
                    _errors += 1
                # update progress
                progress_bar.update(1)

    try:
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisor status could not be scraped due to errors".format(
                    _errors,
                    len(toProcess_block_address),
                    (_errors / len(toProcess_block_address))
                    if len(toProcess_block_address) > 0
                    else 0,
                )
            )
    except:
        pass


def create_db_hypervisor(
    address: str, network: str, block: int, dex: str, static_mode=False
) -> dict():

    try:
        if dex == "uniswapv3":
            hypervisor = gamma_hypervisor_cached(
                address=address, network=network, block=block
            )
        elif dex == "quickswap":
            hypervisor = gamma_hypervisor_quickswap_cached(
                address=address, network=network, block=block
            )
        else:
            raise NotImplementedError(f" {dex} exchange has not been implemented yet")

        # return converted hypervisor
        return hypervisor.as_dict(convert_bint=True, static_mode=static_mode)

    # except ValueError as err:
    #     # most ususal error being  {'code': -32000, 'message': 'execution aborted (timeout = 5s)'}
    #     err_msg = err["message"] if "message" in err else err
    #     logging.getLogger(__name__).exception(
    #         f" Unexpected Value error while creating {network}'s hypervisor {address} at block {block}->   message:{err_msg}"
    #     )
    # except ContractLogicError as err:
    #     logging.getLogger(__name__).exception(
    #         f" Unexpected Web3 error placing call to {network}'s hypervisor {address} at block {block}->   message:{err}"
    #     )
    except:
        logging.getLogger(__name__).exception(
            f" Unexpected error while creating {network}'s hypervisor {address} [dex: {dex}] at block {block}->    error:{sys.exc_info()[0]}"
        )

    return None


### Prices ######################
def feed_prices(
    protocol: str,
    network: str,
    price_ids: set,
    rewrite: bool = False,
    threaded: bool = True,
):
    """Feed database with prices of tokens and blocks specified in token_blocks

    Args:
        protocol (str):
        network (str):
        price_ids (set): list of database ids to be scraped --> "<network>_<block>_<token address>"
    """
    logging.getLogger(__name__).info(f">Feeding {protocol}'s {network} token prices")

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # check already processed prices
    # using only set with database id
    already_processed_prices = set(
        [
            x["id"]
            for x in global_db_manager.get_unique_prices_addressBlock(network=network)
        ]
    )

    # create items to process
    logging.getLogger(__name__).debug(
        "   Building a list of addresses and blocks to be scraped"
    )
    items_to_process = list(price_ids - already_processed_prices)

    if len(items_to_process) > 0:
        # create price helper
        logging.getLogger(__name__).debug(
            "   Get {}'s prices using {} database".format(network, db_name)
        )

        logging.getLogger(__name__).debug("   Force disable price cache ")
        price_helper = price_scraper(
            cache=False,
            cache_filename="uniswapv3_price_cache",
        )
        # log errors
        _errors = 0

        with tqdm.tqdm(total=len(items_to_process)) as progress_bar:

            def loopme(db_id: str):
                """loopme

                Args:
                    network (str):
                    db_id (str): "<network>_<block>_<token address>"

                Returns:
                    tuple: price, token address, block
                """
                try:
                    tmp_var = db_id.split("_")
                    network = tmp_var[0]
                    block = int(tmp_var[1])
                    token = tmp_var[2]

                    # get price
                    return (
                        price_helper.get_price(
                            network=network, token_id=token, block=block, of="USD"
                        ),
                        token,
                        block,
                    )
                except:
                    logging.getLogger(__name__).exception(
                        f"Unexpected error while geting {token} usd price at block {block}"
                    )
                return None

            if threaded:
                # threaded
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                    for price_usd, token, block in ex.map(loopme, items_to_process):
                        if price_usd:
                            # progress
                            progress_bar.set_description(
                                f"[er:{_errors}] Retrieved USD price of 0x..{token[-3:]} at block {block}   "
                            )
                            progress_bar.refresh()
                            # add hypervisor status to database
                            # save price to database
                            global_db_manager.set_price_usd(
                                network=network,
                                block=block,
                                token_address=token,
                                price_usd=price_usd,
                            )
                        else:
                            # error found
                            _errors += 1

                        # update progress
                        progress_bar.update(1)
            else:
                # loop blocks to gather info
                for db_id in items_to_process:

                    price_usd, token, block = loopme(db_id)
                    progress_bar.set_description(
                        f"[er:{_errors}] Retrieving USD price of 0x..{token[-3:]} at block {block}"
                    )
                    progress_bar.refresh()
                    if price_usd:
                        # save price to database
                        global_db_manager.set_price_usd(
                            network=network,
                            block=block,
                            token_address=token,
                            price_usd=price_usd,
                        )
                    else:
                        # error found
                        _errors += 1

                    # add one
                    progress_bar.update(1)

        try:
            if _errors > 0:
                logging.getLogger(__name__).info(
                    "   {} of {} ({:,.1%}) address block prices could not be scraped due to errors".format(
                        _errors,
                        len(items_to_process),
                        (_errors / len(items_to_process))
                        if len(items_to_process) > 0
                        else 0,
                    )
                )
        except:
            pass

    else:
        logging.getLogger(__name__).info(
            "   No new {}'s prices to process for {} database".format(network, db_name)
        )


def create_tokenBlocks_allTokensButWeth(protocol: str, network: str) -> set:
    """create a list of token addresses where weth is not in the pair

    Args:
        protocol (str):
        network (str):

    Returns:
        dict:
    """
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    result = set()
    for status in local_db_manager.get_items_from_database(collection_name="status"):
        for i in [0, 1]:
            if status["pool"][f"token{i}"]["symbol"] != "WETH":
                result.add(
                    "{}_{}_{}".format(
                        network, status["block"], status["pool"][f"token{i}"]["address"]
                    )
                )

    # combine token block lists
    return result


def create_tokenBlocks_topTokens(protocol: str, network: str, limit: int = 5) -> set:
    """Create a list of blocks for each TOP token address

    Args:
        protocol (str):
        network (str):

    Returns:
        dict: {<token address>: <list of blocks>
    """
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # get most used token list
    top_token_symbols = [
        x["symbol"] for x in local_db_manager.get_mostUsed_tokens1(limit=limit)
    ]

    # get a list of all status with those top tokens + blocks
    return set(
        [
            "{}_{}_{}".format(
                network, x["pool"]["token1"]["block"], x["pool"]["token1"]["address"]
            )
            for x in local_db_manager.get_items(
                collection_name="status",
                find={"pool.token1.symbol": {"$in": top_token_symbols}},
                sort=[("block", 1)],
            )
        ]
    )

    # # create unique block list using data from hypervisor status
    # blocks_list = sorted(
    #     local_db_manager.get_distinct_items_from_database(
    #         collection_name="status", field="block"
    #     ),
    #     reverse=True,
    # )

    # # combine token block lists
    # return set(
    #     [
    #         "{}_{}_{}".format(network, block, token)
    #         for token in top_tokens
    #         for block in blocks_list
    #     ]
    # )


def feed_prices_force_sqrtPriceX96(protocol: str, network: str, threaded: bool = True):
    """Using global used known tokens like WETH, apply pools sqrtPriceX96 to
        get token pricess currently 0 or not found

    Args:
        protocol (str):
        network (str):
        rewrite (bool, optional): . Defaults to False.
        threaded (bool, optional): . Defaults to True.
    """
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} token prices [sqrtPriceX96]"
    )

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # check already processed prices
    already_processed_prices = set(
        [
            x["id"]
            for x in global_db_manager.get_unique_prices_addressBlock(network=network)
        ]
    )

    # get a list of the top used tokens1 symbols
    top_tokens = [x["symbol"] for x in local_db_manager.get_mostUsed_tokens1()]

    # get all hype status where token1 == top tokens1 and not already processed
    status_list = [
        x
        for x in local_db_manager.get_items(
            collection_name="status",
            find={"pool.token1.symbol": {"$in": top_tokens}},
            sort=[("block", 1)],
        )
        if not "{}_{}_{}".format(network, x["block"], x["pool"]["token0"]["address"])
        in already_processed_prices
    ]

    # log errors
    _errors = 0

    with tqdm.tqdm(total=len(status_list)) as progress_bar:

        def loopme(status: dict):
            try:
                # calc price
                price_token0 = sqrtPriceX96_to_price_float(
                    sqrtPriceX96=int(status["pool"]["slot0"]["sqrtPriceX96"]),
                    token0_decimals=status["pool"]["token0"]["decimals"],
                    token1_decimals=status["pool"]["token1"]["decimals"],
                )
                # get weth usd price
                usdPrice_token1 = global_db_manager.get_price_usd(
                    network=network,
                    block=status["block"],
                    address=status["pool"]["token1"]["address"],
                )
                # calc token pusd price
                return (usdPrice_token1[0]["price"] * price_token0), status

            except:
                pass

            return None, status

        if threaded:
            # threaded
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for price_usd, item in ex.map(loopme, status_list):
                    if price_usd != None:
                        if price_usd > 0:
                            # progress
                            progress_bar.set_description(
                                f"""[er:{_errors}]  Retrieved USD price of {item["pool"]["token0"]["symbol"]} at block {item["block"]}"""
                            )
                            progress_bar.refresh()
                            # add hypervisor status to database
                            # save price to database
                            global_db_manager.set_price_usd(
                                network=network,
                                block=item["block"],
                                token_address=item["pool"]["token0"]["address"],
                                price_usd=price_usd,
                            )
                        else:
                            logging.getLogger(__name__).warning(
                                f""" Price for {network}'s {item["pool"]["token0"]["symbol"]} ({item["pool"]["token0"]["address"]}) is zero at block {item["block"]}"""
                            )
                    else:
                        # error found
                        logging.getLogger(__name__).warning(
                            f""" No price for {network}'s {item["pool"]["token1"]["symbol"]} ({item["pool"]["token1"]["address"]}) was found in database at block {item["block"]}"""
                        )
                        _errors += 1

                    # update progress
                    progress_bar.update(1)
        else:
            for item in status_list:
                progress_bar.set_description(
                    f"""[er:{_errors}]  Retrieving USD price of {item["pool"]["token0"]["symbol"]} at block {item['block']}"""
                )
                progress_bar.refresh()
                # calc price
                price_usd, notuse = loopme(status=item)
                if price_usd != None:
                    if price_usd > 0:
                        # save to database
                        global_db_manager.set_price_usd(
                            network=network,
                            block=item["block"],
                            token_address=item["pool"]["token0"]["address"],
                            price_usd=price_usd,
                        )
                    else:
                        logging.getLogger(__name__).warning(
                            f""" Price for {network}'s {item["pool"]["token0"]["symbol"]} ({item["pool"]["token0"]["address"]}) is zero at block {item["block"]}"""
                        )
                else:
                    logging.getLogger(__name__).warning(
                        f""" No price for {network}'s {item["pool"]["token1"]["symbol"]} ({item["pool"]["token1"]["address"]}) was found in database at block {item["block"]}"""
                    )
                    _errors += 1

                # add one
                progress_bar.update(1)

    try:
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) address block prices could not be scraped due to errors".format(
                    _errors,
                    len(items_to_process),
                    (_errors / len(items_to_process))
                    if len(items_to_process) > 0
                    else 0,
                )
            )
    except:
        pass


### Blocks Timestamp #####################
def feed_blocks_timestamp(network: str):
    """ """

    logging.getLogger(__name__).info(
        f">Feeding {network} block <-> timestamp information"
    )
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    # get a list of timestamps already in the database
    timestamps_indb = [
        x["timestamp"]
        for x in global_db_manager.get_all_block_timestamp(network=network)
    ]

    # set initial  a list of timestamps to process
    from_date = datetime.timestamp(datetime(year=2021, month=3, day=1))
    try:
        from_date = max(timestamps_indb)
    except:
        # from_date shuld be still defined
        pass

    # define daily parameters
    day_in_seconds = 60 * 60 * 24
    total_days = int((datetime.utcnow().timestamp() - from_date) / day_in_seconds)

    # create a list of timestamps to process  (daily)
    timestamps = [from_date + day_in_seconds * idx for idx in range(total_days)]

    # create a dummy erc20 obj as helper ( use only web3wrap functions)
    dummy_helper = erc20_cached(
        address="0x0000000000000000000000000000000000000000", network=network
    )
    for timestamp in timestamps:
        # brute force search closest block numbers from datetime
        block = dummy_helper.blockNumberFromTimestamp(
            timestamp=timestamp,
            inexact_mode="after",
            eq_timestamp_position="first",
        )


def feed_timestamp_blocks(network: str, protocol: str, threaded: bool = True):
    """fill global blocks data using blocks from the status collection

    Args:
        network (str):
        protocol (str):
    """
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} timestamp <-> block information"
    )

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    # create a dummy object to use inherited func
    dummy_helper = erc20_cached(
        address="0x0000000000000000000000000000000000000000", network=network
    )

    # get a list of blocks already in the database
    blocks_indb = [
        x["block"] for x in global_db_manager.get_all_block_timestamp(network=network)
    ]
    # create a list of items to process
    items_to_process = list()
    for block in local_db_manager.get_distinct_items_from_database(
        collection_name="status", field="block"
    ):
        if not block in blocks_indb:
            items_to_process.append(block)

    # beguin processing
    with tqdm.tqdm(total=len(items_to_process)) as progress_bar:

        def _get_timestamp(block):
            try:
                # get timestamp
                return dummy_helper.timestampFromBlockNumber(block=block), block

            except:
                logging.getLogger(__name__).exception(
                    f"Unexpected error while geting timestamp of block {block}"
                )
            return None

        if threaded:
            # threaded
            args = ((item) for item in items_to_process)
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for timestamp, block in ex.map(lambda p: _get_timestamp(p), args):
                    if timestamp:
                        # progress
                        progress_bar.set_description(
                            f" Retrieved timestamp of block {block}"
                        )
                        progress_bar.refresh()
                        # save to database
                        global_db_manager.set_block(
                            network=network, block=block, timestamp=timestamp
                        )
                    else:
                        # error found
                        _errors += 1

                    # update progress
                    progress_bar.update(1)
        else:
            # loop blocks to gather info
            for item in items_to_process:

                progress_bar.set_description(f" Retrieving timestamp of block {item}")
                progress_bar.refresh()
                try:
                    # get price
                    timestamp = _get_timestamp(item)
                    if timestamp:
                        # save to database
                        global_db_manager.set_block(
                            network=network, block=item, timestamp=timestamp
                        )
                    else:
                        # error found
                        _errors += 1
                except:
                    logging.getLogger(__name__).exception(
                        f"Unexpected error while geting timestamp of block {item}"
                    )
                # add one
                progress_bar.update(1)

    try:
        logging.getLogger(__name__).info(
            "   {} of {} ({:,.1%}) blocks could not be scraped due to errors".format(
                _errors,
                len(items_to_process),
                (_errors / len(items_to_process)) if len(items_to_process) > 0 else 0,
            )
        )
    except:
        pass


def main(option="operations"):

    for protocol in CONFIGURATION["script"]["protocols"].keys():

        for network in CONFIGURATION["script"]["protocols"][protocol][
            "networks"
        ].keys():

            if option == "static":
                for dex in CONFIGURATION["script"]["protocols"][protocol]["networks"][
                    network
                ].keys():
                    # feed database with static hypervisor info
                    feed_hypervisor_static(
                        protocol=protocol, network=network, dex=dex, rewrite=True
                    )

            elif option == "operations":
                for dex in CONFIGURATION["script"]["protocols"][protocol]["networks"][
                    network
                ].keys():
                    # first feed static operations
                    feed_hypervisor_static(protocol=protocol, network=network, dex=dex)

                # feed database with all operations from static hyprervisor addresses
                feed_operations(protocol=protocol, network=network)

            elif option == "status":
                # feed database with statuss from all operations
                feed_hypervisor_status(
                    protocol=protocol, network=network, threaded=True
                )
            elif option == "prices":
                # feed database with prices from all status
                # feed_prices(protocol=protocol, network=network, token_blocks=self._create_tokenBlocks_allTokens(protocol=protocol, network=network))
                feed_prices(
                    protocol=protocol,
                    network=network,
                    token_blocks=self._create_tokenBlocks_topTokens(
                        protocol=protocol, network=network
                    ),
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

    main("static")

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
