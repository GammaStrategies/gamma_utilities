import os
import sys
import logging
import tqdm
import concurrent.futures
import contextlib
from datetime import datetime, date, timedelta
from pathlib import Path
from web3 import Web3
from web3.exceptions import ContractLogicError

# from croniter import croniter

from bins.configuration import (
    CONFIGURATION,
    STATIC_REGISTRY_ADDRESSES,
    RPC_URLS,
    add_to_memory,
    get_from_memory,
)
from bins.general.general_utilities import (
    convert_string_datetime,
    differences,
    log_time_passed,
)
from bins.w3.onchain_data_helper import onchain_data_helper2
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_zyberswap_cached,
    gamma_hypervisor_thena_cached,
    gamma_hypervisor_registry,
    gamma_masterchef_registry,
    gamma_masterchef_v1,
    gamma_masterchef_rewarder,
)
from bins.w3.onchain_utilities.basic import erc20_cached
from bins.w3.helpers import (
    build_hypervisor,
    build_hypervisor_registry,
    build_hypervisor_anyRpc,
    build_hypervisor_registry_anyRpc,
)

from bins.database.common.db_collections_common import (
    database_local,
    database_global,
    db_collections_common,
)
from bins.database.db_user_status import user_status_hypervisor_builder
from bins.database.db_raw_direct_info import direct_db_hypervisor_info
from bins.mixed.price_utilities import price_scraper

from bins.formulas.univ3_formulas import (
    sqrtPriceX96_to_price_float,
    sqrtPriceX96_to_price_float_v2,
)
from datetime import timezone


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
        f">Feeding {protocol}'s {network} {dex} hypervisors static information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_{protocol}")

    # hypervisor addresses to process
    hypervisor_addresses = _get_static_hypervisor_addresses_to_process(
        local_db=local_db, protocol=protocol, network=network, dex=dex, rewrite=rewrite
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
                for result in ex.map(
                    lambda p: create_db_hypervisor_publicRPCs(*p), args
                ):
                    if result:
                        # progress
                        progress_bar.set_description(
                            f' 0x..{result["address"][-4:]} processed '
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
                progress_bar.set_description(f" 0x..{address[-4:]} to be processed")
                progress_bar.refresh()
                if result := create_db_hypervisor_publicRPCs(
                    address=address,
                    network=network,
                    block=0,
                    dex=dex,
                    static_mode=True,
                ):
                    # add hypervisor static data to database
                    local_db.set_static(data=result)
                else:
                    # error found
                    _errors += 1

                # update progress
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisors could not be scraped due to errors".format(
                    _errors,
                    len(hypervisor_addresses),
                    _errors / len(hypervisor_addresses),
                )
            )


# TODO: change gamma_hypervisor_registry to generic
def _get_hypervisors_from_registry(
    gamma_registry: gamma_hypervisor_registry, protocol: str = "gamma"
) -> list:
    try:
        return _get_hypervisors_from_registry_worker(gamma_registry, protocol)

    except ValueError as err:
        logging.getLogger(__name__).error(
            f" Error while fetching hypes from {gamma_registry._network} registry   .error: {sys.exc_info()[0]}"
        )
        # return an empty hype address list
        return []


def _get_hypervisors_from_registry_worker(
    gamma_registry: gamma_hypervisor_registry, protocol
):
    # get hypes
    hypervisor_addresses_registry: list = gamma_registry.get_hypervisors_addresses()

    # apply filters
    filters: dict = (
        CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
    )
    hypes_not_included: list = [
        x.lower()
        for x in filters.get("hypervisors_not_included", {}).get(
            gamma_registry._network, []
        )
    ]

    logging.getLogger(__name__).debug(f"   excluding hypervisors: {hypes_not_included}")
    hypervisor_addresses_registry = [
        x for x in hypervisor_addresses_registry if x not in hypes_not_included
    ]

    return hypervisor_addresses_registry


def _get_static_hypervisor_addresses_to_process(
    local_db: database_local,
    protocol: str,
    network: str,
    dex: str,
    rewrite: bool = False,
) -> list:
    """Create a list of hypervisor addresses to be scraped to feed static database collection

    Args:
        local_db (database_local): _description_
        rewrite (bool, optional): _description_. Defaults to False.

    Returns:
        list:
    """
    # get hyp addresses from database
    logging.getLogger(__name__).debug(
        f"   Retrieving {network} hypervisors addresses from database"
    )
    hypervisor_addresses_database = local_db.get_distinct_items_from_database(
        collection_name="static", field="address"
    )
    # use public RPCs
    hypervisor_addresses_registry = _get_hypervisors_from_registry(
        gamma_registry=_build_hypervisor_registry(
            network=network, dex=dex, publicRPC=True
        )
    )

    result = []
    # rewrite all static info?
    if not rewrite:
        # filter already scraped hypervisors
        for address in hypervisor_addresses_registry:
            if address.lower() in hypervisor_addresses_database:
                logging.getLogger(__name__).debug(
                    f"   {address} hypervisor static info already in db"
                )
            else:
                result.append(address)
    else:
        result = hypervisor_addresses_registry
        logging.getLogger(__name__).debug(
            f"   Rewriting all hypervisors static information of {network}'s {protocol} {dex} "
        )

    return result


def _build_hypervisor_registry(
    network: str, dex: str, publicRPC: bool = False
) -> gamma_hypervisor_registry:
    address = (
        STATIC_REGISTRY_ADDRESSES.get(network, {}).get("hypervisors", {}).get(dex, None)
    )
    """ Will try to build a gamma_hypervisor_registry object using public RPCs. If it fails, it will use private RPCs

    Returns:
        gamma hype registry:
    """
    if publicRPC:
        for rpcURL in RPC_URLS[network]:
            try:
                registry = gamma_hypervisor_registry(
                    address=address,
                    network=network,
                    custom_web3Url=rpcURL,
                )

                # test if works
                registry._contract.functions.counter().call()

                # return working
                return registry
            except Exception as err:
                logging.getLogger(__name__).warning(
                    f"     RPC {rpcURL} could not be used. error: {err}"
                )

    logging.getLogger(__name__).warning(
        f"public RPC source could not be used for {network} hypervisors addresses retrieval. Using private's."
    )
    return gamma_hypervisor_registry(
        address=address,
        network=network,
    )


def feed_operations(
    protocol: str,
    network: str,
    block_ini: int | None = None,
    block_end: int | None = None,
    date_ini: datetime | None = None,
    date_end: datetime | None = None,
):
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors operations information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    filters = CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})

    # create local database manager
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
        f"   Retrieving {network} hypervisors addresses from database"
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
        x.lower() for x in filters.get("hypervisors_not_included", {}).get(network, [])
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
                f"   Setting initial block to {block_ini}, being the last block found in operations database collection"
            )

            # check if hypervisors in static collection are diff from operation's
            if hypervisor_addresses_in_operations and len(hypervisor_addresses) > len(
                hypervisor_addresses_in_operations
            ):
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

    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while looping    .error: {sys.exc_info()[0]}"
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

    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while quering db operations for latest block  error:{sys.exc_info()[0]}"
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

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # apply filters
    hypes_not_included: list = [
        x.lower()
        for x in (
            CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
        )
        .get("hypervisors_not_included", {})
        .get(network, [])
    ]
    logging.getLogger(__name__).debug(f"   excluding hypervisors: {hypes_not_included}")
    # get all static hypervisor info and convert it to dict
    static_info = {
        x["address"]: x
        for x in local_db.get_items(collection_name="static")
        if x["address"] not in hypes_not_included
    }

    # create a unique list of blocks addresses from database to be processed including:
    #       operation blocks and their block-1 relatives
    #       block every X min
    toProcess_block_address = {}
    for x in local_db.get_unique_operations_addressBlock(
        topics=["deposit", "withdraw", "zeroBurn", "rebalance"]
    ):
        # add operation addressBlock to be processed
        toProcess_block_address[f"""{x["address"]}_{x["block"]}"""] = {
            "address": x["address"],
            "block": x["block"],
            "fees_metadata": "ini",
        }
        # add block -1
        toProcess_block_address[f"""{x["address"]}_{x["block"]-1}"""] = {
            "address": x["address"],
            "block": x["block"] - 1,
            "fees_metadata": "end",
        }

    # create croniter
    # c_iter = croniter(expr_format="*/60 */2 * * *", start_time=from_datetime)
    # current_timestamp = c_iter.get_next(start_time=from_datetime.timestamp())

    # add latest block to all hypervisors every 20 min
    try:
        if (
            datetime.now(timezone.utc).timestamp()
            - local_db.get_max_field(collection="status", field="timestamp")[0]["max"]
        ) > 60 * 20:
            latest_block = (
                erc20_cached(
                    address="0x0000000000000000000000000000000000000000",
                    network=network,
                )
                ._w3.eth.get_block("latest")
                .number
            )

            logging.getLogger(__name__).debug(
                f" Adding the latest block [{latest_block}] to all addresses for status to be scraped "
            )

            for address in static_info:
                toProcess_block_address[f"""{address}_{latest_block}"""] = {
                    "address": address,
                    "block": latest_block,
                    "fees_metadata": "mid",
                }
    except IndexError:
        logging.getLogger(__name__).debug(
            f" Seems like there is no {network}'s {protocol} status data in db. Continue without adding latest block to all addresses for status to be scraped"
        )
    except Exception:
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
        f"   Total address blocks {len(toProcess_block_address)} ->  Already processed {len(processed_blocks)} "
    )

    # remove already processed blocks
    for k in processed_blocks:
        try:
            toProcess_block_address.pop(k)
        except KeyError as err:
            # there are many more status blocks than operations ...
            # not to worry
            # logging.getLogger(__name__).warning(
            #     f" Could not find status block address key {k} in operations"
            # )
            pass
        except Exception:
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
                    if result is None:
                        # error found
                        _errors += 1

                    else:
                        # progress
                        progress_bar.set_description(
                            f' {result.get("address", " ")} processed '
                        )
                        progress_bar.refresh()
                        # add hypervisor status to database
                        local_db.set_status(data=result)
                    # update progress
                    progress_bar.update(1)
        else:
            # get operations from database
            for item in toProcess_block_address.values():
                progress_bar.set_description(
                    f' 0x..{item.get("address", "    ")[-4:]} at block {item.get("block", "")} to be processed'
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

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisor status could not be scraped due to errors".format(
                    _errors,
                    len(toProcess_block_address),
                    (_errors / len(toProcess_block_address))
                    if toProcess_block_address
                    else 0,
                )
            )


def create_db_hypervisor(
    address: str,
    network: str,
    block: int,
    dex: str,
    static_mode=False,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
) -> dict():
    try:
        hypervisor = build_hypervisor(
            address=address,
            network=network,
            block=block,
            dex=dex,
            static_mode=static_mode,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            cached=True,
        )

        # return converted hypervisor
        return hypervisor.as_dict(convert_bint=True, static_mode=static_mode)

    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{sys.exc_info()[0]}"
        )

    return None


def create_db_hypervisor_publicRPCs(
    address: str,
    network: str,
    block: int,
    dex: str,
    static_mode=False,
) -> dict():
    try:
        hypervisor = build_hypervisor_anyRpc(
            network=network,
            dex=dex,
            block=block,
            hypervisor_address=address,
            rpcUrls=RPC_URLS[network],
            test=True,
            cached=False,
        )
        # return converted hypervisor
        return hypervisor.as_dict(convert_bint=True, static_mode=static_mode)
    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary using public RPC ->    error:{sys.exc_info()[0]}"
        )

    logging.getLogger(__name__).warning(
        f" Could not use a public RPC endpoint to get {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary. Using private."
    )
    return create_db_hypervisor(
        address=address, network=network, block=block, dex=dex, static_mode=static_mode
    )


### Prices ######################
def feed_prices(
    protocol: str,
    network: str,
    price_ids: set,
    rewrite: bool = False,
    threaded: bool = True,
    coingecko: bool = True,  # TODO: create configuration var
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

    # get already processed prices
    already_processed_prices = get_not_to_process_prices(
        global_db_manager=global_db_manager, network=network
    )

    # create items to process
    logging.getLogger(__name__).debug(
        "   Building a list of addresses and blocks to be scraped"
    )
    # list of usd price ids sorted by descending block number
    items_to_process = sorted(
        list(price_ids - already_processed_prices),
        key=lambda x: int(x.split("_")[1]),
        reverse=True,
    )

    if items_to_process:
        # create price helper
        logging.getLogger(__name__).debug(
            "   Get {}'s prices using {} database".format(network, db_name)
        )

        logging.getLogger(__name__).debug("   Force disable price cache ")
        price_helper = price_scraper(
            cache=False,
            cache_filename="uniswapv3_price_cache",
            coingecko=coingecko,
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
                except Exception:
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

        with contextlib.suppress(Exception):
            if _errors > 0:
                logging.getLogger(__name__).info(
                    "   {} of {} ({:,.1%}) address block prices could not be scraped due to errors".format(
                        _errors,
                        len(items_to_process),
                        (_errors / len(items_to_process)) if items_to_process else 0,
                    )
                )
        return True
    else:
        logging.getLogger(__name__).info(
            "   No new {}'s prices to process for {} database".format(network, db_name)
        )
        return False


def get_not_to_process_prices(global_db_manager: database_global, network: str) -> set:
    already_processed_prices = [
        x["id"]
        for x in global_db_manager.get_unique_prices_addressBlock(network=network)
    ]

    # get zero sqrtPriceX96 ( unsalvable errors found in the past)
    already_processed_prices += [
        f'{network}_{x["block"]}_{x["pool"]["token0"]["address"]}'
        for x in get_from_memory(key="zero_sqrtPriceX96")
    ]

    return set(already_processed_prices)


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
                    f'{network}_{status["block"]}_{status["pool"][f"token{i}"]["address"]}'
                )

    # combine token block lists
    return result


def create_tokenBlocks_allTokens(protocol: str, network: str) -> set:
    """create a total list of token addresses

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

    result = set()
    for status in local_db_manager.get_items_from_database(collection_name="status"):
        for i in [0, 1]:
            result.add(
                f'{network}_{status["block"]}_{status["pool"][f"token{i}"]["address"]}'
            )

    return result


def create_tokenBlocks_topTokens(protocol: str, network: str, limit: int = 5) -> set:
    """Create a list of blocks for each TOP token address ( and WETH )

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
    # force add WETH to list
    if "WETH" not in top_token_symbols:
        top_token_symbols.append("WETH")

    # get a list of all status with those top tokens + blocks
    return set(
        (
            [
                f'{network}_{x["pool"]["token1"]["block"]}_{x["pool"]["token1"]["address"]}'
                for x in local_db_manager.get_items(
                    collection_name="status",
                    find={"pool.token1.symbol": {"$in": top_token_symbols}},
                    sort=[("block", 1)],
                )
            ]
            + [
                f'{network}_{x["pool"]["token0"]["block"]}_{x["pool"]["token0"]["address"]}'
                for x in local_db_manager.get_items(
                    collection_name="status",
                    find={"pool.token0.symbol": {"$in": top_token_symbols}},
                    sort=[("block", 1)],
                )
            ]
        )
    )


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

    # get already processed prices
    already_processed_prices = get_not_to_process_prices(
        global_db_manager=global_db_manager, network=network
    )

    # get a list of the top used tokens1 symbols
    top_tokens = [x["symbol"] for x in local_db_manager.get_mostUsed_tokens1()]

    # get all hype status where token1 == top tokens1 and not already processed
    # avoid top_tokens at token0
    status_list = [
        x
        for x in local_db_manager.get_items(
            collection_name="status",
            find={
                "pool.token1.symbol": {"$in": top_tokens},
                "pool.token0.symbol": {"$nin": top_tokens},
            },
            sort=[("block", 1)],
        )
        if "{}_{}_{}".format(network, x["block"], x["pool"]["token0"]["address"])
        not in already_processed_prices
    ]

    # log errors
    _errors = 0

    with tqdm.tqdm(total=len(status_list)) as progress_bar:

        def loopme(status: dict):
            try:
                # get sqrtPriceX96 from algebra or uniswap
                if "slot0" in status["pool"]:
                    sqrtPriceX96 = int(status["pool"]["slot0"]["sqrtPriceX96"])
                elif "globalState" in status["pool"]:
                    sqrtPriceX96 = int(status["pool"]["globalState"]["sqrtPriceX96"])
                else:
                    raise ValueError("sqrtPriceX96 not found")

                # calc price
                price_token0 = sqrtPriceX96_to_price_float(
                    sqrtPriceX96=sqrtPriceX96,
                    token0_decimals=status["pool"]["token0"]["decimals"],
                    token1_decimals=status["pool"]["token1"]["decimals"],
                )

                # price0, price1 = sqrtPriceX96_to_price_float_v2(
                #     sqrtPriceX96=sqrtPriceX96,
                #     token0_decimals=status["pool"]["token0"]["decimals"],
                #     token1_decimals=status["pool"]["token1"]["decimals"],
                # )

                # get weth usd price
                usdPrice_token1 = global_db_manager.get_price_usd(
                    network=network,
                    block=status["block"],
                    address=status["pool"]["token1"]["address"],
                )

                # calc token usd price
                return (usdPrice_token1[0]["price"] * price_token0), status
            except IndexError:
                # usdPrice_token1 error
                logging.getLogger(__name__).error(
                    f""" Unexpected index error while calc. price for {network}'s {status["pool"]["token0"]["symbol"]} ({status["pool"]["token0"]["address"]}) at block {status["block"]} using token's database data {status["pool"]["token1"]["symbol"]} ({status["pool"]["token1"]["address"]})"""
                )
                logging.getLogger(__name__).error(
                    f" check ---> usdPrice_token1: {usdPrice_token1} | price_token0: {price_token0}"
                )
            except Exception:
                # error found
                logging.getLogger(__name__).exception(
                    f""" Unexpected error while calc. price for {network}'s {status["pool"]["token0"]["symbol"]} ({status["pool"]["token0"]["address"]}) at block {status["block"]} using token's database data {status["pool"]["token1"]["symbol"]} ({status["pool"]["token1"]["address"]})"""
                )
                logging.getLogger(__name__).debug(f" ---> Status: {status}")

            return None, status

        if threaded:
            # threaded
            with concurrent.futures.ThreadPoolExecutor() as ex:
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
                            # get sqrtPriceX96 from algebra or uniswap
                            if "slot0" in item["pool"]:
                                sqrtPriceX96 = int(
                                    item["pool"]["slot0"]["sqrtPriceX96"]
                                )
                            elif "globalState" in item["pool"]:
                                sqrtPriceX96 = int(
                                    item["pool"]["globalState"]["sqrtPriceX96"]
                                )

                            logging.getLogger(__name__).warning(
                                f""" Price for {network}'s {item["pool"]["token1"]["symbol"]} ({item["pool"]["token1"]["address"]}) is zero at block {item["block"]}  ( sqrtPriceX96 is {sqrtPriceX96})"""
                            )
                            if sqrtPriceX96 == 0:
                                # save address to memory so it does not get processed again
                                add_to_memory(key="zero_sqrtPriceX96", value=item)
                    else:
                        # error found
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

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) address block prices could not be scraped due to errors".format(
                    _errors,
                    len(status_list),
                    (_errors / len(status_list)) if status_list else 0,
                )
            )


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
    with contextlib.suppress(Exception):
        from_date = max(timestamps_indb)
    # define daily parameters
    day_in_seconds = 60 * 60 * 24
    total_days = int(
        (datetime.now(timezone.utc).timestamp() - from_date) / day_in_seconds
    )

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
    items_to_process = []
    for block in local_db_manager.get_distinct_items_from_database(
        collection_name="status", field="block"
    ):
        if block not in blocks_indb:
            items_to_process.append(block)

    # beguin processing
    with tqdm.tqdm(total=len(items_to_process)) as progress_bar:

        def _get_timestamp(block):
            try:
                # get timestamp
                return dummy_helper.timestampFromBlockNumber(block=block), block

            except Exception:
                logging.getLogger(__name__).exception(
                    f"Unexpected error while geting timestamp of block {block}"
                )
            return None

        if threaded:
            # threaded
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for timestamp, block in ex.map(_get_timestamp, items_to_process):
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
                except Exception:
                    logging.getLogger(__name__).exception(
                        f"Unexpected error while geting timestamp of block {item}"
                    )
                # add one
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        logging.getLogger(__name__).info(
            "   {} of {} ({:,.1%}) blocks could not be scraped due to errors".format(
                _errors,
                len(items_to_process),
                (_errors / len(items_to_process)) if items_to_process else 0,
            )
        )


### user Status #######################
def feed_user_status(network: str, protocol: str):
    addresses = get_hypervisor_addresses(network=network, protocol=protocol)
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} user status information for {len(addresses)} hypervisors"
    )

    for idx, address in enumerate(addresses):
        logging.getLogger(__name__).debug(
            f"   [{idx} of {len(addresses)}] Building {network}'s {address} user status"
        )

        hype_new = user_status_hypervisor_builder(
            hypervisor_address=address, network=network, protocol=protocol
        )

        try:
            hype_new._process_operations()
        except Exception:
            logging.getLogger(__name__).exception(
                f" Unexpected error while feeding user status of {network}'s  {address}"
            )


def get_hypervisor_addresses(network: str, protocol: str) -> list[str]:
    result = []
    # get database configuration
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    # get blacklisted hypervisors
    blacklisted = (
        CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get(protocol, {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(network, [])
    )
    # check n clean
    if blacklisted is None:
        blacklisted = []

    # retrieve all addresses from database
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    result = local_db_manager.get_distinct_items_from_database(
        collection_name="static", field="address"
    )

    # apply black list
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process before applying filters: {len(result)}"
    )
    # filter blacklisted
    result = [x for x in result if x not in blacklisted]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after applying filters: {len(result)}"
    )

    return result


### Masterchef Static #######################
def feed_masterchef_static(
    network: str | None = None, dex: str | None = None, protocol: str = "gamma"
):
    logging.getLogger(__name__).info(f">Feeding rewards static information")

    for network in [network] if network else STATIC_REGISTRY_ADDRESSES.keys():
        for dex in (
            [dex]
            if dex
            else STATIC_REGISTRY_ADDRESSES.get(network, {})
            .get("MasterChefV2Registry", {})
            .keys()
        ):
            logging.getLogger(__name__).info(
                f"   feeding {protocol}'s {network} rewards for {dex}"
            )
            # set local database name and create manager
            local_db = database_local(
                mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
                db_name=f"{network}_{protocol}",
            )

            # TODO: masterchef v1 registry

            # masterchef v2 registry
            address = STATIC_REGISTRY_ADDRESSES[network]["MasterChefV2Registry"][dex]

            # create masterchef registry
            registry = gamma_masterchef_registry(address, network)

            # get reward addresses from masterchef registry
            reward_registry_addresses = registry.get_masterchef_addresses()

            for registry_address in reward_registry_addresses:
                # create reward registry
                reward_registry = gamma_masterchef_v1(
                    address=registry_address, network=network
                )

                for i in range(reward_registry.poolLength):
                    # TODO: try catch exceptions and rise them for hypervisor_address
                    # get hypervisor address
                    hypervisor_address = reward_registry.lpToken(pid=i)

                    # TODO: how to scrape rid ?
                    for rid in range(100):
                        try:
                            # get reward address
                            rewarder_address = reward_registry.getRewarder(
                                pid=i, rid=rid
                            )

                            # get rewarder
                            rewarder = gamma_masterchef_rewarder(
                                address=rewarder_address, network=network
                            )

                            result = rewarder.as_dict(convert_bint=True)

                            # manually add hypervisor address to rewarder
                            result["hypervisor_address"] = hypervisor_address.lower()

                            # manually add dex
                            result["dex"] = dex

                            # save to database
                            local_db.set_rewards_static(data=result)

                        except ValueError:
                            # no more rid's
                            break
                        except Exception as e:
                            if rewarder_address:
                                logging.getLogger(__name__).exception(
                                    f"   Unexpected error while feeding db with rewarder {rewarder_address}. hype: {hypervisor_address}  . error:{e}"
                                )
                            else:
                                logging.getLogger(__name__).exception(
                                    f"   Unexpected error while feeding db with rewarders from {reward_registry_addresses} registry. hype: {hypervisor_address}  . error:{e}"
                                )
                            break


### gamma_db_v1 -> FastAPI
def feed_fastApi_impermanent(network: str, protocol: str):
    # TODO implement threaded
    threaded = True

    end_date = datetime.now(timezone.utc)
    days = [1, 7, 15, 30]  # all impermament datetimeframes

    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # create global database manager to save to gamma_db_v1
    gamma_db_v1 = db_collections_common(
        mongo_url=mongo_url,
        db_name="gamma_db_v1",
        db_collections={"impermanent": {"id": True}},
    )

    # get a list of hype addresses to process
    addresses = get_hypervisor_addresses(network=network, protocol=protocol)

    # construct items to process arguments
    args = (
        (
            address,
            d,
        )
        for d in days
        for address in addresses
    )

    # control var
    _errors = 1
    args_lenght = len(days) * len(addresses)

    with tqdm.tqdm(total=args_lenght) as progress_bar:

        def construct_result(address: str, days: int) -> dict:
            # create helper
            hype_helper = direct_db_hypervisor_info(
                hypervisor_address=address, network=network, protocol=protocol
            )
            return (
                hype_helper.get_impermanent_data(
                    ini_date=end_date - timedelta(days=days), end_date=end_date
                ),
                days,
            )

        if threaded:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result, days in ex.map(lambda p: construct_result(*p), args):
                    if result:
                        progress_bar.set_description(
                            f"""[er:{_errors}]  Saving impermanent loss data for {result[0]["symbol"]} at {days} days"""
                        )
                        progress_bar.refresh()

                        # create database data with result
                        data = {
                            "id": "{}_{}_{}".format(
                                network, result[0]["address"], days
                            ),
                            "network": network,
                            "address": result[0]["address"],
                            "symbol": result[0]["symbol"],
                            "period": days,
                            "data": result,
                        }

                        # convert all decimal data to float
                        data = gamma_db_v1.convert_decimal_to_float(data)

                        # manually add/replace item to database
                        gamma_db_v1.replace_item_to_database(
                            data=data, collection_name="impermanent"
                        )
                    else:
                        _errors += 1

                    # add one
                    progress_bar.update(1)

        else:
            for result, days in map(lambda p: construct_result(*p), args):
                raise NotImplementedError(" not threaded process is not implemented")

    # control log
    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) {}'s hypervisor/day impermanent data could not be scraped due to errors (totalSupply?)".format(
                    _errors,
                    args_lenght,
                    (_errors / args_lenght) if args_lenght > 0 else 0,
                    network,
                )
            )


####### main ###########


def main(option="operations"):
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            if option == "static":
                for dex in CONFIGURATION["script"]["protocols"][protocol]["networks"][
                    network
                ]:
                    # feed database with static hypervisor info
                    # todo: add rewrite data option to cmd
                    feed_hypervisor_static(
                        protocol=protocol,
                        network=network,
                        dex=dex,
                        rewrite=(
                            CONFIGURATION["_custom_"]["cml_parameters"].rewrite or True
                        ),
                    )

            elif option == "operations":
                for dex in CONFIGURATION["script"]["protocols"][protocol]["networks"][
                    network
                ]:
                    # first feed static operations
                    feed_hypervisor_static(protocol=protocol, network=network, dex=dex)

                # feed database with all operations from static hyprervisor addresses
                feed_operations(
                    protocol=protocol,
                    network=network,
                    date_ini=CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime,
                    date_end=CONFIGURATION["_custom_"]["cml_parameters"].end_datetime,
                    block_ini=CONFIGURATION["_custom_"]["cml_parameters"].ini_block,
                    block_end=CONFIGURATION["_custom_"]["cml_parameters"].end_block,
                )

            elif option == "status":
                # feed database with statuss from all operations
                feed_hypervisor_status(
                    protocol=protocol, network=network, threaded=True
                )
            elif option == "user_status":
                # feed database with user status
                feed_user_status(protocol=protocol, network=network)

            elif option == "prices":
                # feed database with prices from all status
                # feed_prices(protocol=protocol, network=network, token_blocks=self._create_tokenBlocks_allTokens(protocol=protocol, network=network))
                feed_prices(
                    protocol=protocol,
                    network=network,
                    token_blocks=create_tokenBlocks_topTokens(
                        protocol=protocol, network=network
                    ),
                )

            elif option == "rewards":
                feed_masterchef_static(protocol=protocol, network=network)

            elif option == "impermanent_v1":
                # feed fastAPI database with impermanent data
                feed_fastApi_impermanent(protocol=protocol, network=network)

            else:
                raise NotImplementedError(
                    f" Can't find an operation match for {option} "
                )
