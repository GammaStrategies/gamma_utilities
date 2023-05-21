import os
import sys
import logging
import tqdm
import concurrent.futures
import contextlib
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from web3 import Web3
from web3.exceptions import ContractLogicError
from apps.feeds.status import feed_rewards_status

# from croniter import croniter

from bins.configuration import (
    CONFIGURATION,
    STATIC_REGISTRY_ADDRESSES,
    add_to_memory,
    get_from_memory,
)
from bins.general.general_utilities import (
    convert_string_datetime,
    differences,
    log_time_passed,
)
from bins.w3.onchain_data_helper import onchain_data_helper
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_zyberswap_cached,
    gamma_hypervisor_thena_cached,
    gamma_hypervisor_registry,
)
from bins.w3.onchain_utilities import rewarders

from bins.w3.onchain_utilities.basic import erc20_cached
from bins.w3.builders import (
    build_hypervisor,
)

from bins.database.common.db_collections_common import (
    database_local,
    database_global,
    db_collections_common,
)
from bins.database.db_user_status import user_status_hypervisor_builder
from bins.database.db_raw_direct_info import direct_db_hypervisor_info
from bins.mixed.price_utilities import price_scraper

from bins.formulas.dex_formulas import (
    sqrtPriceX96_to_price_float,
    sqrtPriceX96_to_price_float_v2,
)
from bins.formulas.apr import calculate_rewards_apr

from apps.feeds.static import feed_hypervisor_static, feed_rewards_static
from apps.feeds.users import feed_user_status, feed_user_operations
from apps.feeds.status import feed_hypervisor_status


### Operations ######################


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
    onchain_helper = onchain_data_helper(protocol=protocol)

    try:
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
                # tmp_w3 = onchain_helper.create_erc20_helper(network)
                block_end = (
                    onchain_helper.create_erc20_helper(network)
                    ._getBlockData("latest")
                    .number
                )

            date_end = convert_string_datetime(date_end)

        # apply filters
        hypes_not_included: list = [
            x.lower()
            for x in filters.get("hypervisors_not_included", {}).get(network, [])
        ]
        logging.getLogger(__name__).debug(
            f"   excluding hypervisors: {hypes_not_included}"
        )

        # get hypervisor addresses from static database collection and compare them to current operations distinct addresses
        # to decide whether a full timeback query shall be made
        logging.getLogger(__name__).debug(
            f"   Retrieving {network} hypervisors addresses from database"
        )
        hypervisor_static_in_database = {
            x["address"]: x
            for x in local_db.get_items_from_database(
                collection_name="static",
                find={"address": {"$nin": hypes_not_included}},
                projection={"address": 1, "block": 1, "timestamp": 1},
            )
            if x["address"] not in hypes_not_included
        }
        hypervisor_addresses = hypervisor_static_in_database.keys()
        hypervisor_addresses_in_operations = local_db.get_distinct_items_from_database(
            collection_name="operations",
            field="address",
            condition={"address": {"$nin": hypes_not_included}},
        )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"   Unexpected error preparing operations feed for {network}. Can't continue. : {e}"
        )
        # close operations feed
        return

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
                # get minimum block from the new hypervisors found
                new_block_ini = min(
                    [
                        v["block"]
                        for k, v in hypervisor_static_in_database.items()
                        if k in diffs
                    ]
                )
                new_block_ini = (
                    new_block_ini if new_block_ini < block_ini else block_ini
                )
                # TODO: avoid hardcoded vars ( blocks back in time )
                # new_block_ini = block_ini - int(block_ini * 0.005)
                logging.getLogger(__name__).info(
                    f"   {len(diffs)} new hypervisors found in static but not in operations collections. Force initial block {block_ini} back time at {new_block_ini} [{block_ini-new_block_ini} blocks]"
                )
                logging.getLogger(__name__).info(f"   new hypervisors-->  {diffs}")
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

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while searching {network} for operations  .error: {e}"
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
    onchain_helper = onchain_data_helper(protocol=protocol)

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


### Prices ######################
def feed_prices(
    protocol: str,
    network: str,
    price_ids: set,
    rewrite: bool = False,
    threaded: bool = True,
    coingecko: bool = True,  # TODO: create configuration var
    set_source: str = "auto",
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
    # local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    global_db_manager = database_global(mongo_url=mongo_url)

    # get already processed prices #TODO: limit to last 20000 blocks?
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
                                source=set_source,
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
                            source=set_source,
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
        for x in global_db_manager.get_items_from_database(
            collection_name="usd_prices",
            find={"network": network, "price": {"$gt": 0}},
            projection={"id": 1, "_id": 0},
        )
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

    try:
        return set(
            [
                f'{network}_{status["pool"]["block"]}_{status["pool"][f"token{i}"]["address"]}'
                for status in local_db_manager.get_items_from_database(
                    collection_name="status",
                    find={
                        "pool.token0.symbol": {"$ne": "WETH"},
                        "pool.token1.symbol": {"$ne": "WETH"},
                    },
                    projection={"pool": 1, "_id": 0},
                )
                for i in [0, 1]
                if status["pool"][f"token{i}"]["symbol"] != "WETH"
            ]
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Unexpected error while getting {network} {protocol} allTokensButWeth blocks"
        )

    return set()


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
    try:
        if hypervisor_status := local_db_manager.get_items_from_database(
            collection_name="status", projection={"pool": 1, "_id": 0}
        ):
            result = set(
                [
                    f'{network}_{status["pool"]["block"]}_{status["pool"][f"token{i}"]["address"]}'
                    for status in hypervisor_status
                    for i in [0, 1]
                ]
            )
    except Exception as e:
        logging.getLogger(__name__).error(
            f"  Unexpected error found while retrieving hypervisor status from {db_name} to create the allToken block list for price scraping. error-> {e}"
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

    # get most used token list
    top_token_symbols = [
        x["symbol"] for x in local_db_manager.get_mostUsed_tokens1(limit=limit)
    ]
    # force add WETH to list
    if "WETH" not in top_token_symbols:
        top_token_symbols.append("WETH")

    # get a list of all status with those top tokens + blocks
    return set(
        [
            f'{network}_{x["pool"]["token1"]["block"]}_{x["pool"]["token1"]["address"]}'
            for x in local_db_manager.get_items(
                collection_name="status",
                find={
                    "$or": [
                        {"pool.token0.symbol": {"$in": top_token_symbols}},
                        {"pool.token1.symbol": {"$in": top_token_symbols}},
                    ]
                },
                projection={"pool": 1, "_id": 0},
                sort=[("block", 1)],
            )
        ]
    )

    # return set(
    #     (
    #         [
    #             f'{network}_{x["pool"]["token1"]["block"]}_{x["pool"]["token1"]["address"]}'
    #             for x in local_db_manager.get_items(
    #                 collection_name="status",
    #                 find={"pool.token1.symbol": {"$in": top_token_symbols}},
    #                 projection={"pool": 1, "_id": 0},
    #                 sort=[("block", 1)],
    #             )
    #         ]
    #         + [
    #             f'{network}_{x["pool"]["token0"]["block"]}_{x["pool"]["token0"]["address"]}'
    #             for x in local_db_manager.get_items(
    #                 collection_name="status",
    #                 find={"pool.token0.symbol": {"$in": top_token_symbols}},
    #                 projection={"pool": 1, "_id": 0},
    #                 sort=[("block", 1)],
    #             )
    #         ]
    #     )
    # )


def create_tokenBlocks_rewards_DELETEME(protocol: str, network: str) -> set:
    """Create a list of token addresses blocks using static rewards token addresses and blocks from the status collection

    Args:
        protocol (str): _description_
        network (str): _description_

    Returns:
        set: _description_
    """
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    # get static rewards token addresses
    static_rewards = local_db_manager.get_items_from_database(
        collection_name="rewards_static"
    )

    # build a query to get all to be processed blocks for those static rewards
    static_rewards_hype_addresses = [x["hypervisor_address"] for x in static_rewards]
    _query = [
        {"$match": {"hypervisor_address": {"$in": static_rewards_hype_addresses}}},
        {
            "$project": {
                "hype_address": "$hypervisor_address",
                "rewardToken": "$rewardToken",
                "block": "$block",
            }
        },
        {
            "$lookup": {
                "from": "status",
                "let": {
                    "rew_hype_address": "$hype_address",
                    "rew_block": "$block",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$address", "$$rew_hype_address"]},
                                    {"$gte": ["$block", "$$rew_block"]},
                                ],
                            }
                        }
                    },
                    {"$unset": ["_id"]},
                    {"$project": {"block": "$block"}},
                ],
                "as": "status_blocks",
            }
        },
        {
            "$project": {
                "blocks": "$status_blocks.block",
                "hypervisor_address": "$hype_address",
                "rewardToken": "$rewardToken",
            }
        },
        {"$unset": ["_id"]},
    ]

    result = set(
        [
            f'{network}_{block}_{reward_block_todo["rewardToken"]}'
            for reward_block_todo in local_db_manager.query_items_from_database(
                collection_name="rewards_static", query=_query
            )
            for block in reward_block_todo["blocks"]
        ]
    )

    # return a list of network_block_tokenAddress
    return result


def create_tokenBlocks_rewards(protocol: str, network: str) -> set:
    """Create a list of token addresses blocks of all rewards tokens

    Args:
        protocol (str): _description_
        network (str): _description_

    Returns:
        set: _description_
    """
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    batch_size = 50000

    return set(
        [
            f'{network}_{item["block"]}_{item["rewardToken"]}'
            for item in local_db_manager.get_items_from_database(
                collection_name="rewards_status",
                find={},
                projection={"rewardToken": 1, "block": 1},
                batch_size=batch_size,
            )
        ]
    )


def feed_prices_force_sqrtPriceX96(
    protocol: str, network: str, threaded: bool = True, set_source: str = "sqrtPriceX96"
):
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
            projection={"block": 1, "pool": 1, "_id": 0},
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
                                source=set_source,
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
                            source=set_source,
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

    _errors = 0

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
            return None, block

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
        if items_to_process:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) blocks could not be scraped due to errors".format(
                    _errors,
                    len(items_to_process),
                    (_errors / len(items_to_process)) if items_to_process else 0,
                )
            )


### Rewards  #######################

# def feed_masterchef_static(
#     network: str | None = None, dex: str | None = None, protocol: str = "gamma"
# ):
#     logging.getLogger(__name__).info(f">Feeding rewards static information")

#     for network in [network] if network else STATIC_REGISTRY_ADDRESSES.keys():
#         for dex in (
#             [dex]
#             if dex
#             else STATIC_REGISTRY_ADDRESSES.get(network, {})
#             .get("MasterChefV2Registry", {})
#             .keys()
#         ):
#             logging.getLogger(__name__).info(
#                 f"   feeding {protocol}'s {network} rewards for {dex}"
#             )
#             # set local database name and create manager
#             local_db = database_local(
#                 mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
#                 db_name=f"{network}_{protocol}",
#             )

#             # TODO: masterchef v1 registry

#             # masterchef v2 registry
#             address = STATIC_REGISTRY_ADDRESSES[network]["MasterChefV2Registry"][dex]

#             # create masterchef registry
#             registry = rewarders.gamma_masterchef_registry(address, network)

#             # get reward addresses from masterchef registry
#             reward_registry_addresses = registry.get_masterchef_addresses()

#             for registry_address in reward_registry_addresses:
#                 # create reward registry
#                 reward_registry = rewarders.gamma_masterchef_v1(
#                     address=registry_address, network=network
#                 )

#                 for i in range(reward_registry.poolLength):
#                     # TODO: try catch exceptions and rise them for hypervisor_address
#                     # get hypervisor address
#                     hypervisor_address = reward_registry.lpToken(pid=i)

#                     # TODO: how to scrape rid ?
#                     for rid in range(100):
#                         try:
#                             # get reward address
#                             rewarder_address = reward_registry.getRewarder(
#                                 pid=i, rid=rid
#                             )

#                             # get rewarder
#                             rewarder = rewarders.gamma_masterchef_rewarder(
#                                 address=rewarder_address, network=network
#                             )

#                             result = rewarder.as_dict(convert_bint=True)

#                             # manually add hypervisor address to rewarder
#                             result["hypervisor_address"] = hypervisor_address.lower()

#                             # manually add dex
#                             result["dex"] = dex
#                             result["pid"] = i
#                             result["rid"] = rid

#                             # save to database
#                             local_db.set_rewards_static(data=result)

#                         except ValueError:
#                             # no more rid's
#                             break
#                         except Exception as e:
#                             if rewarder_address:
#                                 logging.getLogger(__name__).exception(
#                                     f"   Unexpected error while feeding db with rewarder {rewarder_address}. hype: {hypervisor_address}  . error:{e}"
#                                 )
#                             else:
#                                 logging.getLogger(__name__).exception(
#                                     f"   Unexpected error while feeding db with rewarders from {reward_registry_addresses} registry. hype: {hypervisor_address}  . error:{e}"
#                                 )
#                             break


### gamma_db_v1 -> FastAPI
# def feed_fastApi_impermanent(network: str, protocol: str):
#     # TODO implement threaded
#     threaded = True

#     end_date = datetime.now(timezone.utc)
#     days = [1, 7, 15, 30]  # all impermament datetimeframes

#     mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
#     # create global database manager to save to gamma_db_v1
#     gamma_db_v1 = db_collections_common(
#         mongo_url=mongo_url,
#         db_name="gamma_db_v1",
#         db_collections={"impermanent": {"id": True}},
#     )

#     # get a list of hype addresses to process
#     addresses = get_hypervisor_addresses(network=network, protocol=protocol)

#     # construct items to process arguments
#     args = (
#         (
#             address,
#             d,
#         )
#         for d in days
#         for address in addresses
#     )

#     # control var
#     _errors = 1
#     args_lenght = len(days) * len(addresses)

#     with tqdm.tqdm(total=args_lenght) as progress_bar:

#         def construct_result(address: str, days: int) -> dict:
#             # create helper
#             hype_helper = direct_db_hypervisor_info(
#                 hypervisor_address=address, network=network, protocol=protocol
#             )
#             return (
#                 hype_helper.get_impermanent_data(
#                     ini_date=end_date - timedelta(days=days), end_date=end_date
#                 ),
#                 days,
#             )

#         if threaded:
#             with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
#                 for result, days in ex.map(lambda p: construct_result(*p), args):
#                     if result:
#                         progress_bar.set_description(
#                             f"""[er:{_errors}]  Saving impermanent loss data for {result[0]["symbol"]} at {days} days"""
#                         )
#                         progress_bar.refresh()

#                         # create database data with result
#                         data = {
#                             "id": "{}_{}_{}".format(
#                                 network, result[0]["address"], days
#                             ),
#                             "network": network,
#                             "address": result[0]["address"],
#                             "symbol": result[0]["symbol"],
#                             "period": days,
#                             "data": result,
#                         }

#                         # convert all decimal data to float
#                         data = gamma_db_v1.convert_decimal_to_float(data)

#                         # manually add/replace item to database
#                         gamma_db_v1.replace_item_to_database(
#                             data=data, collection_name="impermanent"
#                         )
#                     else:
#                         _errors += 1

#                     # add one
#                     progress_bar.update(1)

#         else:
#             for result, days in map(lambda p: construct_result(*p), args):
#                 raise NotImplementedError(" not threaded process is not implemented")

#     # control log
#     with contextlib.suppress(Exception):
#         if _errors > 0:
#             logging.getLogger(__name__).info(
#                 "   {} of {} ({:,.1%}) {}'s hypervisor/day impermanent data could not be scraped due to errors (totalSupply?)".format(
#                     _errors,
#                     args_lenght,
#                     (_errors / args_lenght) if args_lenght > 0 else 0,
#                     network,
#                 )
#             )


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
                    feed_hypervisor_static(
                        protocol=protocol,
                        network=network,
                        dex=dex,
                        rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                    )

                    # feed rewarders static
                    feed_rewards_static(
                        network=network,
                        dex=dex,
                        protocol=protocol,
                        rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
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
                # feed status
                feed_hypervisor_status(
                    protocol=protocol, network=network, threaded=True
                )

                # feed rewards status
                feed_rewards_status(protocol=protocol, network=network)

            elif option == "user_status":
                # feed database with user status
                feed_user_operations(protocol=protocol, network=network)

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
                feed_rewards_static(protocol=protocol, network=network)

            # elif option == "impermanent_v1":
            #     # feed fastAPI database with impermanent data
            #     # feed_fastApi_impermanent(protocol=protocol, network=network)

            else:
                raise NotImplementedError(
                    f" Can't find an operation match for {option} "
                )
