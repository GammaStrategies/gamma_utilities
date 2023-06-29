import contextlib
from datetime import datetime
import logging
import tqdm
import concurrent.futures

from bins.configuration import CONFIGURATION, add_to_memory, get_from_memory
from bins.database.common.db_collections_common import database_global, database_local
from bins.formulas.dex_formulas import sqrtPriceX96_to_price_float
from bins.general.file_utilities import load_json
from bins.mixed.price_utilities import price_scraper


def feed_all_prices(network: str, max_prices: int | None = None):
    feed_prices(
        network=network,
        price_ids=create_tokenBlocks_all(network=network, limit=max_prices),
        use_not_to_process_prices=True,
        max_prices=max_prices,
    )


def feed_prices(
    network: str,
    price_ids: set,
    rewrite: bool = False,
    threaded: bool = True,
    coingecko: bool = True,  # TODO: create configuration var
    use_not_to_process_prices: bool = True,
    limit_not_to_process_prices: int | None = None,
    max_prices: int | None = None,
):
    """Feed database with prices of tokens and blocks specified in token_blocks

    Args:
        protocol (str):
        network (str):
        price_ids (set): list of database ids to be scraped --> "<network>_<block>_<token address>"



    """
    logging.getLogger(__name__).info(f">Feeding gamma's {network} token prices")

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"
    global_db_manager = database_global(mongo_url=mongo_url)

    # get already processed prices (or not)
    already_processed_prices = (
        get_already_processed_prices(network=network, limit=limit_not_to_process_prices)
        if use_not_to_process_prices
        else set()
    )

    # list of usd price ids sorted by descending block number
    if items_to_process := sorted(
        list(price_ids - already_processed_prices),
        key=lambda x: int(x.split("_")[1]),
        reverse=True,
    ):
        if max_prices and len(items_to_process) > max_prices:
            logging.getLogger(__name__).debug(
                f"   Limiting the scraping price list to {max_prices} prices ( from {len(items_to_process)})"
            )
            items_to_process = items_to_process[:max_prices]

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
                    _price, _source = price_helper.get_price(
                        network=network, token_id=token, block=block, of="USD"
                    )

                    return (
                        _price,
                        _source,
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
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    for price_usd, source, token, block in ex.map(
                        loopme, items_to_process
                    ):
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
                                source=source,
                            )
                        else:
                            # error found
                            _errors += 1

                        # update progress
                        progress_bar.update(1)
            else:
                # loop blocks to gather info
                for db_id in items_to_process:
                    price_usd, source, token, block = loopme(db_id)
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
                            source=source,
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


def get_already_processed_prices(network: str, limit: int | None = None) -> set[str]:
    """Get prices in database for a network
        Sorted by decreasing block number (from the most recent to past)

    Args:
        network (str): _description_
        limit (int | None, optional): . Defaults to No limit.

    Returns:
        set: strings <network>_<block>_<tokenX_address>
    """

    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    logging.getLogger(__name__).debug(
        f"   Getting {f'a maximum of {limit}' if limit else 'all'} {network}'s prices from database "
    )

    _processed_prices = [
        x["id"]
        for x in database_global(mongo_url=mongo_url).get_items_from_database(
            collection_name="usd_prices",
            find={"network": network, "price": {"$gt": 0}},
            projection={"id": 1, "_id": 0},
            sort=[("block", -1)],
            batch_size=10000,
            limit=limit,
        )
    ]

    # get zero sqrtPriceX96 ( unsalvable errors found in the past)
    _processed_prices += [
        f'{network}_{x["block"]}_{x["pool"]["token0"]["address"]}'
        for x in get_from_memory(key="zero_sqrtPriceX96")
    ]

    return set(_processed_prices)


def create_tokenBlocks_all(network: str, limit: int | None = None) -> set:
    hypes = create_tokenBlocks_allHypervisorTokens(network=network, limit=limit)
    rewarders = create_tokenBlocks_allRewardsTokens(network=network, limit=limit)
    return hypes.union(rewarders)


def create_tokenBlocks_allHypervisorTokens(
    network: str, limit: int | None = None
) -> set:
    """Create a list of token addresses and blocks to process price for
        Sorted by descending block number (from the most recent to past)
    Args:
        network (str):

    Returns:
        set:
    """
    logging.getLogger(__name__).debug(f"  Creating {network}'s hypervisors tokens list")
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    result = set()
    try:
        if hypervisor_status := local_db_manager.get_items_from_database(
            collection_name="status",
            find={},
            projection={"pool": 1, "_id": 0},
            sort=[("block", -1)],
            limit=limit,
            batch_size=50000,
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


def create_tokenBlocks_allRewardsTokens(network: str, limit: int | None = None) -> set:
    """Create a list of token addresses blocks of all rewards tokens
        Sorted by descending block number (from the most recent to past)
    Args:
        network (str):

    Returns:
        set: strings <network>_<block>_<tokenX_address>
    """
    logging.getLogger(__name__).debug(f"  Creating {network}'s rewarders tokens list")
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    batch_size = 50000

    return set(
        [
            f'{network}_{item["block"]}_{item["rewardToken"]}'
            for item in local_db_manager.get_items_from_database(
                collection_name="rewards_status",
                find={},
                projection={"rewardToken": 1, "block": 1},
                sort=[("block", -1)],
                limit=limit,
                batch_size=batch_size,
            )
        ]
    )


def feed_current_usd_prices(threaded: bool = True):
    """Feed current usd prices for all tokens specified in data/price_token_address.json file"""

    logging.getLogger(__name__).info(
        "Feeding current usd prices for all tokens specified in data/price_token_address.json file"
    )

    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db = database_global(mongo_url=mongo_url)
    price_helper = price_scraper(cache=False)

    def loopme(network, address) -> tuple[str, str, bool]:
        # get price
        price, source = price_helper.get_price(network=network, token_id=address)
        # save price
        if price:
            db.set_current_price_usd(
                network=network,
                token_address=address,
                price_usd=price,
                source=source,
            )

            return network, f"{address} saved", True

        return network, f"{address} - no price found", False

    # load token addresses by chain
    if token_addresses := load_json(filename="price_token_address", folder_path="data"):
        _errors = 0
        #
        args = [
            (network, address)
            for network, addresses in token_addresses.items()
            for address in addresses
        ]
        with tqdm.tqdm(total=len(args)) as progress_bar:
            #
            if threaded:
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    for network, address, result in ex.map(lambda p: loopme(*p), args):
                        if not result:
                            _errors += 1
                        # progress
                        progress_bar.set_description(
                            f" [errors: {_errors}] {network} USD price for {address}"
                        )
                        progress_bar.update(1)

            else:
                for network, address in args:
                    # get price
                    net, addr, result = loopme(network=network, address=address)

                    if not result:
                        _errors += 1
                    # progress
                    progress_bar.set_description(
                        f" [errors: {_errors}] {net} USD price for {addr}"
                    )
                    progress_bar.update(1)
        #
        logging.getLogger(__name__).info(
            f" {_errors} errors found while feeding current prices"
        )


# ##############################
#
# OLD TODO: check and clean
#
# ##############################


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
            collection_name="status",
            projection={"pool": 1, "_id": 0},
            sort=[("pool.block", -1)],
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
                sort=[("block", 1)],
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
    already_processed_prices = get_already_processed_prices(
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
