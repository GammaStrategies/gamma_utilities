import contextlib
from datetime import datetime
import logging
import random
import tqdm
import concurrent.futures
from bins.apis.coingecko_utilities import coingecko_price_helper

from bins.configuration import CONFIGURATION, add_to_memory, get_from_memory
from bins.database.common.database_ids import create_id_price
from bins.database.common.db_collections_common import database_global, database_local
from bins.formulas.dex_formulas import sqrtPriceX96_to_price_float
from bins.general.enums import Chain, databaseSource
from bins.general.file_utilities import load_json, save_json
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
            thegraph=False,
            geckoterminal_sleepNretry=True,
            source_order=[
                databaseSource.ONCHAIN,
                databaseSource.GECKOTERMINAL,
                databaseSource.COINGECKO,
            ]
            if coingecko
            else [databaseSource.ONCHAIN, databaseSource.GECKOTERMINAL],
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
        create_id_price(
            network=network,
            block=x["block"],
            token_address=x["pool"]["token0"]["address"],
        )
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
                    create_id_price(
                        network=network,
                        block=status["pool"]["block"],
                        token_address=status["pool"][f"token{i}"]["address"],
                    )
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
            create_id_price(
                network=network, block=item["block"], token_address=item["rewardToken"]
            )
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
