import logging

import tqdm
from bins.database.common.database_ids import create_id_price

from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain
from bins.mixed.price_utilities import price_scraper


def check_status_prices(
    network: str, local_db_manager: database_local, global_db_manager: database_global
):
    """Check that all status tokens have usd prices

    Args:
        local_db_manager (database_local):
        global_db_manager (database_global):
    """
    # get all prices + address + block
    prices = {
        x["id"]
        for x in global_db_manager.get_unique_prices_addressBlock(network=network)
    }

    # get tokens and blocks present in database
    prices_todo = set()

    for x in local_db_manager.get_items_from_database(collection_name="status"):
        for i in [0, 1]:
            db_id = create_id_price(
                network=network,
                block=x["pool"][f"token{i}"]["block"],
                token_address=x["pool"][f"token{i}"]["address"],
            )

            if db_id not in prices:
                prices_todo.add(db_id)

    if prices_todo:
        logging.getLogger(__name__).warning(
            " Found {} token blocks without price, from a total of {} ({:,.1%})".format(
                len(prices_todo), len(prices), len(prices_todo) / len(prices)
            )
        )


def check_stable_prices(
    network: str, local_db_manager: database_local, global_db_manager: database_global
):
    """Search database for predefined stable tokens usd price devisations from 1
        and log it

    Args:
        network (str): _description_
        local_db_manager (database_local):
        global_db_manager (database_global):
    """
    logging.getLogger(__name__).debug(
        f" Seek deviations of {network}'s stable token usd prices from 1 usd"
    )

    stables_symbol_list = ["USDC", "USDT", "LUSD", "DAI"]
    stables = {
        x["pool"]["token0"]["symbol"]: x["pool"]["token0"]["address"]
        for x in local_db_manager.get_items_from_database(
            collection_name="static",
            find={"pool.token0.symbol": {"$in": stables_symbol_list}},
        )
    } | {
        x["pool"]["token1"]["symbol"]: x["pool"]["token1"]["address"]
        for x in local_db_manager.get_items_from_database(
            collection_name="static",
            find={"pool.token1.symbol": {"$in": stables_symbol_list}},
        )
    }

    # database ids var
    db_ids = []

    for x in global_db_manager.get_items_from_database(
        collection_name="usd_prices",
        find={"address": {"$in": list(stables.values())}, "network": network},
    ):
        # check if deviation from 1 is significative
        if abs(x["price"] - 1) > 0.3:
            logging.getLogger(__name__).warning(
                f" Stable {x['network']}'s {x['address']} usd price is {x['price']} at block {x['block']}"
            )
            # add id
            db_ids.append(x["_id"])

    if db_ids:
        logging.getLogger(__name__).warning(
            f" Error found in database '{global_db_manager._db_name}' collection 'usd_prices'  ids: {db_ids}"
        )


def list_tokens_without_price(chains: list[Chain] | None = None) -> list[dict]:
    """Get a list of tokens that can't get prices from by using the price_scraper (current configuration).

    Args:
        chains (list[Chain] | None, optional): list of chains to process. Defaults to All.

    Returns:
        list[dict]: [ {address:"0x000000000000", symbol:"xBTC", chain: Chain} ]
    """
    result = []
    chains = chains or list(Chain)

    with tqdm.tqdm(total=len(chains)) as progress_bar:
        for chain in chains:
            # get tokens from static collection
            static_hypes = get_from_localdb(
                network=chain.database_name, collection="static", find={}
            )
            if not static_hypes:
                logging.getLogger(__name__).warning(
                    f" No tokens found in {chain.database_name} static collection"
                )
                continue

            # create a list of unique tokens from the hypervisor['pool'] token0 and token1 fields
            tokens = {
                x["pool"]["token0"]["address"]: x["pool"]["token0"]["symbol"]
                for x in static_hypes
            } | {
                x["pool"]["token1"]["address"]: x["pool"]["token1"]["symbol"]
                for x in static_hypes
            }
            if not tokens:
                logging.getLogger(__name__).error(
                    f" No tokens found in {chain.database_name} static collection"
                )
                continue
            # try get prices for all those at current block
            price_helper = price_scraper(thegraph=False)

            for token_address, token_symbol in tokens.items():
                try:
                    _tmpPrice, _tmpSource = price_helper.get_price(
                        network=chain.database_name,
                        token_id=token_address,
                        block=0,
                    )
                    if not _tmpPrice:
                        result.append(
                            {
                                "address": token_address,
                                "symbol": token_symbol,
                                "chain": chain,
                            }
                        )
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Error getting price for {chain.database_name} {token_symbol} {token_address} -> {e}"
                    )

                progress_bar.set_description(
                    f" [no price:{len(result):,.0f}]. Checking {chain.fantasy_name}  {token_symbol}"
                )
                progress_bar.refresh()
            progress_bar.update(1)

    return result
