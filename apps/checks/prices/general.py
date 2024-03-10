import logging
from bins.database.common.database_ids import create_id_price

from bins.database.common.db_collections_common import database_global, database_local


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
