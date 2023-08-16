import logging
from ..configuration import CONFIGURATION
from ..database.common.db_collections_common import database_global, database_local
from ..general.enums import Chain


def get_default_localdb(network: str) -> database_local:
    return database_local(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
        db_name=f"{network}_gamma",
    )


def get_default_globaldb() -> database_global:
    return database_global(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
    )


def get_from_localdb(network: str, collection: str, **kwargs) -> list:
    """get data from a local database type

    Args:
        network (str):
        collection (str):

    Returns:
        list: result
    """
    return get_default_localdb(network=network).get_items_from_database(
        collection_name=collection, **kwargs
    )


def get_price_from_db(
    network: str,
    block: int,
    token_address: str,
) -> float:
    """
    Get the price of a token at a specific block from database
    May return price of block -1 +1 if not found at block

    Args:
        network (str):
        block (int):
        token_address (str):

    Returns:
        float: usd price of token at block
    """
    # try get the prices from database
    global_db = database_global(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
    )

    if token_price := global_db.get_price_usd(
        network=network, block=block, address=token_address
    ):
        return token_price[0]["price"]

    # if price not found, check if block+1 block-1 has price ( because there is a low probability to high difference)
    if token_price := global_db.get_price_usd(
        network=network, block=block + 1, address=token_address
    ):
        logging.getLogger(__name__).warning(
            f" No price for {token_address} on {network} at block {block} has been found in database. Instead using price from block {block+1}"
        )
        return token_price[0]["price"]

    elif token_price := global_db.get_price_usd(
        network=network, block=block - 1, address=token_address
    ):
        logging.getLogger(__name__).warning(
            f" No price for {token_address} on {network} at block {block} has been found in database. Instead using price from block {block-1}"
        )
        return token_price[0]["price"]

    raise ValueError(
        f" No price for {token_address} on {network} at blocks {block}, {block+1} and {block-1} in database."
    )


def get_prices_from_db(
    network: str,
    block: int,
    token_addresses: list[str],
) -> dict:
    # try get the prices from database
    if token_prices := get_default_globaldb().get_items_from_database(
        collection_name="usd_prices",
        find=dict(network=network, address={"$in": token_addresses}, block=block),
    ):
        return {price["address"]: price["price"] for price in token_prices}


def get_latest_price_from_db(network: str, token_address: str) -> float:
    # try get the prices from database
    if token_price := get_default_globaldb().get_items_from_database(
        collection_name="current_usd_prices",
        query=dict(network=network, address=token_address),
        sort=dict(block=-1),
        limit=1,
    ):
        return token_price[0]["price"]


def get_latest_prices_from_db(network: str, token_addresses: list[str]) -> dict:
    # try get the prices from database
    if token_prices := get_default_globaldb().get_items_from_database(
        collection_name="current_usd_prices",
        find=dict(network=network, address={"$in": token_addresses}),
    ):
        return {price["address"]: price["price"] for price in token_prices}
