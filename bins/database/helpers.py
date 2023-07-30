import logging
from ..configuration import CONFIGURATION
from ..database.common.db_collections_common import database_global, database_local
from ..general.enums import Chain


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
