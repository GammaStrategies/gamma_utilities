from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global
from bins.general.enums import databaseSource
from bins.mixed.price_utilities import price_scraper


def get_price(
    network: str, token_address: str, block: int
) -> tuple[float, databaseSource]:
    """get price of token at block
    Will return a tuple with price and source
    """
    if CONFIGURATION.get("sources", {}).get("coingeko_api_key", None):
        return price_scraper(
            cache=False,
            thegraph=False,
            coingecko=True,
            geckoterminal_sleepNretry=True,
            source_order=[
                databaseSource.CHAINLINK,
                databaseSource.ONCHAIN,
                databaseSource.COINGECKO,
                databaseSource.GECKOTERMINAL,
            ],
        ).get_price(network=network, token_id=token_address, block=block)
    else:
        return price_scraper(
            cache=False,
            thegraph=False,
            coingecko=True,
            geckoterminal_sleepNretry=True,
            source_order=[
                databaseSource.CHAINLINK,
                databaseSource.ONCHAIN,
                databaseSource.GECKOTERMINAL,
                databaseSource.COINGECKO,
            ],
        ).get_price(network=network, token_id=token_address, block=block)


def add_price_to_token(
    network: str, token_address: str, block: int, price: float, source: databaseSource
):
    """force special price add to database:
     will create a field called "origin" with "manual" as value to be ableto identify at db

    Args:
        network (str):
        token_address (str):
        block (int):
        price (float):
    """

    # setup database managers
    global_db_manager = database_global(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
    )

    global_db_manager.set_price_usd(
        network=network,
        block=block,
        token_address=token_address,
        price_usd=price,
        source=source,
    )


def get_price_of_token(network: str, token_address: str, block: int) -> float:
    """get price of token at block

    Args:
        network (str):
        token_address (str):
        block (int):

    Returns:
        float:
    """

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    # get price from database
    price = global_db_manager.get_price_usd(
        network=network, block=block, address=token_address
    )

    if price:
        return price[0]["price"]
    else:
        return 0.0
