import logging
from bins.config.current import BLOCKS_PER_SECOND

from bins.errors.general import ProcessingError
from bins.w3.builders import build_erc20_helper
from ..configuration import CONFIGURATION
from ..database.common.db_collections_common import database_global, database_local
from ..general.enums import Chain, error_identity, text_to_chain


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
    # TODO: except pymongo.errors.OperationFailure as e: and raise a Process error
    return get_default_localdb(network=network).get_items_from_database(
        collection_name=collection, **kwargs
    )


# PRICE HELPERS


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

    raise ProcessingError(
        chain=text_to_chain(network),
        item={"address": token_address, "block": block},
        identity=error_identity.PRICE_NOT_FOUND,
        action=f"scrape_price",
        message=f" No price for {token_address} on {network} at blocks {block}, {block+1} and {block-1} in database.",
    )
    # raise ValueError(
    #     f" No price for {token_address} on {network} at blocks {block}, {block+1} and {block-1} in database."
    # )


def get_price_from_db_extended(
    network: str,
    block: int,
    token_address: str,
    within_timeframe: int | None = None,
) -> float:
    """Get the price of a token at the specified block (+-2). OR
        Get the closest price from the specified block within a time window.

        Price database is used only.

    Args:
        network (str):
        block (int):
        token_address (str):
        within_timeframe (int, optional): in MINUTES
                    When defined, it will return the price of the closest block found within this timeframe minutes.
                    When not defined, it will return the price at the specified block+-2. Defaults to None.
    Returns:
        float:
    """
    # try get the prices from database
    if token_price := get_default_globaldb().get_price_usd(
        network=network, block=block, address=token_address
    ):
        return token_price[0]["price"]

    if not within_timeframe in [None, 0]:
        # get chain blocks per second
        tmp_erc20 = build_erc20_helper(chain=text_to_chain(network))
        blocks_per_second = BLOCKS_PER_SECOND.get(
            network, None
        ) or tmp_erc20.average_blockTime(blocksaway=tmp_erc20.block * 0.20)
        # save it to global variable
        if not network in BLOCKS_PER_SECOND:
            BLOCKS_PER_SECOND[network] = blocks_per_second

        # calculate the number of blocks to group as timespan
        blocks_away = int((within_timeframe * 60) / blocks_per_second)
    else:
        # default to 2 blocks away
        blocks_away = 2

    if token_price := get_default_globaldb().get_price_usd_closestBlock(
        network=network, block=block, address=token_address, limit=blocks_away
    ):
        logging.getLogger(__name__).warning(
            f" No price for {token_address} on {network} at block {block} has been found in database. Instead using price from block {token_price[0]['block']}"
        )
        return token_price[0]["price"]

    raise ProcessingError(
        chain=text_to_chain(network),
        item={"address": token_address, "block": block},
        identity=error_identity.PRICE_NOT_FOUND,
        action=f"scrape_price",
        message=f" No price for {token_address} on {network} at blocks {block}, +-{blocks_away} in database.",
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


def get_prices_from_db_usingBlockAddress(
    chain: Chain, blockAddress_list: list[str]
) -> dict:
    """Get prices for all tokens at the specified blocks using a blockAddress list ( <block>_<token address> )

    Args:
        chain (Chain): chain
        blockAddress_list (list[str]): list of srt(<block>_<token address>)

    Returns:
        dict: {
            srt(<block>_<token address>):price
        }
    """
    return {
        f"{x['block']}_{x['address']}": x["price"]
        for x in get_default_globaldb().get_items_from_database(
            collection_name="usd_prices",
            find=dict(
                id={"$in": [f"{chain.database_name}_{id}" for id in blockAddress_list]}
            ),
        )
    }


def get_prices_from_db_usingHypervisorStatus(
    chain: Chain, hypervisor_status_list: list[dict]
) -> dict:
    # build a block-address list of all tokens in hypervisor status list
    blockAddress_list = []
    for x in hypervisor_status_list:
        blockAddress_list.append(f"{x['block']}_{x['pool']['token0']['address']}")
        blockAddress_list.append(f"{x['block']}_{x['pool']['token1']['address']}")

    # get prices
    return get_prices_from_db_usingBlockAddress(
        chain=chain, blockAddress_list=blockAddress_list
    )


def get_latest_price_from_db(network: str, token_address: str) -> float:
    # try get the prices from database
    if token_price := get_default_globaldb().get_items_from_database(
        collection_name="current_usd_prices",
        find=dict(network=network, address=token_address),
        sort=[("block", -1)],
        limit=1,
    ):
        return token_price[0]["price"]


def get_latest_prices_from_db(
    network: str, token_addresses: list[str] | None = None
) -> dict[float]:
    find = (
        dict(network=network, address={"$in": token_addresses})
        if token_addresses
        else dict(network=network)
    )
    # try get the prices from database
    if token_prices := get_default_globaldb().get_items_from_database(
        collection_name="current_usd_prices",
        find=find,
    ):
        return {price["address"]: price["price"] for price in token_prices}
