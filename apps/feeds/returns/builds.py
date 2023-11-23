from decimal import Decimal
import logging
import time
from apps.feeds.operations import feed_operations
from apps.feeds.utils import filter_hypervisor_data_for_apr, get_hypervisor_data_for_apr
from apps.hypervisor_periods.returns.general import hypervisor_periods_returns
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
)
from bins.general.enums import Chain, Protocol, error_identity
from bins.general.general_utilities import create_chunks
from bins.w3.builders import (
    get_latest_block,
)

from .objects import period_yield_data


def feed_hypervisor_returns(
    chain: Chain, hypervisor_addresses: list[str] | None = None
):
    """Feed hypervisor returns from the specified chain and hypervisor addresses

    Args:
        chain (Chain):
        hypervisor_addresses (list[str]): list of hype addresses

    """

    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name} returns information"
    )

    if _last_returns_data_db := get_last_return_data_from_db(
        chain=chain, hypervisor_addresses=hypervisor_addresses
    ):
        # get addresses and blocks to feed
        for hypervisor_address, block_ini in _last_returns_data_db:
            # beguin using one block after the last one found in database
            # block_ini += 1

            # create chunks of blocks to feed data so that we don't overload the database
            # get chain latest block
            latest_block = get_latest_block(chain=chain)
            # define chunk size
            # chunk_size = 50000  # blocks
            # create chunks
            # chunks = create_chunks(min=block_ini, max=latest_block, chunk_size=chunk_size)

            # logging.getLogger(__name__).debug(
            #    f" {len(chunks)} chunks created to feed each hypervisor returns data so that the database does not overload"
            # )
            # get hypervisor returns for each chunk
            # for block_chunk_ini, block_chink_end in chunks:
            # logging.getLogger(__name__).debug(
            #     f" Feeding block chunk {block_chunk_ini} to {block_chink_end} for {chain.database_name}'s {hypervisor_address} hypervisor"
            # )
            # create yield data
            if period_yield_list := create_period_yields(
                chain=chain,
                hypervisor_address=hypervisor_address,
                block_ini=block_ini,
                block_end=latest_block,
            ):
                # convert to dict and save
                try:
                    _todict = [x.to_dict() for x in period_yield_list]
                    # save converted to dict results to database
                    save_hypervisor_returns_to_database(
                        chain=chain,
                        period_yield_list=_todict,
                    )
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Could not convert yield result to dictionary, so not saved -> {e}"
                    )

    else:
        logging.getLogger(__name__).info(
            f" No hypervisor returns to process for {chain.database_name}. "
        )


def save_hypervisor_returns_to_database(
    chain: Chain,
    period_yield_list: list[dict],
):
    # convert Decimals to Bson decimals

    # save all at once
    if db_return := get_default_localdb(
        network=chain.database_name
    ).set_hypervisor_return_bulk(
        data=[database_local.convert_decimal_to_d128(x) for x in period_yield_list]
    ):
        logging.getLogger(__name__).debug(
            f"     {chain.database_name} saved returns -> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
        )
    else:
        logging.getLogger(__name__).error(
            f"  database did not return anything while trying to save hypervisor returns to database for {chain.database_name}"
        )


def create_period_yields(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    try_solve_errors: bool = False,
) -> list[period_yield_data]:
    """Create a list of period yield data objects from the specified chain and hypervisor address

    Args:
        chain (Chain): network
        hypervisor_address (str): address
        timestamp_ini (int): initial timestamp
        timestamp_end (int): end timestamp
        block_ini (int): _description_
        block_end (int):
        try_solve_errors (bool, optional): try to solve errors. Defaults to False.

    Returns:
        list[period_yield_data]: _description_
    """

    # if no block nor timestamp is specified, get the all time data
    if not (block_ini or timestamp_ini):
        timestamp_ini = 0
        timestamp_end = int(time.time())

    # create helper
    return_helper = hypervisor_periods_returns(
        chain=chain, hypervisor_address=hypervisor_address
    )

    return return_helper.execute_processes_within_hypervisor_periods(
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        block_ini=block_ini,
        block_end=block_end,
        try_solve_errors=try_solve_errors,
    )


def get_last_return_data_from_db(
    chain: Chain, hypervisor_addresses: list[str] | None = None
) -> list[tuple[str, int]]:
    """Get the last end period found for this hypervisor in hypervisor returns
    using blocks

    Args:
        chain (Chain): network
        hypervisor_address (str): address

    Returns:
        list[tuple[str, int]]: list of tuples with the hypervisor address and the last end block found in hypervisor returns
    """
    batch_size = 50000
    result = []

    # get all hypervisors addresses
    hypervisors_static = {
        x["address"]: x["block"]
        for x in get_from_localdb(
            network=chain.database_name,
            collection="static",
            find={"address": {"$in": hypervisor_addresses}}
            if hypervisor_addresses
            else {},
            projection={"address": 1, "block": 1, "_id": 0},
        )
        if x["address"]
        not in CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get("gamma", {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(chain.database_name, [])
    }

    # get all hypes n max block at hypervisor returns database
    # get the last end block found in hypervisor returns for each hype in the specified list
    query = []
    if (
        _match := {"$match": {"address": {"$in": hypervisor_addresses}}}
        if hypervisor_addresses
        else {}
    ):
        query.append(_match)
    # the last end block found in hypervisor returns
    query.append(
        {"$group": {"_id": "$address", "end_block": {"$max": "$timeframe.end.block"}}}
    )
    hypervisors_in_returns = {
        x["_id"]: x["end_block"]
        for x in get_from_localdb(
            network=chain.database_name,
            collection="hypervisor_returns",
            aggregate=query,
            batch_size=batch_size,
        )
    }

    for address, block in hypervisors_static.items():
        if address in hypervisors_in_returns:
            result.append((address, hypervisors_in_returns[address]))
        else:
            result.append((address, block))

    # return
    return result
