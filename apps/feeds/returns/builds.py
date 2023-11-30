from multiprocessing import Pool
import logging
import time
from apps.errors.actions import process_error
from apps.feeds.operations import feed_operations
from apps.hypervisor_periods.returns.general import hypervisor_periods_returns
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
)
from bins.errors.general import ProcessingError
from bins.general.enums import Chain, Protocol, error_identity
from bins.general.general_utilities import create_chunks
from bins.w3.builders import (
    get_latest_block,
)

from .objects import period_yield_data


def feed_hypervisor_returns(
    chain: Chain,
    hypervisor_addresses: list[str] | None = None,
    multiprocess: bool = True,
):
    """Feed hypervisor returns from the specified chain and hypervisor addresses

    Args:
        chain (Chain):
        hypervisor_addresses (list[str]): list of hype addresses
        multiprocess (bool, optional): use multiprocessing. Defaults to True.

    """

    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name} returns information {f'[multiprocessing]' if multiprocess else ''}"
    )

    if _last_returns_data_db := get_last_return_data_from_db(
        chain=chain, hypervisor_addresses=hypervisor_addresses
    ):
        latest_block = get_latest_block(chain=chain)

        if multiprocess:
            # build arguments for multiprocessing
            _args = [
                (chain, hypervisor_address, block_ini, latest_block)
                for hypervisor_address, block_ini in _last_returns_data_db
            ]

            with Pool() as pool:
                pool.starmap(feed_hypervisor_returns_work, _args)
        else:
            # get addresses and blocks to feed
            for hypervisor_address, block_ini in _last_returns_data_db:
                feed_hypervisor_returns_work(
                    chain=chain,
                    hypervisor_address=hypervisor_address,
                    block_ini=block_ini,
                    latest_block=latest_block,
                )
    else:
        logging.getLogger(__name__).info(
            f" No hypervisor returns to process for {chain.database_name}. "
        )


def feed_hypervisor_returns_work(
    chain: Chain, hypervisor_address: str, block_ini: int, latest_block: int
):
    block_end = latest_block
    # limit the qtty of blocks to feed at once and repeat next time if more blocks are needed
    # find potential results qtty and filter > 25000
    _count_potential_items = get_default_localdb(
        network=chain.database_name
    ).count_documents(
        collection_name="operations",
        filter={
            "address": hypervisor_address,
            "blockNumber": {"$gte": block_ini, "$lte": latest_block},
            "topic": {"$in": ["deposit", "withdraw", "rebalance", "zeroBurn"]},
        },
    )
    if _count_potential_items > 25000:
        # get the last block found in database, for those first 25000 items
        block_end = get_default_localdb(
            network=chain.database_name
        ).get_items_from_database(
            collection_name="operations",
            find={
                "address": hypervisor_address,
                "blockNumber": {"$gte": block_ini, "$lte": latest_block},
                "topic": {"$in": ["deposit", "withdraw", "rebalance", "zeroBurn"]},
            },
            projection={"blockNumber": 1, "_id": 0},
            skip=24999,
            limit=1,
            batch_size=1000,
            sort=[("blockNumber", 1)],
        )[
            0
        ][
            "blockNumber"
        ]

        logging.getLogger(__name__).debug(
            f" >25,000 items found for {chain.database_name} {hypervisor_address}. Limiting from {block_ini} to {block_end} blocks. Will continue from {block_end} on next loop ( make sure it happens)"
        )

    try:
        if period_yield_list := create_period_yields(
            chain=chain,
            hypervisor_address=hypervisor_address,
            block_ini=block_ini,
            block_end=block_end,
            try_solve_errors=False,
        ):
            # convert to dict and save
            try:
                _todict = [x.to_dict() for x in period_yield_list]
                # save converted to dict results to database
                save_hypervisor_returns_to_database(
                    chain=chain,
                    period_yield_list=_todict,
                )
            except AttributeError as e:
                # AttributeError: 'NoneType' object has no attribute 'to_dict'
                logging.getLogger(__name__).error(
                    f" Could not convert yield result to dictionary, so not saved. Probably because of a previous hopefully solved error -> {e}"
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Could not convert yield result to dictionary, so not saved -> {e}"
                )
    except ProcessingError as e:
        logging.getLogger(__name__).error(
            f" Could not create yield data for {chain.database_name} {hypervisor_address} -> {e}"
        )
        # try solve error
        process_error(error=e)


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

    try:
        return return_helper.execute_processes_within_hypervisor_periods(
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
            try_solve_errors=try_solve_errors,
        )
    except ProcessingError as e:
        logging.getLogger(__name__).debug(
            f" Could not create yield data for {chain.database_name} {hypervisor_address} -> {e}"
        )
        # try solve error
        process_error(error=e)
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"  Error while creating yield data -> {e}"
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
