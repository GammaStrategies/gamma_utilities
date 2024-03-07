from concurrent.futures import ProcessPoolExecutor
import logging

import tqdm
from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_hypervisor_static
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, Protocol, text_to_protocol
from bins.general.general_utilities import initializer
from bins.w3.builders import (
    build_db_hypervisor_multicall,
    build_erc20_helper,
    build_hypervisor,
)
from bins.w3.protocols.gamma.hypervisor import gamma_hypervisor_multicall


def feed_latest_hypervisor_snapshots(
    chain: Chain, protocols: list[Protocol] | None = None, save_to_database: bool = True
) -> list[dict] | int | None:
    """Create snapshots of hypervisors at current block and save it to databasse, if required

    Args:
        chain (Chain): network
        protocols (list[Protocol] | None, optional): list of protocols. Defaults to All.
        save_to_database (bool, optional): When false, will return the list of hypervisors. Defaults to True.

    Returns:
        list[dict]: list of hypervisors
        or
        int: number of hypervisors processed when save_to_database is False
        or
        None: when no hypervisors are found
    """
    logging.getLogger(__name__).debug(
        f" {'Feeding database with' if save_to_database else 'Creating'} latest hypervisor status list for {chain.database_name} {protocols if protocols else ''} "
    )

    # get all hypervisors not excluded
    hypes_not_included = (
        CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get("gamma", {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(chain.database_name, [])
    )
    hypervisors_static = get_from_localdb(
        network=chain.database_name,
        collection="static",
        find={"address": {"$nin": hypes_not_included}},
    )
    if not hypervisors_static:
        logging.getLogger(__name__).warning(
            f" No hypervisors found for {chain.database_name}"
        )
        return

    # to be able to decrease the call to a specific block, we need to get the current block and timestamp
    erc_helper = build_erc20_helper(chain=chain)
    _block = erc_helper.block
    _timestamp = erc_helper._timestamp

    # create hypervisor multicall objects for each hypervisor
    data_list = [
        {
            "address": hype["address"],
            "network": chain.database_name,
            "block": _block,
            "timestamp": _timestamp,
            "dex": text_to_protocol(hype["dex"]),
            "pool_address": hype["pool"]["address"],
            "token0_address": hype["pool"]["token0"]["address"],
            "token1_address": hype["pool"]["token1"]["address"],
        }
        for hype in hypervisors_static
    ]
    save_to_db = []
    _fails = 0
    with tqdm.tqdm(total=len(data_list)) as progress_bar:
        # prepare arguments
        with ProcessPoolExecutor(max_workers=8, initializer=initializer) as ex:
            for hypervisor_status_db in ex.map(execute_process_loop, data_list):
                if not hypervisor_status_db:
                    _fails += 1
                    continue

                # add id to hype dict  ( hype address is unique in latest hypervisor snapshot so use static formula)
                hypervisor_status_db["id"] = create_id_hypervisor_static(
                    hypervisor_address=hypervisor_status_db["address"]
                )
                # add to save list
                save_to_db.append(hypervisor_status_db)

                # check if we should save to db when we reach 20
                if save_to_database and len(save_to_db) >= 20:
                    # save to db
                    if save_latest_hype_snapshots(chain=chain, data=save_to_db):
                        # reset when successful
                        save_to_db = []

                progress_bar.set_description(
                    f"  Feeding {chain.fantasy_name} latest hype snapshots: {_fails} not processed"
                )
                progress_bar.update(1)

    # check if we should save lefties
    if save_to_database and len(save_to_db) > 0:
        # save to db
        if save_latest_hype_snapshots(chain=chain, data=save_to_db):
            # reset when successful
            save_to_db = []

    if save_to_db:
        return save_to_db
    else:
        # return the number of items successfully processed
        return len(data_list) - _fails


# HELPER FUNCTIONS


def execute_process_loop(data: dict):
    # execute multicalls
    result = build_db_hypervisor_multicall(**data)
    if not result:
        logging.getLogger(__name__).error(
            f" Cannot create hypervisor snapshot {data['network']} {data['dex']} {data['address']} from  at block {data['block']}  "
        )
    return result


def save_latest_hype_snapshots(chain: Chain, data: list[dict]) -> bool:
    """Save latest hypervisor snapshot to database

    Args:
        chain (Chain): _description_
        data (list[dict]): hypervisor as dictionary

    Returns:
        bool: success or fail
    """
    db_return = get_default_localdb(
        network=chain.database_name
    ).replace_items_to_database(
        data=data, collection_name="latest_hypervisor_snapshots"
    )

    if not db_return:
        logging.getLogger(__name__).error(
            f" Error saving {len(data)} latest hypervisor snapshot items to database {chain.database_name}"
        )
        return False
    else:
        logging.getLogger(__name__).debug(
            f" {'Saved' if db_return.upserted_count else 'Modified'} {db_return.upserted_count or db_return.modified_count} latest hypervisor snapshot items to database {chain.database_name}"
        )
        return True
