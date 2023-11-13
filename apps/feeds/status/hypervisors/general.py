import contextlib
from datetime import datetime, timezone
import logging
import concurrent.futures
import tqdm


from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_hypervisor_status
from bins.database.common.db_collections_common import database_local

from bins.w3.builders import build_db_hypervisor, build_db_hypervisor_multicall
from bins.w3.protocols.general import erc20_cached


def feed_hypervisor_status(
    protocol: str, network: str, rewrite: bool = False, threaded: bool = True
):
    """Creates hypervisor status at all operations block and block-1
            + every 20 minutes after last found status block ( if those minutes have already passed )

    Args:
        protocol (str):
        network (str):
        rewrite (bool): rewrite all status
        threaded: (bool):
    """

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors status information {'[rewriting all]' if rewrite else ''}"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # apply filters
    hypes_not_included: list = [
        x.lower()
        for x in (
            CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
        )
        .get("hypervisors_not_included", {})
        .get(network, [])
    ]
    logging.getLogger(__name__).debug(f"   excluding hypervisors: {hypes_not_included}")
    # get all static hypervisor info and convert it to dict
    static_info = {
        x["address"]: x
        for x in local_db.get_items(collection_name="static")
        if x["address"] not in hypes_not_included
    }

    # create a unique list of blocks addresses from database to be processed including:
    #       operation blocks and their block-1 relatives
    #       block every X min
    toProcess_block_address = {}
    for x in local_db.get_unique_operations_addressBlock(
        topics=["deposit", "withdraw", "zeroBurn", "rebalance"]
    ):
        # add operation addressBlock to be processed

        toProcess_block_address[
            create_id_hypervisor_status(
                hypervisor_address=x["address"], block=x["block"]
            )
        ] = {
            "address": x["address"],
            "block": x["block"],
            "fees_metadata": "ini",
        }
        # add block -1
        toProcess_block_address[
            create_id_hypervisor_status(
                hypervisor_address=x["address"], block=x["block"] - 1
            )
        ] = {
            "address": x["address"],
            "block": x["block"] - 1,
            "fees_metadata": "end",
        }

    # add latest block to all hypervisors every 20 min
    try:
        if (
            datetime.now(timezone.utc).timestamp()
            - local_db.get_max_field(collection="status", field="timestamp")[0]["max"]
        ) > 60 * 20:
            latest_block = (
                erc20_cached(
                    address="0x0000000000000000000000000000000000000000",
                    network=network,
                )
                ._w3.eth.get_block("latest")
                .number
            )

            logging.getLogger(__name__).debug(
                f" Adding the latest block [{latest_block}] to all addresses for status to be scraped "
            )

            for address in static_info:
                toProcess_block_address[
                    create_id_hypervisor_status(
                        hypervisor_address=address, block=latest_block
                    )
                ] = {
                    "address": address,
                    "block": latest_block,
                    "fees_metadata": "mid",
                }
    except IndexError:
        logging.getLogger(__name__).debug(
            f" Seems like there is no {network}'s {protocol} status data in db. Continue without adding latest block to all addresses for status to be scraped"
        )
    except Exception:
        logging.getLogger(__name__).exception(
            " unexpected error while adding new blocks to status scrape process "
        )

    if rewrite:
        # rewrite all address blocks
        processed_blocks = {}
    else:
        # get a list of blocks already processed
        processed_blocks = {
            create_id_hypervisor_status(
                hypervisor_address=x["address"], block=x["block"]
            ): x
            for x in local_db.get_unique_status_addressBlock()
        }

    logging.getLogger(__name__).debug(
        f"   Total address blocks {len(toProcess_block_address)} ->  Already processed {len(processed_blocks)} "
    )

    # remove already processed blocks
    for k in processed_blocks:
        try:
            toProcess_block_address.pop(k)
        except KeyError as err:
            # there are many more status blocks than operations ...
            # not to worry
            # logging.getLogger(__name__).warning(
            #     f" Could not find status block address key {k} in operations"
            # )
            pass
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error found while construction block addresses to feed db with hype status.  err-> {e}"
            )

    # set log list of hypervisors with errors
    _errors = 0

    with tqdm.tqdm(total=len(toProcess_block_address), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    item["address"],
                    network,
                    item["block"],
                    static_info[item["address"]]["dex"],
                    # False,
                )
                for item in toProcess_block_address.values()
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(
                    lambda p: create_and_save_hypervisor_status(*p), args
                ):
                    if not result:
                        # error found
                        _errors += 1

                    # else:
                    #     # progress
                    #     progress_bar.set_description(
                    #         f' {result.get("address", " ")} processed '
                    #     )
                    #     progress_bar.refresh()
                    #     # add hypervisor status to database
                    #     local_db.set_status(data=result)
                    # update progress
                    progress_bar.update(1)
        else:
            # get operations from database
            for item in toProcess_block_address.values():
                progress_bar.set_description(
                    f' 0x..{item.get("address", "    ")[-4:]} at block {item.get("block", "")} to be processed'
                )

                progress_bar.refresh()
                result = build_db_hypervisor(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                    dex=static_info[item["address"]]["dex"],
                    cached=True,
                    static_mode=False,
                )
                if not create_and_save_hypervisor_status(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                    dex=static_info[item["address"]]["dex"],
                ):
                    # error found
                    _errors += 1
                # if result != None:
                #     # add hypervisor status to database
                #     local_db.set_status(data=result)
                # else:
                #     # error found
                #     _errors += 1
                # update progress
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisor status could not be scraped due to errors".format(
                    _errors,
                    len(toProcess_block_address),
                    (_errors / len(toProcess_block_address))
                    if toProcess_block_address
                    else 0,
                )
            )


def create_and_save_hypervisor_status(
    address: str, network: str, block: int, dex: str
) -> bool:
    """create hyperivor status at the specified block and save it into the database

    Args:
        address (str):
        network (Chain):
        block (int):
        dex (str):

    Returns:
        bool: saved or not
    """
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    db_name = f"{network}_gamma"

    try:
        # create hype and save

        if hype := build_db_hypervisor(
            address=address, network=network, block=block, dex=dex, cached=True
        ):
            # save hype
            database_local(mongo_url=mongo_url, db_name=db_name).set_status(data=hype)
            # return success
            return True
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" unexpected error while creating and saving hype status {address} at block {block} -> {e}"
        )

    # return failure
    return False
