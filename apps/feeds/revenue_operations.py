from datetime import datetime
import logging

import tqdm
from apps.feeds.queue.queue_item import QueueItem
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, queueItemType
from bins.general.general_utilities import convert_string_datetime
from bins.w3.onchain_data_helper import onchain_data_helper
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.gamma.collectors import wallet_transfers_collector


def create_revenue_operations_address(
    network: str,
    block_ini: int | None = None,
    block_end: int | None = None,
) -> tuple[list[str], int, int]:
    logging.getLogger(__name__).info(
        f" Creating {network}'s revenue operations wallet addresses"
    )

    # debug variables
    fixed_revenue_addresses = set(
        CONFIGURATION["script"]["protocols"]["gamma"]
        .get("filters", {})
        .get("revenue_wallets", {})
        .get(network, [])
        or []
    )

    # create a web3 protocol helper
    onchain_helper = onchain_data_helper(protocol="gamma")

    # get all feeRecipient addresses from static, if field does not exist, scrape it.
    hypervisors_static_list = get_from_localdb(
        network=network,
        collection="static",
        find={},
    )

    if not hypervisors_static_list:
        logging.getLogger(__name__).debug(
            f" No hypervisor static data found for {network}. Returning with None values on addresses creation."
        )
        return None, None, None

    # merge feeRecipient addresses from hypes static
    for hype_static in hypervisors_static_list:
        if hype_static.get("feeRecipient", None):
            fixed_revenue_addresses.add(hype_static["feeRecipient"].lower())

    try:
        # try getting initial block as last found in database
        if not block_ini:
            block_ini = get_db_last_revenue_operation_block(network=network)
            logging.getLogger(__name__).debug(
                f"   Setting initial block to {block_ini}, being the last block found in revenue operations database collection"
            )

        if not block_ini or block_ini == 0:
            # pick the minimum block from it
            logging.getLogger(__name__).info(
                f"   Getting {network} initial block from the minimum static hype's collection block found"
            )
            block_ini = min([x["block"] for x in hypervisors_static_list])

        if not block_end:
            # get last block from onchain
            block_end = (
                onchain_helper.create_erc20_helper(network)
                ._getBlockData("latest")
                .number
            )

        # check for block range inconsistency
        if block_end < block_ini:
            raise ValueError(
                f" Initial block {block_ini} is higher than end block: {block_end}"
            )

        # exclude wrong addresses 0x0000000000000000000000000000000000000000
        try:
            fixed_revenue_addresses.remove("0x0000000000000000000000000000000000000000")
        except Exception:
            pass

        return list(fixed_revenue_addresses), block_ini, block_end

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while searching {network} for revenue operations  .error: {e}"
        )

    return None, None, None


def feed_revenue_operations(
    chain: Chain,
    addresses: list,
    block_ini: int,
    block_end: int,
    max_blocks_step: int = 1000,
):
    """Enqueue transfer logs from feeRecipient or other addresses and save em to the database

    Args:
        network (str): Network to scrape
        addresses (list): List of addresses to get all transfer operations
        block_ini (int): Initial block to scrape
        block_end (int): End block to scrape
        max_blocks_step (int, optional): Maximum blocks to scrape at once in one query (seee operations_generator) Careful bc some RPCs do not like respond well to high values. Defaults to 1000.
    """

    # set global protocol helper
    data_collector = wallet_transfers_collector(
        network=chain.database_name, wallet_addresses=addresses
    )

    logging.getLogger(__name__).info(
        "   Feeding database with {}'s {} wallets transfer operations from blocks {} to {}".format(
            chain.database_name,
            len(addresses),
            block_ini,
            block_end,
        )
    )

    with tqdm.tqdm(total=100) as progress_bar:
        # create callback progress funtion
        def _update_progress(text=None, remaining=None, total=None):
            # set text
            if text:
                progress_bar.set_description(text)
            # set total
            if total:
                progress_bar.total = total
            # update current
            if remaining:
                progress_bar.update(((total - remaining) - progress_bar.n))
            else:
                progress_bar.update(1)
            # refresh
            progress_bar.refresh()

        # set progress callback to data collector
        data_collector.progress_callback = _update_progress

        for operations in data_collector.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            max_blocks=max_blocks_step,
        ):
            # process operation
            task_enqueue_revenue_operations(
                operations=operations,
                network=chain.database_name,
                operation_type=queueItemType.REVENUE_OPERATION,
            )


def task_enqueue_revenue_operations(
    operations: list[dict], network: str, operation_type: queueItemType
):
    # build a list of operations to be added to the queue
    to_add = [
        QueueItem(
            type=operation_type,
            block=int(operation["blockNumber"]),
            address=operation["address"].lower(),
            data=operation,
        ).as_dict
        for operation in operations
    ]

    if db_return := get_default_localdb(network=network).replace_items_to_database(
        data=to_add, collection_name="queue"
    ):
        logging.getLogger(__name__).debug(
            f"     db return-> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
        )
    else:
        logging.getLogger(__name__).error(
            f"  database did not return anything while saving {operation_type}s to queue"
        )


def get_db_last_revenue_operation_block(network: str) -> int:
    # get last block from revenue operations database collection
    last_revenue_operations_blocks = get_from_localdb(
        network=network,
        collection="revenue_operations",
        find={},
        sort=[("block", -1)],
        limit=1,
    )

    return (
        last_revenue_operations_blocks[0]["block"]
        if last_revenue_operations_blocks
        else 0
    )
