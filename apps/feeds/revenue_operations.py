from datetime import datetime
import logging

import tqdm
from web3 import Web3
from apps.feeds.queue.queue_item import QueueItem
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, queueItemType
from bins.general.general_utilities import convert_string_datetime
from bins.w3.onchain_data_helper import onchain_data_helper
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.gamma.collectors import (
    wallet_transfers_collector,
    rewardPaid_collector,
)


def create_revenue_addresses(
    network: str,
    block_ini: int | None = None,
    block_end: int | None = None,
    revenue_address_type: str = "hypervisors",
) -> tuple[list[str], int, int]:
    """Create a list of wallet addresses and the block range to scrape for transaction operations
        Gamma wallets are the feeRecipient addresses from the hypervisors static data and the ones manually set at the configuration file
    Args:
        network (str):
        block_ini (int | None, optional): . Defaults to None.
        block_end (int | None, optional): . Defaults to None.

    Returns:
        tuple[list[str], int, int]:  addresses, block_ini, block_end
    """
    logging.getLogger(__name__).info(
        f" Creating a {network}'s wallet addresses list and block range to scrape for revenue operations"
    )

    # load fixed revenue addresses from configuration file
    fixed_revenue_config = (
        CONFIGURATION["script"]["protocols"]["gamma"]
        .get("filters", {})
        .get("revenue_wallets", {})
        .get(network, {})
        or {}
    )

    # only hypervisors addresses
    fixed_revenue_addresses = set()
    for k, v in fixed_revenue_config.items():
        if revenue_address_type == v.split("_")[0]:
            fixed_revenue_addresses.add(k.lower())

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

    # create a web3 protocol helper
    onchain_helper = onchain_data_helper(protocol="gamma")

    if revenue_address_type == "hypervisors":
        # merge feeRecipient addresses from hypes static
        for hype_static in hypervisors_static_list:
            if hype_static.get("feeRecipient", None):
                fixed_revenue_addresses.add(hype_static["feeRecipient"].lower())

    try:
        # try getting initial block as last found in database
        if not block_ini:
            block_ini = get_db_last_revenue_operation_block(
                network=network, addresses=fixed_revenue_addresses
            )
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
    # addresses: list,
    block_ini: int,
    block_end: int,
    max_blocks_step: int = 1000,
    rewrite: bool = False,
):
    """Enqueue transfer logs from feeRecipient or other addresses and save em to the database

    Args:
        network (str): Network to scrape
        addresses (list): List of addresses to get all transfer operations
        block_ini (int): Initial block to scrape
        block_end (int): End block to scrape
        max_blocks_step (int, optional): Maximum blocks to scrape at once in one query (seee operations_generator) Careful bc some RPCs do not like respond well to high values. Defaults to 1000.
    """

    # 1) transfers to hypervisor revenue addresses
    (
        addresses,
        block_ini,
        block_end,
    ) = create_revenue_addresses(
        network=chain.database_name,
        block_ini=block_ini,
        block_end=block_end,
        revenue_address_type="hypervisors",
    )
    feed_revenue_operations_from_hypervisors(
        chain=chain,
        addresses=addresses,
        block_ini=block_ini,
        block_end=block_end,
        max_blocks_step=max_blocks_step,
        rewrite=rewrite,
    )

    # 2) rewardPaid to revenue addresses
    addresses, block_ini, block_end = create_revenue_addresses(
        network=chain.database_name,
        block_ini=block_ini,
        block_end=block_end,
        revenue_address_type="venft",
    )
    feed_revenue_operations_from_venft(
        chain=chain,
        addresses=addresses,
        block_ini=block_ini,
        block_end=block_end,
        max_blocks_step=max_blocks_step,
        rewrite=rewrite,
    )


def feed_revenue_operations_from_hypervisors(
    chain: Chain,
    addresses: list,
    block_ini: int,
    block_end: int,
    max_blocks_step: int = 1000,
    rewrite: bool = False,
):
    # set global protocol helper
    collector_helper = wallet_transfers_collector(
        wallet_addresses=addresses,
        network=chain.database_name,
    )

    logging.getLogger(__name__).info(
        "   Feeding database with {}'s {} wallets transfer operations (hypes) from blocks {} to {}".format(
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
        collector_helper.progress_callback = _update_progress

        try:
            for operations in collector_helper.operations_generator(
                block_ini=block_ini,
                block_end=block_end,
                max_blocks=max_blocks_step,
            ):
                # process operation
                task_enqueue_revenue_operations(
                    operations=operations,
                    network=chain.database_name,
                    operation_type=queueItemType.REVENUE_OPERATION,
                    rewrite=rewrite,
                )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error while feeding {chain.database_name} revenue operations: {e}"
            )


def feed_revenue_operations_from_venft(
    chain: Chain,
    addresses: list,
    block_ini: int,
    block_end: int,
    max_blocks_step: int = 1000,
    rewrite: bool = False,
):
    # set global protocol helper
    collector_helper = rewardPaid_collector(
        wallet_addresses=addresses,
        network=chain.database_name,
    )

    logging.getLogger(__name__).info(
        "   Feeding database with {}'s {} wallets transfer operations (veNFT) from blocks {} to {}".format(
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
        collector_helper.progress_callback = _update_progress

        try:
            for operations in collector_helper.operations_generator(
                block_ini=block_ini,
                block_end=block_end,
                max_blocks=max_blocks_step,
            ):
                # process operation
                task_enqueue_revenue_operations(
                    operations=operations,
                    network=chain.database_name,
                    operation_type=queueItemType.REVENUE_OPERATION,
                    rewrite=rewrite,
                )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error while feeding {chain.database_name} revenue operations: {e}"
            )


def task_enqueue_revenue_operations(
    operations: list[dict],
    network: str,
    operation_type: queueItemType,
    rewrite: bool = False,
):
    # build a list of operations to be added to the queue
    to_add = []
    to_add_ids = []
    for operation in operations:
        item = QueueItem(
            type=operation_type,
            block=int(operation["blockNumber"]),
            address=operation["address"].lower(),
            data=operation,
        ).as_dict
        to_add.append(item)
        to_add_ids.append(item["id"])

    # check if operations are already in database or rewrite is set
    if not rewrite:
        # get a list of operations already in database
        for already_in_db in get_from_localdb(
            network=network,
            collection="revenue_operations",
            find={"id": {"$in": to_add_ids}},
        ):
            # remove already in database operations from to_add_ids
            to_add_ids.remove(already_in_db["id"])

        # remove already in database operations from to_add
        to_add = [x for x in to_add if x["id"] in to_add_ids]

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


def get_db_last_revenue_operation_block(
    network: str, addresses: list | None = None
) -> int:
    # get last block from revenue operations database collection

    last_revenue_operations_blocks = get_from_localdb(
        network=network,
        collection="revenue_operations",
        find={} if addresses is None else {"address": {"$in": addresses}},
        sort=[("blockNumber", -1)],
        limit=1,
    )

    return (
        last_revenue_operations_blocks[0]["blockNumber"]
        if last_revenue_operations_blocks
        else 0
    )


# src = hype --> reward
# src = a hype's pool --> swap
# src = 0x0 --> mint ( hype position as user)
# src = other:
#       token = known hype's pool token --> [ or transfer or reward]
#       dst =
