# Main processing function
from datetime import datetime, timezone
import logging
import time

from apps.feeds.queue.helpers import to_free_or_not_to_free_item
from apps.feeds.queue.pulls.block import pull_from_queue_block
from apps.feeds.queue.pulls.hypervisor import (
    pull_from_queue_hypervisor_static,
    pull_from_queue_hypervisor_status,
)
from apps.feeds.queue.pulls.mfd import pull_from_queue_latest_multiFeeDistribution
from apps.feeds.queue.pulls.operation import pull_from_queue_operation
from apps.feeds.queue.pulls.price import pull_from_queue_price
from apps.feeds.queue.pulls.revenue_operation import pull_from_queue_revenue_operation
from apps.feeds.queue.pulls.reward import pull_from_queue_reward_status
from apps.feeds.queue.pulls.user import pull_from_queue_user_operation
from apps.feeds.queue.queue_item import QueueItem
from bins.database.helpers import get_default_localdb
from bins.general.enums import queueItemType
from bins.general.general_utilities import log_time_passed, seconds_to_time_passed


def pull_from_queue(
    network: str,
    types: list[queueItemType] | None = None,
    find: dict | None = None,
    sort: list | None = None,
):
    # get first item from queue
    if db_queue_item := get_item_from_queue(
        network=network, types=types, find=find, sort=sort
    ):
        try:
            # convert database queue item to class
            queue_item = QueueItem(**db_queue_item)

            logging.getLogger(__name__).debug(
                f" Processing {queue_item.type} queue item -> count: {queue_item.count} creation: {log_time_passed.get_timepassed_string(datetime.fromtimestamp(queue_item.creation,timezone.utc))} ago"
            )

            # process queue item
            return process_queue_item_type(network=network, queue_item=queue_item)

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error processing {queue_item.type} queue item: {e}"
            )
            # set queue item free but save counter
            if db_result := get_default_localdb(network=network).free_queue_item(
                id=queue_item.id, count=queue_item.count
            ):
                logging.getLogger(__name__).debug(
                    f" {network}'s queue item {queue_item.id} has been set as not being processed, and count has been updated"
                )

            raise e
    else:
        # no item found
        logging.getLogger(__name__).debug(f" No queue item found for {network}")

    return True


def get_item_from_queue(
    network: str,
    types: list[queueItemType] | None = None,
    find: dict | None = None,
    sort: list | None = None,
) -> dict | None:
    """FIFO queue but error count zero have priority over > 0.
    Get first item not being processed

    Args:
        network (str):
        types (list[queueItemType] | None, optional): . Defaults to All.
        find (dict, optional): . Defaults to {"processing": 0, "count": {"$lt": 5}}.
        sort (list, optional): . Defaults to [("count", 1), ("creation", 1)]. 1 is ascending, -1 is descending

    Returns:
        dict | None: queue item
    """
    if not find:
        find = {"processing": 0, "count": {"$lt": 5}}
    if not sort:
        sort = [("count", 1), ("creation", 1)]

    return get_default_localdb(network=network).get_queue_item(
        types=types,
        find=find,
        sort=sort,
    )


# classifier
def process_queue_item_type(network: str, queue_item: QueueItem) -> bool:
    """Get item from queue and process it.

        Items with count>0 will be processed if queue.can_be_processed is True

    Args:
        network (str): network name
        queue_item (QueueItem):


    Returns:
        bool: processed successfully or not
    """

    if queue_item.can_be_processed == False:
        logging.getLogger(__name__).error(
            f" {network}'s queue item {queue_item.id} cannot be processed yet (more cooldown time defined). Will be processed later"
        )
        return False

    logging.getLogger(__name__).info(
        f"Processing {network}'s {queue_item.type} queue item with count {queue_item.count} at block {queue_item.block}"
    )

    if queue_item.type == queueItemType.HYPERVISOR_STATUS:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_hypervisor_status,
        )
        # return pull_from_queue_hypervisor_status(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.REWARD_STATUS:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_reward_status,
        )
        # return pull_from_queue_reward_status(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.PRICE:
        return pull_common_processing_work(
            network=network, queue_item=queue_item, pull_func=pull_from_queue_price
        )
        # return pull_from_queue_price(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.BLOCK:
        return pull_common_processing_work(
            network=network, queue_item=queue_item, pull_func=pull_from_queue_block
        )
        # return pull_from_queue_block(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.OPERATION:
        return pull_common_processing_work(
            network=network, queue_item=queue_item, pull_func=pull_from_queue_operation
        )

    elif queue_item.type == queueItemType.LATEST_MULTIFEEDISTRIBUTION:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_latest_multiFeeDistribution,
        )

    elif queue_item.type == queueItemType.REVENUE_OPERATION:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_revenue_operation,
        )
    elif queue_item.type == queueItemType.HYPERVISOR_STATIC:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_hypervisor_static,
        )

    elif queue_item.type == queueItemType.USER_OPERATION:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_user_operation,
        )

    else:
        # reset queue item

        # set queue item as not being processed
        if db_result := get_default_localdb(network=network).free_queue_item(
            id=queue_item.id
        ):
            logging.getLogger(__name__).debug(
                f" {network}'s queue item {queue_item.id} has been set as not being processed"
            )

        # raise error
        raise ValueError(
            f" Unknown queue item type {queue_item.type} at network {network}"
        )


# Main processing function
def pull_common_processing_work(
    network: str, queue_item: QueueItem, pull_func: callable
):
    # build a result variable
    result = pull_func(network=network, queue_item=queue_item)

    # benchmark
    if result:
        # remove item from queue
        if db_return := get_default_localdb(network=network).del_queue_item(
            queue_item.id
        ):
            if db_return.deleted_count or db_return.acknowledged:
                logging.getLogger(__name__).debug(
                    f" {network}'s queue item {queue_item.id} has been removed from queue"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" {network}'s queue item {queue_item.id} has not been removed from queue. database returned {db_return.raw_result}"
                )
        else:
            logging.getLogger(__name__).error(
                f"  No database return received when deleting {network}'s queue item {queue_item.id}."
            )

        # log total process
        curr_time = time.time()
        logging.getLogger("benchmark").info(
            f" {network} queue item {queue_item.type}  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
        )
    else:
        # free item ?
        to_free_or_not_to_free_item(network=network, queue_item=queue_item)

    # return result
    return result
