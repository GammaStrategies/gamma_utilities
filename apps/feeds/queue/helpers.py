import logging
import time
from apps.feeds.queue.queue_item import QueueItem
from bins.database.helpers import get_default_localdb


def to_free_or_not_to_free_item(
    network: str,
    queue_item: QueueItem,
) -> bool:
    """Free item from processing if count is lower than X,
        so that after X fails, next time will need an unlock before processing, (unlock = taking longer to process)

    Args:
        queue_item (QueueItem):
        local_db (database_local):

    Returns:
        bool: freed or not
    """
    # do not free items not
    if queue_item.count < 5 and queue_item.can_be_processed:
        if db_return := get_default_localdb(network=network).free_queue_item(
            id=queue_item.id, count=queue_item.count
        ):
            return True
        else:
            logging.getLogger(__name__).error(
                f" No database return received while trying to free queue item {queue_item.id}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" Not freeing {queue_item.type} {queue_item.id} from queue because it failed {queue_item.count} times and needs a cooldown. Will need to be unlocked by a 'check' command"
        )
        # save item with count

        if db_return := get_default_localdb(network=network).find_one_and_update(
            collection_name="queue",
            find={"id": queue_item.id},
            update={"$set": {"count": queue_item.count}},
        ):
            logging.getLogger(__name__).debug(
                f" Updated count of queue item {queue_item.type} {queue_item.id} to {queue_item.count}"
            )

    return False
