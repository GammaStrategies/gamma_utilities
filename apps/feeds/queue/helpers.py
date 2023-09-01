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
            db_queue_item=queue_item.as_dict
        ):
            if db_return.modified_count or db_return.upserted_id:
                logging.getLogger(__name__).debug(
                    f" Freed {queue_item.type} {queue_item.id} from queue"
                )
                return True
            else:
                logging.getLogger(__name__).error(
                    f" Could not free {queue_item.type} {queue_item.id} from queue. Database returned: {db_return.raw_result}"
                )
        else:
            logging.getLogger(__name__).error(
                f" No database return received while trying to free queue item {queue_item.id}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" Not freeing {queue_item.type} {queue_item.id} from queue because it failed {queue_item.count} times and needs a cooldown. Will need to be unlocked by a 'check' command"
        )
        # save item with count
        if db_return := get_default_localdb(network=network).set_queue_item(
            data=queue_item.as_dict
        ):
            logging.getLogger(__name__).debug(
                f" Saved {queue_item.type} {queue_item.id} with count {queue_item.count} to queue"
            )

    return False
