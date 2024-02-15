import logging
from apps.feeds.queue.queue_item import QueueItem
from bins.database.helpers import get_default_globaldb
from bins.general.enums import text_to_chain
from bins.w3.builders import build_erc20_helper

from bins.w3.protocols.general import bep20, erc20


def pull_from_queue_block(network: str, queue_item: QueueItem) -> bool:

    try:
        dummy = build_erc20_helper(
            chain=text_to_chain(network),
            address=queue_item.address,
            cached=False,
            block=queue_item.block,
        )

        if dummy._timestamp:
            # save block into database
            if db_return := get_default_globaldb().set_block(
                network=network, block=dummy.block, timestamp=dummy._timestamp
            ):
                # evaluate if price has been saved
                if (
                    db_return.upserted_id
                    or db_return.modified_count
                    or db_return.matched_count
                ):
                    logging.getLogger(__name__).debug(
                        f" {network} queue item {queue_item.id} block saved to database"
                    )
                    # define result
                    return True
                else:
                    logging.getLogger(__name__).error(
                        f" {network} queue item {queue_item.id} block not saved to database. database returned: {db_return.raw_result}"
                    )
            else:
                logging.getLogger(__name__).error(
                    f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).debug(
                f" Cant get timestamp for {network}'s block {queue_item.block}"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s block queue item: {e}"
        )

    # return result
    return False
