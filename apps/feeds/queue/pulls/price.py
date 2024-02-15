import logging
from apps.feeds.queue.queue_item import QueueItem
from bins.checkers.address import check_is_token
from bins.configuration import TOKEN_ADDRESS_EXCLUDE
from bins.database.helpers import get_default_globaldb
from bins.general.enums import text_to_chain

from bins.mixed.price_utilities import price_scraper


def pull_from_queue_price(network: str, queue_item: QueueItem) -> bool:
    # check prices not to process
    if queue_item.address.lower() in TOKEN_ADDRESS_EXCLUDE.get(network, {}):
        logging.getLogger(__name__).debug(
            f" {network} queue item {queue_item.id} price is excluded from processing. Removing from queue"
        )
        # remove from queue
        return True

    # check if address is actually a contract
    if not check_is_token(chain=text_to_chain(network), address=queue_item.address):
        # remove from queue
        logging.getLogger(__name__).debug(
            f" {network} queue item {queue_item.id} address {queue_item.address} is not a contract. Removing from queue"
        )
        return True

    try:
        # set price gatherer
        price_helper = price_scraper(
            cache=False, thegraph=False, geckoterminal_sleepNretry=True
        )
        # get price
        price, source = price_helper.get_price(
            network=network, token_id=queue_item.address, block=queue_item.block
        )

        if price:
            # save price into database
            if db_return := get_default_globaldb().set_price_usd(
                network=network,
                block=queue_item.block,
                token_address=queue_item.address,
                price_usd=price,
                source=source,
            ):
                # evaluate if price has been saved
                if (
                    db_return.upserted_id
                    or db_return.modified_count
                    or db_return.matched_count
                ):
                    logging.getLogger(__name__).debug(f" {network} price saved")

                    return True

                else:
                    logging.getLogger(__name__).error(
                        f" {network} price not saved. Database returned :{db_return.raw_result}"
                    )
            else:
                logging.getLogger(__name__).error(
                    f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).debug(
                f" Cant get price for {network}'s {queue_item.address} token"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s price queue item: {e}"
        )

    # return result
    return False
