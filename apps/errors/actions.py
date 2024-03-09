import logging
from apps.errors.processing.negative_fees import actions_on_negative_fees
from apps.errors.processing.no_hypervisor_end import actions_on_no_hypervisor_period_end
from apps.errors.processing.supply_difference import actions_on_supply_difference
from apps.feeds.queue.queue_item import QueueItem
from apps.feeds.revenue_operations import feed_revenue_operations
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.errors.general import ProcessingError
from bins.general.enums import error_identity, queueItemType, text_to_protocol
from bins.log.telegram_logger import send_to_telegram
from bins.w3.builders import build_erc20_helper, build_hypervisor


def process_error(error: ProcessingError):
    """Take action based on error identity"""

    if error.identity == error_identity.RETURN_NONE:
        pass
    elif error.identity == error_identity.OVERLAPED_PERIODS:
        pass
    elif error.identity == error_identity.SUPPLY_DIFFERENCE:
        # this can indicate wrong snapshots or missing operation between two blocks
        if error.action == "rescrape":
            actions_on_supply_difference(error=error)
    elif error.identity == error_identity.NEGATIVE_FEES:
        if error.action == "rescrape":
            # take actions
            actions_on_negative_fees(error=error)
    elif error.identity == error_identity.INVALID_MFD:
        if error.action == "remove":
            # remove invalid mfd? TODO
            pass
    elif error.identity == error_identity.PRICE_NOT_FOUND:
        if error.action == "scrape_price":
            logging.getLogger(__name__).debug(
                f" Reaction to price not found -> Creating queueItem price for {error.item}"
            )
            try:
                # add a new queue item to scrape price.
                # If the item is already in the database, its count will be reset to zero but not the creation time
                price_item_db = QueueItem(
                    type=queueItemType.PRICE,
                    block=int(error.item["block"]),
                    address=error.item["address"],
                    data={},
                ).as_dict

                # get queue item if exists in database
                if dbItem := get_from_localdb(
                    network=error.chain.database_name,
                    collection="queue",
                    find={"id": price_item_db["id"]},
                ):
                    # select first and lonely item from list
                    dbItem = dbItem[0]
                    logging.getLogger(__name__).debug(
                        f" Reaction to price not found -> Reseting count field for existing queueItem price for {error.item}"
                    )
                    # modify count and replace
                    dbItem["count"] = 0
                    get_default_localdb(
                        network=error.chain.database_name
                    ).set_queue_item(data=dbItem)
                else:
                    # add to database
                    get_default_localdb(
                        network=error.chain.database_name
                    ).set_queue_item(data=price_item_db)
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f"  Error while trying to solve an error (wtf loop): {e}"
                )

    elif error.identity == error_identity.LPFEES_WITHOUT_REVENUE:
        if error.action == "rescrape":
            # try rescraping revenue_operations from ini end timestamps on that chain
            logging.getLogger(__name__).debug(
                f" Reaction to Lp fees found without revenue -> Trying to scrape revenue_operations of {error.chain.database_name} from {error.item['ini_timestamp']} to {error.item['end_timestamp']}"
            )
            try:
                # find block numbers for ini and end timestamps
                # create erc20 helper
                erc20 = build_erc20_helper(chain=error.chain)
                # get block numbers
                _ini_block = erc20.blockNumberFromTimestamp(
                    timestamp=error.item["ini_timestamp"]
                )
                _end_block = erc20.blockNumberFromTimestamp(
                    timestamp=error.item["end_timestamp"]
                )
                # rescrape revenue operations
                feed_revenue_operations(
                    chain=error.chain,
                    block_ini=_ini_block,
                    block_end=_end_block,
                    max_blocks_step=5000,
                    rewrite=False,
                )

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f"  Error while trying to solve an error (wtf loop): {e}"
                )

    elif error.identity == error_identity.WRONG_CONTRACT_FIELD_TYPE:
        send_to_telegram.error(msg=error.message, topic="configuration", dtime=True)

    elif error.identity == error_identity.NO_HYPERVISOR_PERIOD_END:
        if error.action == "rescrape":
            # take actions
            actions_on_no_hypervisor_period_end(error=error)

    else:
        logging.getLogger(__name__).warning(
            f"Unknown error identity {error.identity}. Can't process error"
        )
