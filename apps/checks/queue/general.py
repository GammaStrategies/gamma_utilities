import logging
from apps.checks.base_objects import analysis_item, base_analyzer_object
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, queueItemType
from bins.mixed.price_utilities import price_scraper


class queue_analyzer(base_analyzer_object):
    def __init__(self):
        super().__init__()

    # sometimes, the queue is filled with hype addresses instead of token addresses ( TODO: check how this actually happens)
    def check_queue_hypervisor_prices(self, chain: Chain) -> list[dict]:
        """Identify queued hypervisors addresses as tokens

        Args:
            chain (Chain):

        Returns:
            list[dict]: list of queue items in the database matching criteria
        """

        # get a list of hypervisor addresses
        hypervisor_addresses = {
            x["address"]
            for x in get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={},
                projection={"address": 1},
            )
        }

        # get a group of prices stuck in the queue
        wrong_prices = get_from_localdb(
            network=chain.database_name,
            collection="queue",
            aggregate=[
                {
                    "$match": {
                        "type": queueItemType.PRICE,
                        "address": {"$in": list(hypervisor_addresses)},
                    }
                },
                {"$sort": {"block": 1}},
            ],
        )

        if not wrong_prices:
            return []

        else:
            logging.getLogger(__name__).debug(
                f" Found {len(wrong_prices)} hypervisors saved as tokens in the queue."
            )
            self.items.append(
                analysis_item(
                    name="hypervisor_as_token_in_queue",
                    data=wrong_prices,
                    log_message=f" Found {len(wrong_prices)} hypervisors saved as tokens in the queue",
                    telegram_message=f" <b>QUEUE</b> Found {len(wrong_prices)} hypervisors saved as tokens in the queue",
                )
            )

    # summary list of types and number of items of a certain type in the queue
    def check_queue_summary(self, chain: Chain):
        """Get a list of queue item types and their count

        Args:
            chain (Chain): _description_
        """

        _query = [
            {
                "$group": {
                    "_id": {"type": "$type", "count": "$count"},
                    "qtty": {"$sum": 1},
                }
            },
            {"$project": {"type": "$_id.type", "count": "$_id.count", "qtty": "$qtty"}},
            {"$sort": {"type": 1, "count": 1, "qtty": 1}},
            {"$project": {"_id": 0}},
        ]

        queue_summary = get_from_localdb(
            network=chain.database_name,
            collection="queue",
            aggregate=_query,
        )

        if not queue_summary:
            logging.getLogger(__name__).debug(
                f" No queue items found for {chain.fantasy_name} chain."
            )
            return

        # build result
        result = {}
        for item in queue_summary:
            # TYPE
            if item["type"] not in result:
                result[item["type"]] = {}

            # COUNT
            if item["count"] not in result[item["type"]]:
                result[item["type"]][item["count"]] = 0

            # QTTY
            result[item["type"]][item["count"]] += item["qtty"]

        # build message
        log_message = f" {chain.fantasy_name} queue summary: \n"
        telegram_message = f" <b>QUEUE</b> {chain.fantasy_name} summary: \n"
        for itm_type, values in result.items():
            log_message += f" {itm_type}:\n"
            telegram_message += f"<i> {itm_type}</i>:\n"
            # sort by count (ascending)
            for k in sorted(values.keys()):
                log_message += f"      {k} -> {values[k]} items\n"
                telegram_message += (
                    f"      <i>{item['count']}</i> -> {item['qtty']} items\n"
                )

        # log queue items
        self.items.append(
            analysis_item(
                name="queue_summary",
                data=queue_summary,
                log_message=log_message,
                telegram_message=telegram_message,
            )
        )

    # check prices not configured correctly
    def check_price_configuration(self, chain: Chain):
        """Check if all tokens left in queue can be priced correctly

        Args:
            chains (list[Chain] | None, optional): list of chains to process. Defaults to All.
        """
        # get tokens from static collection
        grouped_queued_prices = get_from_localdb(
            network=chain.database_name,
            collection="queue",
            aggregate=[
                {"$match": {"type": queueItemType.PRICE}},
                {
                    "$group": {
                        "_id": "$address",
                        "count": {"$sum": 1},
                        "items": {"$push": "$$ROOT"},
                    }
                },
                {"$sort": {"count": 1}},
            ],
        )

        if not grouped_queued_prices:
            logging.getLogger(__name__).debug(
                f" No tokens found in {chain.database_name} queue collection"
            )
            return

        # try get prices for all those at current block
        price_helper = price_scraper(thegraph=False)

        for queue_item in grouped_queued_prices:

            try:
                _tmpPrice, _tmpSource = price_helper.get_price(
                    network=chain.database_name,
                    token_id=queue_item["_id"],
                    block=0,
                )
                if not _tmpPrice:
                    self.items.append(
                        analysis_item(
                            name="missing_price",
                            data={
                                "network": chain.database_name,
                                "address": queue_item["_id"],
                            },
                            log_message=f" {chain.fantasy_name} token is not getting the price correctly {queue_item['_id']}",
                            telegram_message=f"<b> {chain.fantasy_name} token is not getting the price correctly </b><pre>{queue_item['_id']}</pre>",
                        )
                    )

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error getting price for {chain.database_name} {queue_item['_id']} -> {e}"
                )
