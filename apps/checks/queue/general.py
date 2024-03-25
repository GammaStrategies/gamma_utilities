import logging
from apps.checks.base_objects import analysis_item, base_analyzer_object
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, queueItemType


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
            x["adddress"]
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
            for item in sorted(values.keys()):
                log_message += f"      {item['count']} -> {item['qtty']} items\n"
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
