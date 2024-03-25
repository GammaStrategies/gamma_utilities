import logging
from apps.checks.base_objects import analysis_item, base_analyzer_object
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, queueItemType


class queue_analyzer(base_analyzer_object):
    def __init__(self):
        super().__init__()

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
