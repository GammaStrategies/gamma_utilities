import logging
import time
import tqdm
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_default_localdb, get_from_localdb


def repair_queue_locked_items():
    """
    Reset queue items that are locked for more than 10 minutes
    No queue item should be running for more than 2 minutes
    items with the field count =>10 will not be unlocked
    """

    logging.getLogger(__name__).info(
        f">Repair queue items that are locked for more than 10 minutes..."
    )
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(f"          processing {network} ...")

            ids = []

            # get a list of queue items with processing >0
            for queue_item in tqdm.tqdm(
                get_from_localdb(
                    network=network,
                    collection="queue",
                    find={"processing": {"$gt": 0}},
                )
            ):
                # check seconds passed since processing
                minutes_passed = (time.time() - queue_item["processing"]) / 60
                if minutes_passed > 15:
                    # add item id to queue
                    ids.append(queue_item["id"])

                    # # free locked processing
                    # get_default_localdb(network=network).free_queue_item(
                    #     id=queue_item["id"]
                    # )
                    # logging.getLogger(__name__).debug(
                    #     f" {network}'s queue item {queue_item['id']} has been in the processing state for {minutes_passed} minutes. It probably halted. Freeing it..."
                    # )
            if ids:
                # free locked processing
                if db_return := get_default_localdb(network=network).free_queue_items(
                    ids=ids
                ):
                    logging.getLogger(__name__).info(
                        f" {network}'s queue items {db_return.modified_count} have been in the processing state for more than 15 minutes thus are now free."
                    )
