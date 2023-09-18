import logging
import time
import threading
from multiprocessing import Pool
from apps.feeds.queue.pull import pull_from_queue
from apps.feeds.queue.queue_item import create_selector_per_network, queue_item_selector

from bins.configuration import CONFIGURATION
from bins.general.enums import queueItemType


PARALEL_TASKS = []


### Process all queues in parallel ###


def poll_results():
    while True:
        for task in PARALEL_TASKS[:]:
            if task.ready():
                # print("Task result:", task.get())
                PARALEL_TASKS.remove(task)


def process_queues(
    maximum_tasks: int = 10,
    item_selector_per_network: dict[str, dict[queue_item_selector]] | None = None,
):
    """_summary_

    Args:
        maximum_tasks (int, optional): . Defaults to 10.
        item_selector_per_network (dict[str, dict[queue_item_selector]] | None, optional): check create_selector_per_network function . Defaults to None.
    """
    poller_thread = threading.Thread(target=poll_results)
    poller_thread.start()

    logging.getLogger(__name__).info(
        f"Starting parallel feed with {maximum_tasks} tasks"
    )

    # create queue item selector per network
    if not item_selector_per_network:
        logging.getLogger(__name__).debug(
            f" Creating a default queue item selector per network"
        )
        item_selector_per_network = create_selector_per_network()

    with Pool() as p:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )

                # add queue item type to the queue, if possible
                for network in networks:
                    if len(PARALEL_TASKS) < maximum_tasks:
                        # select current item
                        queue_item_selector = item_selector_per_network[protocol][
                            network
                        ]

                        # add to the queue
                        PARALEL_TASKS.append(
                            p.apply_async(
                                pull_from_queue,
                                (
                                    network,
                                    queue_item_selector.current_queue_item_types,
                                    queue_item_selector.find,
                                    queue_item_selector.sort,
                                ),
                            )
                        )

                        # select nex item type
                        queue_item_selector.next()


def select_process_queues(maximum_tasks: int = 10, queue_level: int | None = None):
    """Select the level of queue to process:
            level 0 will scrape queue items with max priority to count 0
            level 1 will scrape queue items with count >0 ( and max priority lower count )
            level 2 will scrape queue items with count >1 ( and max priority lower count )
            ...

    Args:
        maximum_tasks (int, optional): . Defaults to 10.
        queue_level (int, optional): . Defaults to 0.
    """
    # maximum count variable to process when queue level is > 0
    maximum_count = 5
    # create queue item selector per network
    item_selector_per_network = None

    if queue_level:
        logging.getLogger(__name__).warning(
            f" >>>>>>>> Queue items with {queue_level} or more errors will be processed (below {queue_level} errors will not be processed at all) <<<<<<<<<"
        )
        # create queue item selector per network
        item_selector_per_network = create_selector_per_network(
            queue_items_list=None,
            find={
                "processing": 0,
                "$and": [
                    {"count": {"$gte": queue_level}},
                    {"count": {"$lte": maximum_count}},
                ],
            },
            sort=[("count", 1), ("creation", 1)],
        )

    # process queues
    process_queues(
        maximum_tasks=maximum_tasks, item_selector_per_network=item_selector_per_network
    )
