import logging
import time
import threading
from multiprocessing import Pool
from apps.feeds.queue.pull import pull_from_queue

from bins.configuration import CONFIGURATION
from bins.general.enums import queueItemType


PARALEL_TASKS = []


def poll_results():
    while True:
        for task in PARALEL_TASKS[:]:
            if task.ready():
                # print("Task result:", task.get())
                PARALEL_TASKS.remove(task)


def process_all_queues(maximum_tasks: int = 10):
    poller_thread = threading.Thread(target=poll_results)
    poller_thread.start()

    logging.getLogger(__name__).info(
        f"Starting parallel feed with {maximum_tasks} tasks"
    )

    # create an ordered list of queue item types
    queue_items_list = create_priority_queueItemType()
    # queue_items_list = CONFIGURATION["_custom_"]["cml_parameters"].queue_types or list(
    #     queueItemType
    # )
    # # order by priority
    # queue_items_list.sort(key=lambda x: x.order, reverse=False)
    # set current queue item index
    current_queue_item_index = 0
    with Pool() as p:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )

                # select the next queue item type
                if current_queue_item_index < len(queue_items_list):
                    queue_items_task = queue_items_list[current_queue_item_index]
                    current_queue_item_index += 1
                else:
                    current_queue_item_index = 0
                    queue_items_task = queue_items_list[current_queue_item_index]

                # add queue item type to the queue, if possible
                for network in networks:
                    if len(PARALEL_TASKS) < maximum_tasks:
                        PARALEL_TASKS.append(
                            p.apply_async(
                                pull_from_queue,
                                (
                                    network,
                                    queue_items_task,
                                ),
                            )
                        )


def create_priority_queueItemType() -> list[list[queueItemType]]:
    # create an ordered list of queue item types
    queue_items_list = CONFIGURATION["_custom_"]["cml_parameters"].queue_types or list(
        queueItemType
    )

    custom_level0 = [
        queueItemType.OPERATION,
        queueItemType.BLOCK,
        queueItemType.HYPERVISOR_STATUS,
        queueItemType.HYPERVISOR_STATIC,
    ]
    types_combination = {
        queueItemType.OPERATION: [queueItemType.BLOCK],
        queueItemType.BLOCK: [queueItemType.OPERATION],
        queueItemType.HYPERVISOR_STATUS: [
            queueItemType.OPERATION,
            queueItemType.BLOCK,
            queueItemType.HYPERVISOR_STATIC,
        ],
        queueItemType.HYPERVISOR_STATIC: [
            queueItemType.OPERATION,
            queueItemType.BLOCK,
            queueItemType.PRICE,
        ],
        queueItemType.PRICE: custom_level0,
        queueItemType.LATEST_MULTIFEEDISTRIBUTION: custom_level0,
        queueItemType.REWARD_STATUS: custom_level0,
        queueItemType.REWARD_STATIC: [
            queueItemType.OPERATION,
            queueItemType.BLOCK,
        ],
    }

    # order by priority
    queue_items_list.sort(key=lambda x: x.order, reverse=False)

    result = []
    for queue_item in queue_items_list:
        if queue_item in types_combination:
            tmp_result = types_combination[queue_item]
            tmp_result.append(queue_item)
            result.append(tmp_result)

    return result
