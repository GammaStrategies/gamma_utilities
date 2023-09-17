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

    did_I_message = False

    # create an ordered list of queue item types
    queue_items_list = create_priority_queueItemType()
    # set current queue item index
    current_queue_item_index = create_index_per_network()
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
                        try:
                            # select the next queue item type
                            if current_queue_item_index[protocol][network] < len(
                                queue_items_list
                            ):
                                queue_items_task = queue_items_list[
                                    current_queue_item_index[protocol][network]
                                ]
                                current_queue_item_index[protocol][network] += 1
                            else:
                                current_queue_item_index[protocol][network] = 0
                                queue_items_task = queue_items_list[
                                    current_queue_item_index[protocol][network]
                                ]
                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error while calculating index for queueitem to process: {e} "
                            )
                            if not did_I_message:
                                logging.getLogger("telegram").info(
                                    f" Error while calculating index "
                                )
                                did_I_message = True

                        # add to the queue
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
    # order by priority
    queue_items_list.sort(key=lambda x: x.order, reverse=False)

    # queue is processed in creation order:
    #   Include for each queue item type the types that need to be processed before it
    types_combination = {
        queueItemType.OPERATION: [],
        queueItemType.BLOCK: [],
        queueItemType.HYPERVISOR_STATUS: [
            # queueItemType.BLOCK,
            # queueItemType.HYPERVISOR_STATIC,
            # queueItemType.PRICE,
        ],
        # only do price when price
        queueItemType.PRICE: [],
        queueItemType.LATEST_MULTIFEEDISTRIBUTION: [
            # queueItemType.BLOCK,
            # queueItemType.PRICE,
            # queueItemType.HYPERVISOR_STATUS,
            # queueItemType.REWARD_STATUS,
        ],
        queueItemType.REWARD_STATUS: [
            # queueItemType.BLOCK,
            # queueItemType.PRICE,
            # queueItemType.HYPERVISOR_STATUS,
            # queueItemType.REWARD_STATIC,
        ],
        # not used
        queueItemType.HYPERVISOR_STATIC: [],
        queueItemType.REWARD_STATIC: [
            # queueItemType.HYPERVISOR_STATIC
        ],
    }

    # build a result
    result = []
    for queue_item in queue_items_list:
        if queue_item in types_combination:
            tmp_result = types_combination[queue_item]
            tmp_result.append(queue_item)
            result.append(tmp_result)

    return result


def create_index_per_network() -> dict:
    result = {}

    for protocol in CONFIGURATION["script"]["protocols"]:
        if not protocol in result:
            result[protocol] = {}

        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            # create index
            result[protocol][network] = 0

    return result
