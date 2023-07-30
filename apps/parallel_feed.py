import logging
import time
import threading
from multiprocessing import Pool
from .feeds.queue import pull_from_queue

from bins.configuration import CONFIGURATION


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

    with Pool() as p:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    if len(PARALEL_TASKS) < maximum_tasks:
                        PARALEL_TASKS.append(
                            p.apply_async(
                                pull_from_queue,
                                (
                                    network,
                                    CONFIGURATION["_custom_"][
                                        "cml_parameters"
                                    ].queue_types,
                                ),
                            )
                        )
