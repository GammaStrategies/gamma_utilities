# number of parallel processes # script.queue_maximum_tasks
# number of cores
# machine hardware information ( cpu, memory, )

# script.min_loop_time ( on specific services )

from datetime import datetime, timezone
from apps.feeds.queue.queue_item import QueueItem
from bins.general.general_utilities import seconds_to_time_passed


class queue_processing_benchmark:
    def __init__(self, network: str, queue_item: QueueItem):
        self.queue_item = queue_item
        self.network = network
        self.start_time = datetime.timestamp(datetime.now(timezone.utc))

    def time_passed_processing(self) -> float:
        """Time passed since queue item processing

        Returns:
            float: seconds passed
        """
        return self.start_time - self.queue_item.processing

    def time_passed_lifetime(self) -> float:
        """Time passed since queue item creation

        Returns:
            float: seconds passed
        """
        return self.start_time - self.queue_item.creation

    def message_text(self) -> str:
        """Text to log about queue item processing"""
        return f" {self.network} queue item {self.queue_item.type}  processing time: {seconds_to_time_passed(self.time_passed_processing())}  total lifetime: {seconds_to_time_passed(self.time_passed_lifetime())}"
