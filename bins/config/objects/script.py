from dataclasses import dataclass


@dataclass
class config_queue:
    parallel_tasks: int = 1  # maximum number of parallel queue tasks to run at once


@dataclass
class config_script:
    min_loop_time: int = 5  # minimum cost for the loop process in number of minutes to wait for ( loop at min. every X minutes) usefull to reduce web3 calls
    queue: config_queue = None

    def __post_init__(self):
        if isinstance(self.queue, dict):
            self.queue = config_queue(**self.queue)

        # init queue
        if not self.queue:
            self.queue = config_queue()
