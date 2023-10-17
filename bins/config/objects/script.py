from dataclasses import dataclass


@dataclass
class config_queue:
    parallel_tasks: int = 1  # maximum number of parallel queue tasks to run at once

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        return self.__dict__.copy()


@dataclass
class config_script:
    min_loop_time: int = 5  # minimum cost for the loop process in number of minutes to wait for ( loop at min. every X minutes) usefull to reduce web3 calls
    queue: config_queue = None
    coingeko_api_key: str | None = None

    def __post_init__(self):
        if isinstance(self.queue, dict):
            self.queue = config_queue(**self.queue)

        # init queue
        if not self.queue:
            self.queue = config_queue()

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        _dict = self.__dict__.copy()
        _dict["queue"] = self.queue.to_dict()
        return _dict
