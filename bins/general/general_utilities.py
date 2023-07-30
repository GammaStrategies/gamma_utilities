import contextlib
from functools import wraps
import logging
from logging import Logger, getLogger
import os
import yaml
import datetime as dt
from pathlib import Path
from typing import Iterable, Any, Tuple


log = getLogger(__name__)


# SCRIPT UTIL
def check_configuration_file(config_file):
    """Checks if self.configuration file has all fields correctly formateed
    Raises:
       Exception: [description]
    """

    if "logs" not in config_file:
        raise ValueError(
            "Configuration file is not configured correctly. 'logs' field is missing"
        )
    elif "path" not in config_file["logs"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'path' field is missing in logs"
        )
    elif "save_path" not in config_file["logs"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'save_path' field is missing in logs"
        )

    if "cache" not in config_file:
        raise ValueError(
            "Configuration file is not configured correctly. 'sources' field is missing"
        )
    elif "enabled" not in config_file["cache"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'enabled' field is missing in cache"
        )
    elif "save_path" not in config_file["cache"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'save_path' field is missing in cache"
        )

    if "sources" not in config_file:
        raise ValueError(
            "Configuration file is not configured correctly. 'sources' field is missing"
        )
    elif "api_keys" not in config_file["sources"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'api_keys' field is missing in sources"
        )
    elif "etherscan" not in config_file["sources"]["api_keys"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'etherscan' field is missing in sources.api_keys"
        )
    elif "polygonscan" not in config_file["sources"]["api_keys"]:
        raise ValueError(
            "Configuration file is not configured correctly. 'polygonscan' field is missing in sources.api_keys"
        )


def load_configuration(cfg_name="config.yaml"):
    """Load and return configuration object
       "config.yaml" file should be placed in root

    Returns:
       [configuration object]
    """
    if os.path.exists(cfg_name):
        with open(cfg_name, "rt", encoding="utf8") as f:
            try:
                return yaml.safe_load(f.read())
            except Exception as e:
                print(f"Error in Logging Configuration: {e}")
    else:
        print(f" {cfg_name} configuration file not found")

    raise FileNotFoundError(f" {cfg_name} configuration file not found")


## LIST STUFF
def differences(list1: list, list2: list) -> list:
    """Return differences between lists

    Arguments:
       list1 {list} -- [description]
       list2 {list} -- [description]

    Returns:
       list -- the difereences
    """
    return list(set(list1) - set(list2))


def equalities(list1: list, list2: list) -> list:
    """Return equalities between lists

    Arguments:
       list1 {list} -- [description]
       list2 {list} -- [description]

    Returns:
       list -- the difereences
    """
    return [itm for itm in list1 if itm in list2]


def signal_last(it: Iterable[Any]) -> Iterable[Tuple[bool, Any]]:
    """Iterate thru elements returning a bool indicating if this is the last item of the iterable and the iterated item

        credit: https://betterprogramming.pub/is-this-the-last-element-of-my-python-for-loop-784f5ff90bb5

    Args:
        it (Iterable[Any]):

    Returns:
        Iterable[Tuple[bool, Any]]:

    Yields:
        Iterator[Iterable[Tuple[bool, Any]]]:
    """

    iterable = iter(it)
    ret_var = next(iterable)
    for val in iterable:
        yield False, ret_var
        ret_var = val
    yield True, ret_var


def signal_first(it: Iterable[Any]) -> Iterable[Tuple[bool, Any]]:
    """Iterate thru elements returning a bool indicating if this is the first item of the iterable and the iterated item

        credit: https://betterprogramming.pub/is-this-the-last-element-of-my-python-for-loop-784f5ff90bb5
    Args:
        it (Iterable[Any]):

    Returns:
        Iterable[Tuple[bool, Any]]:

    Yields:
        Iterator[Iterable[Tuple[bool, Any]]]:
    """

    iterable = iter(it)
    yield True, next(iterable)
    for val in iterable:
        yield False, val


def get_missing_integers(data: list[int], interval: int = 1200) -> list[int]:
    """Given a list of integers, return a list of missing integers following a given interval
         The interval shall be 2 times greater than the defined so that it fits between the data items already in the list

    Args:
        data (list[int]):
        interval (int, optional):  . Defaults to 1200.

    Returns:
        list[int]: _description_
    """
    # Initialize the result list
    result_list = []

    for i, item in enumerate(data[:-1]):
        if data[i + 1] - item > interval * 2:
            # calculate how many items are missing between the current and the next item
            missing_items = (data[i + 1] - item) // interval
            # add the missing items to the result list
            for j in range(1, missing_items):
                result_list.append(item + interval * j)

    return result_list


# DATETIME
def convert_string_datetime(string: str) -> dt.datetime:
    if string.lower().strip() == "now":
        return dt.datetime.now(dt.timezone.utc)
        # POSIBILITY 01
    with contextlib.suppress(Exception):
        return dt.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S")
        # POSIBILITY 02
    with contextlib.suppress(Exception):
        return dt.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%fZ")
        # POSIBILITY 03
    with contextlib.suppress(Exception):
        return dt.datetime.strptime(string, "%Y-%m-%d")


class time_controller:
    def __init__(self, seconds_frame: int = 60):
        """

        Args:
           seconds_frame (int, optional):   . Defaults to 60.
        """
        # define time control var
        self.lastupdate = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=8)
        # set timespan to be controlling
        self.timespan_secs = seconds_frame

    def hit(self) -> int:
        """save current datetime and retrieve seconds passed

        Returns:
           [int] -- total seconds passed since last hit ( or since creation if no last hit)
        """
        now = dt.datetime.now(dt.timezone.utc)
        result = now - self.lastupdate
        # update last time
        self.lastupdate = now

        # return time passed
        return result.total_seconds()

    def has_time_passed(self) -> bool:
        """Has defined time passed

        Returns:
           bool --
        """
        return (
            dt.datetime.now(dt.timezone.utc) - self.lastupdate
        ).total_seconds() > self.timespan_secs


class log_time_passed:
    def __init__(self, fName="", callback: Logger = None):
        self.start = dt.datetime.now(dt.timezone.utc)
        self.end = None
        self.fName = fName
        self._callback: Logger = callback

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # xception handling here
        if self._callback is not None:
            self._callback.debug(
                f" took {self.get_timepassed_string(self.start,self.end)} to complete {self.fName}"
            )

    @staticmethod
    def get_timepassed_string(
        start_time: dt.datetime, end_time: dt.datetime = None
    ) -> str:
        if not end_time:
            end_time = dt.datetime.now(dt.timezone.utc)
        _timelapse = end_time - start_time
        _passed = _timelapse.total_seconds()
        if _passed < 60:
            _timelapse_unit = "seconds"
        elif _passed < 60 * 60:
            _timelapse_unit = "minutes"
            _passed /= 60
        elif _passed < 60 * 60 * 24:
            _timelapse_unit = "hours"
            _passed /= 60 * 60
        else:
            _timelapse_unit = "days"
            _passed /= 60 * 60 * 24

        return "{:,.2f} {}".format(_passed, _timelapse_unit)

    def stop(self):
        self.end = dt.datetime.now(dt.timezone.utc)


# TIME LOG DECORATOR
def log_execution_time(f):
    """Decorator to log execution time of a function

    Args:
        f (function): function to be decorated

    """
    from bins.configuration import CONFIGURATION

    # check if enabled in configuration
    if not CONFIGURATION["logs"]["log_execution_time"]:
        return f

    @wraps(f)
    def wrapper(*args, **kwargs):
        start_time = dt.datetime.now(dt.timezone.utc)
        result = f(*args, **kwargs)
        end_time = dt.datetime.now(dt.timezone.utc)
        _timelapse = end_time - start_time
        _passed = _timelapse.total_seconds()

        logging.getLogger(__name__).debug(
            f"{f.__name__} took {seconds_to_time_passed(_passed)} to complete"
        )
        return result

    return wrapper


def seconds_to_time_passed(seconds: float) -> str:
    """_summary_

    Args:
        seconds (float): _description_

    Returns:
        str: _description_
    """

    if seconds < 60:
        _timelapse_unit = "seconds"
    elif seconds < 60 * 60:
        _timelapse_unit = "minutes"
        seconds /= 60
    elif seconds < 60 * 60 * 24:
        _timelapse_unit = "hours"
        seconds /= 60 * 60
    else:
        _timelapse_unit = "days"
        seconds /= 60 * 60 * 24
    return f"{round(seconds,2)} {_timelapse_unit}"
