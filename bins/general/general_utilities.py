import contextlib
from functools import wraps
import logging
from logging import Logger, getLogger
import os
import yaml
import datetime as dt
from pathlib import Path
from typing import Iterable, Any, Tuple
from dateutil.relativedelta import relativedelta
from collections.abc import MutableMapping

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


def identify_me() -> str:
    """Identify this computer with any info available"""

    try:
        return os.path.expanduser("~")
    except Exception as e:
        pass

    try:
        return os.getlogin()
    except Exception as e:
        pass

    return "unknown"


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


def create_chunks(
    min: int, max: int, chunk_size: int, allow_repeat: bool = False
) -> list[tuple[int, int]]:
    """build chunks of data

    Args:
        min (int): minimum value
        max (int): maximum value
        chunk_size (int): size of each chunk
        allow_repeat (bool, optional): allow to repeat the max last value at the initial next chunk. ( needed in database APR calc...). Defaults to False.

    Returns:
        list: list of tuples with chunk (start:int, end:int)
    """
    result = []
    # create chunks
    for i in range(min, max, chunk_size):
        # add chunk to list if it is not the last one
        if i + chunk_size < max:
            if allow_repeat:
                result.append((i, i + chunk_size))
            else:
                (
                    result.append((i, i + chunk_size))
                    if len(result) == 0
                    else result.append((i + 1, i + chunk_size))
                )
        else:
            if allow_repeat:
                result.append((i, max))
            else:
                (
                    result.append((i, max))
                    if len(result) == 0
                    else result.append((i + 1, max))
                )
    return result


def convert_keys_to_str(iterable):
    """
    dict_rename_key method

    Args:
        iterable (dict):
        old_key (string):
        new_key (string):

    Returns:
        dict:

    """
    if isinstance(iterable, dict):
        for key in list(iterable.keys()):
            if not isinstance(key, str):
                iterable[str(key)] = iterable.pop(key)

            if isinstance(iterable[str(key)], (dict, list)):
                iterable[str(key)] = convert_keys_to_str(iterable[str(key)])
    elif isinstance(iterable, list):
        for i in range(len(iterable)):
            iterable[i] = convert_keys_to_str(iterable[i])

    return iterable


# DATETIME
def convert_string_datetime(string: str) -> dt.datetime:
    """Convert to UTC

    Args:
        string (str):

    Returns:
        dt.datetime:
    """
    if string.lower().strip() == "now":
        return dt.datetime.now(dt.timezone.utc)

    # POSIBILITY 01
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string + " +0000", "%Y-%m-%d %H:%M:%S %z")
        return _datetime
    # POSIBILITY 01.1
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string + " +0000", "%Y-%m-%dT%H:%M:%S %z")
        return _datetime

    # POSIBILITY 02
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string, "%Y-%m-%d %H:%M:%S.%fZ")
        logging.getLogger(__name__).debug(f" {string} converted to {_datetime}")
        # convert timezone to utc
        _datetime = _datetime.replace(tzinfo=dt.timezone.utc)
        logging.getLogger(__name__).debug(f"  Changed to utc {_datetime}")
        return _datetime
    # POSIBILITY 02.1
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%fZ")
        logging.getLogger(__name__).debug(f" {string} converted to {_datetime}")
        # convert timezone to utc
        _datetime = _datetime.replace(tzinfo=dt.timezone.utc)
        logging.getLogger(__name__).debug(f"  Changed to utc {_datetime}")
        return _datetime

    # POSIBILITY 03
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string + " +0000", "%Y-%m-%d %z")
        return _datetime

    # POSIBILITY 04
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string + " +0000", "%Y-%m-%d %H:%M:%S.%f %z")
        return _datetime
    # POSIBILITY 04.1
    with contextlib.suppress(Exception):
        _datetime = dt.datetime.strptime(string + " +0000", "%Y-%m-%dT%H:%M:%S.%f %z")
        return _datetime

    raise ValueError(f"Can't convert string to datetime: {string}")


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


def get_last_timestamp_of_month(year: int, month: int) -> float:
    """Get last timestamp of a given month

    Args:
        year (int): year
        month (int): month

    Returns:
        int: timestamp
    """

    return (
        dt.datetime(year=year, month=month, day=1, tzinfo=dt.timezone.utc)
        + relativedelta(months=1)
        - relativedelta(seconds=1)
    ).timestamp()


def get_last_timestamp_of_day(timestamp: int, tz: dt.timezone | None = None) -> float:
    """Get last timestamp of the timestamp given day

    Args:
        timestamp (int): timestamp ( can be any time of the day)
        tz (dt.timezone | None, optional): timezone. Defaults to UTC.

    Returns:
        int: timestamp
    """
    # set timezone
    tz = tz or dt.timezone.utc
    return (
        dt.datetime.fromtimestamp(timestamp, tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        + relativedelta(days=1)
        - relativedelta(seconds=1)
    ).timestamp()


# DICT CONVERSION
def flatten_dict(
    d: MutableMapping, parent_key: str = "", sep: str = "."
) -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


# PROCESSES UTILS
from signal import signal, SIGINT


def initializer():
    signal(SIGINT, lambda: None)


# MATH UTILS
import math

millnames = ["", "k", "M", "B", "T"]


def millify(n):
    n = float(n)
    millidx = max(
        0,
        min(
            len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))
        ),
    )

    return "{:.0f}{}".format(n / 10 ** (3 * millidx), millnames[millidx])
