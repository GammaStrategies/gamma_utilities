import contextlib
import getopt
import os
import sys
import yaml
import datetime as dt
from pathlib import Path
from logging import Logger, getLogger
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


def convert_commandline_arguments(argv) -> dict:
    """converts command line arguments to a dictionary of those

    Arguments:
       argv {[type]} -- [description]

    Returns:
       dict -- [description]
    """

    # TODO: implement argparse

    # GET COMMAND LINE ARGUMENTS
    prmtrs = {}

    try:
        opts, args = getopt.getopt(
            argv, "", ["config=", "db_feed=", "service=", "network=", "protocol="]
        )

    except getopt.GetoptError as err:
        _print_command_line_help(err)
    # loop and retrieve each command
    for opt, arg in opts:
        if opt in ("--config"):
            # TODO: check if it is a string   if isinstance(arg,str)
            # TODO: check if file exists
            prmtrs["config_file"] = arg.strip()

        if opt in ("--db_feed"):
            # database feed
            prmtrs["db_feed"] = arg.strip()

        if opt in ("--service"):
            # service
            prmtrs["service"] = arg.strip()
            prmtrs["service_parameters"] = {}

        if opt in ("--network"):
            # network service
            prmtrs["network"] = arg.strip()

        if opt in ("--protocol"):
            # network service
            prmtrs["protocol"] = arg.strip()
    return prmtrs


def _print_command_line_help(err):
    print("             <filename>.py <options>")
    print("Options:")
    # config file
    print("Load custom config file:")
    print("    --config=<filename>")
    # database feeder:  -db_feed operations
    print("Feed database:")
    print("    --db_feed=<option>")
    print("    <option>: operations, status, static, prices ")
    # service
    print("Run in loop:")
    print(" --service=<option>")
    print("    <option>: global, local and network ")
    print("    <network> option needs: network= and protocol= ")
    print(" ")
    print(" ")
    print(" ")
    # examples
    print("to execute feed db with custom configuration file:")
    print("             <filename>.py --config <filename.yaml> --db_feed <option>")
    print(f"error message: {err.msg}")
    print(f"opt message: {err.opt}")
    sys.exit(2)


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
        return "{:,.2f} {}".format(_passed, _timelapse_unit)

    def stop(self):
        self.end = dt.datetime.now(dt.timezone.utc)
