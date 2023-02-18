import getopt
import os
import sys
import yaml
import datetime as dt
from pathlib import Path
from logging import Logger, getLogger

log = getLogger(__name__)

# SCRIPT UTIL
def check_configuration_file(config_file):
    """Checks if self.configuration file has all fields correctly formateed
    Raises:
       Exception: [description]
    """

    if not "logs" in config_file:
        raise Exception(
            "Configuration file is not configured correctly. 'logs' field is missing"
        )
    elif not "path" in config_file["logs"]:
        raise Exception(
            "Configuration file is not configured correctly. 'path' field is missing in logs"
        )
    elif not "save_path" in config_file["logs"]:
        raise Exception(
            "Configuration file is not configured correctly. 'save_path' field is missing in logs"
        )

    if not "cache" in config_file:
        raise Exception(
            "Configuration file is not configured correctly. 'sources' field is missing"
        )
    elif not "enabled" in config_file["cache"]:
        raise Exception(
            "Configuration file is not configured correctly. 'enabled' field is missing in cache"
        )
    elif not "save_path" in config_file["cache"]:
        raise Exception(
            "Configuration file is not configured correctly. 'save_path' field is missing in cache"
        )

    if not "sources" in config_file:
        raise Exception(
            "Configuration file is not configured correctly. 'sources' field is missing"
        )
    elif not "api_keys" in config_file["sources"]:
        raise Exception(
            "Configuration file is not configured correctly. 'api_keys' field is missing in sources"
        )
    elif not "etherscan" in config_file["sources"]["api_keys"]:
        raise Exception(
            "Configuration file is not configured correctly. 'etherscan' field is missing in sources.api_keys"
        )
    elif not "polygonscan" in config_file["sources"]["api_keys"]:
        raise Exception(
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
                print("Error in Logging Configuration: {}".format(e))
    else:
        print(" {} configuration file not found".format(cfg_name))


def convert_commandline_arguments(argv) -> dict:
    """converts command line arguments to a dictionary of those

    Arguments:
       argv {[type]} -- [description]

    Returns:
       dict -- [description]
    """

    # TODO: implement argparse

    # GET COMMAND LINE ARGUMENTS
    prmtrs = dict()  # the parameters we will pass

    try:
        opts, args = getopt.getopt(
            argv, "", ["config=", "db_feed=", "service=", "network=", "protocol="]
        )

    except getopt.GetoptError as err:
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
        print("error message: {}".format(err.msg))
        print("opt message: {}".format(err.opt))
        sys.exit(2)

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
    result = list()

    for itm in list1:
        if itm in list2:
            result.append(itm)

    return result


# DATETIME
def convert_string_datetime(string: str) -> dt.datetime:

    if string.lower().strip() == "now":
        return dt.datetime.utcnow()
    else:
        # POSIBILITY 01
        try:
            return dt.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S")
        except:
            pass
        # POSIBILITY 02
        try:
            return dt.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            pass


class time_controller:
    def __init__(self, seconds_frame: int = 60):
        """

        Args:
           seconds_frame (int, optional):   . Defaults to 60.
        """
        # define time control var
        self.lastupdate = dt.datetime.utcnow() - dt.timedelta(hours=8)
        # set timespan to be controlling
        self.timespan_secs = seconds_frame

    def hit(self) -> int:
        """save current datetime and retrieve seconds passed

        Returns:
           [int] -- total seconds passed since last hit ( or since creation if no last hit)
        """
        now = dt.datetime.utcnow()
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
            dt.datetime.utcnow() - self.lastupdate
        ).total_seconds() > self.timespan_secs


class log_time_passed:
    def __init__(self, fName="", callback: Logger = None):
        self.start = dt.datetime.utcnow()
        self.end = None
        self.fName = fName
        self._callback: Logger = callback

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # xception handling here
        if not self._callback == None:
            self._callback.debug(
                f" took {self.get_timepassed_string(self.start,self.end)} to complete {self.fName}"
            )

    @staticmethod
    def get_timepassed_string(
        start_time: dt.datetime, end_time: dt.datetime = None
    ) -> str:
        if not end_time:
            end_time = dt.datetime.utcnow()
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
        self.end = dt.datetime.utcnow()
