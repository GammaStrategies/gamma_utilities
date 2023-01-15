import getopt
import os
import yaml
import datetime as dt
from pathlib import Path


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

    # GET COMMAND LINE ARGUMENTS
    prmtrs = dict()  # the parameters we will pass

    try:
        opts, args = getopt.getopt(argv, "c:", ["config="])
    except getopt.GetoptError as err:
        print("             <filename>.py <options>")
        print("Options:")
        print(" -c <filename> or --config=<filename>")
        print(" ")
        print(" ")
        print(" ")
        print("to execute with custom configuration file:")
        print(
            "             <filename>.py -s <start date as %Y-%m-%d> -e <end date as %Y-%m-%d> -c <filename.yaml>"
        )
        print("error message: {}".format(err.msg))
        print("opt message: {}".format(err.opt))
        sys.exit(2)

    # loop and retrieve each command
    for opt, arg in opts:
        if opt in ("-c", "config="):
            # todo: check if it is a string   if isinstance(arg,str)
            # todo: check if file exists
            prmtrs["config_file"] = arg

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
