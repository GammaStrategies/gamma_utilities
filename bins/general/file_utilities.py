import json
import sys
import os
import datetime as dt
import logging
import csv

from pathlib import Path


# ENCODER/DECODER to format json datetime items
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.isoformat()


class CustomDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.try_datetime, *args, **kwargs)

    @staticmethod
    def try_datetime(d):
        ret = {}
        for key, value in d.items():
            try:
                ret[key] = dt.datetime.fromisoformat(value)
            except (ValueError, TypeError):
                ret[key] = value
        return ret


# LOAD / SAVE JSON
def load_json(filename: str, folder_path: str):
    path_to_file = "{}/{}.json".format(folder_path, filename)  # full filename
    if os.path.exists(path_to_file):
        with open(path_to_file, "r") as f:
            try:
                return json.load(f, cls=CustomDecoder)
            except Exception as e:
                logging.getLogger(__name__).exception(
                    "Unexpected error while loading {} file    .error: {}".format(
                        path_to_file, sys.exc_info()[0]
                    )
                )
    return None


def save_json(filename: str, data, folder_path: str) -> bool:
    """Save json to file path

    Args:
       filename (str): file name
       data (_type_): json data
       folder_path (str): folder path name

    Returns:
       bool: Returns true when successfull
    """
    path_to_file = "{}/{}.json".format(folder_path, filename)  # full filename
    path_to_tempfile = "{}/{}.tmp".format(folder_path, filename)  # full filename
    # check if folder exists
    if not os.path.exists(folder_path):
        # Create a new directory
        os.makedirs(name=folder_path, exist_ok=True)

    # save it to temporary file
    with open(path_to_tempfile, "w") as f:
        try:
            json.dump(data, f, cls=CustomEncoder)
        except Exception as e:
            logging.getLogger(__name__).exception(
                "Unexpected error while saving {} file    .error: {}".format(
                    path_to_tempfile, sys.exc_info()[0]
                )
            )
            return False
        # remove old file
        try:
            os.remove(path_to_file)
        except FileNotFoundError:
            # file does not exist or something...
            pass
        except Exception:
            logging.getLogger(__name__).exception(
                "Unexpected error while deleting {} file    .error: {}".format(
                    path_to_file, sys.exc_info()[0]
                )
            )

        # rename tmp file
        try:
            os.rename(path_to_tempfile, path_to_file)
        except Exception as e:
            logging.getLogger(__name__).exception(
                "Unexpected error while renaming {} file    .error: {}".format(
                    path_to_tempfile, sys.exc_info()[0]
                )
            )
            return False

        return True

    return False


# SAVE CSV
def SaveCSV(filename, columns, rows):
    """Save multiple rows to CSV

    Arguments:
       rows {[type]} -- corresponding to fieldname headers defined like:
                       [{
                       'time': self.time,
                       'id': self.oid,
                       'side': self.side,
                       'price': self.price,
                       'size': self.size
                       }, ...]
    """
    my_file = Path(filename)
    if not my_file.is_file():
        with open(filename, "a") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()

    with open(filename, "a") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        for i in rows:
            writer.writerow(i)


def SaveCSV_row(filename, columns, row):
    """Save 1 row to CSV

    Arguments:
       row (dict)
    """

    my_file = Path(filename)
    # check if folder exists
    if not os.path.exists(my_file.parent):
        # Create a new directory
        os.makedirs(name=my_file.parent, exist_ok=True)

    if not my_file.is_file():
        with open(filename, "a") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()

    with open(filename, "a") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writerow(row)


# YIELD FILES IN SPECIFIED PATH
def get_files(path: str):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file
