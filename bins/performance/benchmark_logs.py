# benchmark analysis
from datetime import datetime
import re


def analize_benchmark_log(log_file: str) -> dict | None:
    # result
    result = {
        "total_items": 0,
        "processing_time": 0,  # seconds
        "lifetime": 0,  # seconds
        "average_processing_time": 0,
        "average_lifetime": 0,
        "networks": {},
        "types": {},
        "timeframe": {
            "ini": None,
            "end": None,
        },
    }
    # regex
    regex_search = "(?P<datetime>\d{4}\D\d{2}\D\d{2}\s\d{2}\D\d{2}\D\d{2}\D\d{3}).*?\-\s\s(?P<network>.*?)\squeue\sitem\s(?P<type>.*)\sprocessing\stime\:\s(?P<processing>.*?)\sseconds\s\stotal\slifetime\:\s(?P<lifetime>.*?)\s(?P<lifetime_units>days|hours|seconds|weeks)"
    # find all matches in log file
    if matches := re.finditer(regex_search, log_file):
        # analyze
        for match in matches:
            # get matching vars
            # convert string datetime (2023-09-07 02:09:08,323) to datetime
            raw_dt = match.group("datetime")
            network = match.group("network")
            type = match.group("type")
            processing = match.group("processing")
            lifetime = match.group("lifetime")
            lifetime_units = match.group("lifetime_units")

            # add timeframe data
            dtime = datetime.strptime(raw_dt, "%Y-%m-%d %H:%M:%S,%f")
            if result["timeframe"]["ini"] is None or result["timeframe"]["ini"] > dtime:
                result["timeframe"]["ini"] = dtime
            if result["timeframe"]["end"] is None or result["timeframe"]["end"] < dtime:
                result["timeframe"]["end"] = dtime

            # convert lifetime to seconds
            if lifetime_units == "seconds":
                lifetime = float(lifetime)
            elif lifetime_units == "minutes":
                lifetime = float(lifetime) * 60
            elif lifetime_units == "hours":
                lifetime = float(lifetime) * 60 * 60
            elif lifetime_units == "days":
                lifetime = float(lifetime) * 60 * 60 * 24
            elif lifetime_units == "weeks":
                lifetime = float(lifetime) * 60 * 60 * 24 * 7

            # add network
            if network not in result["networks"]:
                result["networks"][network] = {
                    "total_items": 0,
                    "processing_time": 0,  # seconds
                    "average_processing_time": 0,
                    "average_lifetime": 0,
                    "lifetime": 0,  # seconds
                    "types": {},
                }

            # add types
            if type not in result["networks"][network]["types"]:
                result["networks"][network]["types"][type] = {
                    "total_items": 0,
                    "processing_time": 0,  # seconds
                    "lifetime": 0,  # seconds
                    "average_processing_time": 0,
                    "average_lifetime": 0,
                }
            if type not in result["types"]:
                result["types"][type] = {
                    "total_items": 0,
                    "processing_time": 0,  # seconds
                    "lifetime": 0,  # seconds
                    "average_processing_time": 0,
                    "average_lifetime": 0,
                }

            # add total items
            result["total_items"] += 1

            # add items
            result["networks"][network]["total_items"] += 1
            result["networks"][network]["types"][type]["total_items"] += 1
            result["types"][type]["total_items"] += 1

            # add processing time
            result["networks"][network]["processing_time"] += float(processing)
            result["networks"][network]["types"][type]["processing_time"] += float(
                processing
            )
            result["types"][type]["processing_time"] += float(processing)

            # add lifetime
            result["networks"][network]["lifetime"] += float(lifetime)
            result["networks"][network]["types"][type]["lifetime"] += float(lifetime)
            result["types"][type]["lifetime"] += float(lifetime)

        # logging.getLogger(__name__).debug(f" Calculating averages")
        # calculate averages
        for network in result["networks"]:
            result["networks"][network]["average_processing_time"] = (
                result["networks"][network]["processing_time"]
                / result["networks"][network]["total_items"]
            )
            result["networks"][network]["average_lifetime"] = (
                result["networks"][network]["lifetime"]
                / result["networks"][network]["total_items"]
            )
            for type in result["networks"][network]["types"]:
                result["networks"][network]["types"][type][
                    "average_processing_time"
                ] = (
                    result["networks"][network]["types"][type]["processing_time"]
                    / result["networks"][network]["types"][type]["total_items"]
                )
                result["networks"][network]["types"][type]["average_lifetime"] = (
                    result["networks"][network]["types"][type]["lifetime"]
                    / result["networks"][network]["types"][type]["total_items"]
                )

        for type in result["types"]:
            result["types"][type]["average_processing_time"] = (
                result["types"][type]["processing_time"]
                / result["types"][type]["total_items"]
            )
            result["types"][type]["average_lifetime"] = (
                result["types"][type]["lifetime"] / result["types"][type]["total_items"]
            )

        # totals
        result["processing_time"] = sum(
            [
                result["networks"][network]["processing_time"]
                for network in result["networks"]
            ]
        )
        result["lifetime"] = sum(
            [result["networks"][network]["lifetime"] for network in result["networks"]]
        )
        result["average_processing_time"] = (
            result["processing_time"] / result["total_items"]
            if result["total_items"]
            else 0
        )
        result["average_lifetime"] = (
            result["lifetime"] / result["total_items"] if result["total_items"] else 0
        )

    return result
