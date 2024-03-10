import logging
from apps.repair.prices.logs import get_all_logfiles, load_logFile

from bins.general import general_utilities
from bins.performance.benchmark_logs import (
    analyze_benchmark_info_log,
    analyze_benchmark_log,
)


def benchmark_logs_analysis():
    # get all log files
    log_files = get_all_logfiles(log_names=["benchmark"])
    logging.getLogger(__name__).info(f"Processing {len(log_files)} log files")
    # create aggregated data
    aggregated_data = []
    timeframe = {
        "ini": None,
        "end": None,
    }
    aggregated_types = {}
    aggregated_networks = {}
    # process logs
    for log_file in log_files:
        # analyze log file
        logging.getLogger(__name__).debug(f" analyzing {log_file}")
        if result := analyze_benchmark_log(log_file=load_logFile(log_file)):
            # check if there is data in result
            if not result["total_items"] > 0:
                logging.getLogger(__name__).debug(
                    f"    - no data in {log_file}  [skipping]"
                )
                continue

            # add raw to result
            aggregated_data.append(result)

            # calculate items per day
            total_seconds_in_period = (
                result["timeframe"]["end"] - result["timeframe"]["ini"]
            ).total_seconds()
            total_items_x_second = (
                result["total_items"] / total_seconds_in_period
                if total_seconds_in_period > 0
                else 0
            )
            total_items_x_day = total_items_x_second * 60 * 60 * 24
            items_x_month = total_items_x_day * 30

            if (
                timeframe["ini"] is None
                or timeframe["ini"] > result["timeframe"]["ini"]
            ):
                timeframe["ini"] = result["timeframe"]["ini"]
            if (
                timeframe["end"] is None
                or timeframe["end"] < result["timeframe"]["end"]
            ):
                timeframe["end"] = result["timeframe"]["end"]

            # summary log
            logging.getLogger(__name__).info(
                f"    - {total_items_x_day:,.0f} it/day [ processed {result['total_items']} in {general_utilities.log_time_passed.get_timepassed_string(result['timeframe']['ini'],result['timeframe']['end'])} from {result['timeframe']['ini']} to {result['timeframe']['end']}] "
            )
            # per type log ( log averagee per type )
            for type in result["types"]:
                percentage = (
                    result["types"][type]["total_items"] / result["total_items"]
                    if result["total_items"] > 0
                    else 0
                )
                logging.getLogger(__name__).info(
                    f"        - type {type} -> {result['types'][type]['average_processing_time']:,.0f} sec. [ processed {result['types'][type]['total_items']:,.0f}  {percentage:,.0%} of total  ] "
                )

                # add to aggregated types
                if type not in aggregated_types:
                    aggregated_types[type] = {
                        "total_items": 0,
                        "total_processing_time": 0,
                    }
                aggregated_types[type]["total_items"] += result["types"][type][
                    "total_items"
                ]
                aggregated_types[type]["total_processing_time"] += result["types"][
                    type
                ]["processing_time"]

            # per network log ( log averagee per network )
            for network in result["networks"]:
                percentage = (
                    result["networks"][network]["total_items"] / result["total_items"]
                    if result["total_items"] > 0
                    else 0
                )
                logging.getLogger(__name__).info(
                    f"        - chain {network} -> {result['networks'][network]['average_processing_time']:,.0f} sec. [ processed {result['networks'][network]['total_items']:,.0f}  {percentage:,.0%} of total] "
                )

                # add to aggregated networks
                if network not in aggregated_networks:
                    aggregated_networks[network] = {
                        "total_items": 0,
                        "total_processing_time": 0,
                    }
                aggregated_networks[network]["total_items"] += result["networks"][
                    network
                ]["total_items"]
                aggregated_networks[network]["total_processing_time"] += result[
                    "networks"
                ][network]["processing_time"]

    # get information from info logs regarding all processed items
    all_processed_items_data = benchmark_logs_analysis_get_processing()

    # calculate items per day
    total_seconds_in_period = (
        (timeframe["end"] - timeframe["ini"]).total_seconds()
        if timeframe["ini"] and timeframe["end"]
        else 0
    )
    aggregated_items = sum([x["total_items"] for x in aggregated_data])
    aggregated_items_x_second = (
        (aggregated_items / total_seconds_in_period)
        if total_seconds_in_period > 0
        else 0
    )
    aggregated_items_x_day = aggregated_items_x_second * 60 * 60 * 24
    aggregated_items_x_month = aggregated_items_x_day * 30

    # calculate real processed items
    try:
        processed_items_succesfully = (
            aggregated_items / all_processed_items_data["aggregated_items"]
            if all_processed_items_data["aggregated_items"] > 0
            else 0
        )
        processed_items_succesfully_day = (
            aggregated_items_x_day / all_processed_items_data["aggregated_items_x_day"]
            if all_processed_items_data["aggregated_items_x_day"] > 0
            else 0
        )
        processed_items_succesfully_month = (
            aggregated_items_x_month
            / all_processed_items_data["aggregated_items_x_month"]
            if all_processed_items_data["aggregated_items_x_month"] > 0
            else 0
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error calculating processed_items_succesfully: {e}"
        )
        processed_items_succesfully = 0
        processed_items_succesfully_day = 0
        processed_items_succesfully_month = 0

    # log aggregated data
    logging.getLogger(__name__).info(
        f"Aggregated data from {timeframe['ini']} to {timeframe['end']}"
    )
    logging.getLogger(__name__).info(
        f"    - {len(aggregated_data)} log files processed"
    )
    logging.getLogger(__name__).info(
        f"    - successfully processed {aggregated_items:,.0f} items of {all_processed_items_data['aggregated_items']:,.0f} [{processed_items_succesfully:,.0%} success rate]"
    )
    logging.getLogger(__name__).info(
        f"    - calculated {aggregated_items_x_day:,.0f} items per day [{processed_items_succesfully_day:,.0%}]"
    )
    logging.getLogger(__name__).info(
        f"    - calculated {aggregated_items_x_month:,.0f} items per month  [{processed_items_succesfully_month:,.0%}]"
    )

    # log aggregated types
    logging.getLogger(__name__).info(f"    - calculated items by type")
    for type, values in aggregated_types.items():
        it_x_sec = (
            values["total_items"] / values["total_processing_time"]
            if values["total_processing_time"] > 0
            else 0
        )
        sec_x_it = (
            values["total_processing_time"] / values["total_items"]
            if values["total_items"] > 0
            else 0
        )
        # it_x_day = it_x_sec * 60 * 60 * 24
        percentage = (
            values["total_items"] / aggregated_items if aggregated_items > 0 else 0
        )

        # check if all_processed_items_data has data with regards to this type ( it should )
        percentage_succesfully = 0
        if type in all_processed_items_data["aggregated_types"]:
            try:
                percentage_succesfully = (
                    values["total_items"]
                    / all_processed_items_data["aggregated_types"][type]["total_items"]
                    if all_processed_items_data["aggregated_types"][type]["total_items"]
                    > 0
                    else 0
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error calculating percentage_succesfully for type {type}: {e}"
                )

        logging.getLogger(__name__).info(
            f"        type {type} ->  {sec_x_it:,.0f} sec/it [ processed {values['total_items']:,.0f} -> {percentage:,.0%} of total successfull] [{percentage_succesfully:,.0%} success rate]"
        )

    # log aggregated networks
    logging.getLogger(__name__).info(f"    - calculated items by network")
    for network, values in aggregated_networks.items():
        it_x_sec = (
            values["total_items"] / values["total_processing_time"]
            if values["total_processing_time"] > 0
            else 0
        )
        sec_x_it = (
            values["total_processing_time"] / values["total_items"]
            if values["total_items"] > 0
            else 0
        )
        # it_x_day = it_x_sec * 60 * 60 * 24
        percentage = (
            values["total_items"] / aggregated_items if aggregated_items > 0 else 0
        )

        # check if all_processed_items_data has data with regards to this network ( it should )
        if network in all_processed_items_data["aggregated_networks"]:
            try:
                percentage_succesfully = (
                    values["total_items"]
                    / all_processed_items_data["aggregated_networks"][network][
                        "total_items"
                    ]
                    if all_processed_items_data["aggregated_networks"][network][
                        "total_items"
                    ]
                    > 0
                    else 0
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error calculating percentage_succesfully for network {network}: {e}"
                )
                percentage_succesfully = 0

        logging.getLogger(__name__).info(
            f"        network {network} -> {sec_x_it:,.0f} sec/it [ processed {values['total_items']:,.0f} -> {percentage:,.0%} of total successfull]  [{percentage_succesfully:,.0%} success rate]"
        )


def benchmark_logs_analysis_get_processing() -> dict:
    """Get all processed ( not finished ) items from info logs ( useful to later compare it with benchmark logs)

    Returns:
        dict: {
            "aggregated_data": <list of dict>,
            "timeframe": {
                "ini": <datetime>,
                "end": <datetime>,
            },
            "aggregated_types": {
                <type>: {
                    "total_items": <int>,
                    "aggregated_count": <int>,
                }
            },
            "aggregated_networks": {
                <network>: {
                    "total_items": <int>,
                    "aggregated_count": <int>,
                }
            },
            "aggregated_items": <int> total items processed within the timeframe,
            "aggregated_items_x_second": <int> items processed per second,
            "aggregated_items_x_day": <int> items processed per day,
            "aggregated_items_x_month": <int> items processed per month,
        }
    """

    logging.getLogger(__name__).debug(
        f" Getting all processed ( not finished ) items from info logs"
    )
    # get all log files
    log_files = get_all_logfiles(log_names=["info"])

    # create aggregated data
    aggregated_data = []
    timeframe = {
        "ini": None,
        "end": None,
    }
    aggregated_types = {}
    aggregated_networks = {}
    # process logs
    for log_file in log_files:
        # analyze log file
        logging.getLogger(__name__).debug(f" analyzing {log_file}")
        if result := analyze_benchmark_info_log(log_file=load_logFile(log_file)):
            # check if there is data in result
            if not result["total_items"] > 0:
                logging.getLogger(__name__).debug(
                    f"    - no data in {log_file}  [skipping]"
                )
                continue

            # add raw to result
            aggregated_data.append(result)

            # calculate items per day
            total_seconds_in_period = (
                result["timeframe"]["end"] - result["timeframe"]["ini"]
            ).total_seconds()
            total_items_x_second = (
                result["total_items"] / total_seconds_in_period
                if total_seconds_in_period > 0
                else 0
            )
            total_items_x_day = total_items_x_second * 60 * 60 * 24
            items_x_month = total_items_x_day * 30

            if (
                timeframe["ini"] is None
                or timeframe["ini"] > result["timeframe"]["ini"]
            ):
                timeframe["ini"] = result["timeframe"]["ini"]
            if (
                timeframe["end"] is None
                or timeframe["end"] < result["timeframe"]["end"]
            ):
                timeframe["end"] = result["timeframe"]["end"]

            # summary log
            # logging.getLogger(__name__).info(
            #     f"    - {total_items_x_day:,.0f} it/day [ processed {result['total_items']} in {general_utilities.log_time_passed.get_timepassed_string(result['timeframe']['ini'],result['timeframe']['end'])} from {result['timeframe']['ini']} to {result['timeframe']['end']}] "
            # )
            # per type log ( log averagee per type )
            for type in result["types"]:
                percentage = (
                    result["types"][type]["total_items"] / result["total_items"]
                    if result["total_items"] > 0
                    else 0
                )
                # logging.getLogger(__name__).info(
                #     f"        - type {type} -> {result['types'][type]['average_processing_time']:,.0f} sec. [ processed {result['types'][type]['total_items']:,.0f}  {percentage:,.0%} of total  ] "
                # )

                # add to aggregated types
                if type not in aggregated_types:
                    aggregated_types[type] = {
                        "total_items": 0,
                        "aggregated_count": 0,
                    }
                aggregated_types[type]["total_items"] += result["types"][type][
                    "total_items"
                ]
                aggregated_types[type]["aggregated_count"] += result["types"][type][
                    "aggregated_count"
                ]

            # per network log ( log averagee per network )
            for network in result["networks"]:
                percentage = (
                    result["networks"][network]["total_items"] / result["total_items"]
                    if result["total_items"] > 0
                    else 0
                )
                # logging.getLogger(__name__).info(
                #     f"        - chain {network} -> {result['networks'][network]['average_processing_time']:,.0f} sec. [ processed {result['networks'][network]['total_items']:,.0f}  {percentage:,.0%} of total] "
                # )

                # add to aggregated networks
                if network not in aggregated_networks:
                    aggregated_networks[network] = {
                        "total_items": 0,
                        "aggregated_count": 0,
                    }
                aggregated_networks[network]["total_items"] += result["networks"][
                    network
                ]["total_items"]
                aggregated_networks[network]["aggregated_count"] += result["networks"][
                    network
                ]["aggregated_count"]

    # calculate items per day
    total_seconds_in_period = (
        (timeframe["end"] - timeframe["ini"]).total_seconds()
        if timeframe["ini"] and timeframe["end"]
        else 0
    )
    aggregated_items = sum([x["total_items"] for x in aggregated_data])
    aggregated_items_x_second = (
        (aggregated_items / total_seconds_in_period)
        if total_seconds_in_period > 0
        else 0
    )
    aggregated_items_x_day = aggregated_items_x_second * 60 * 60 * 24
    aggregated_items_x_month = aggregated_items_x_day * 30

    return {
        "aggregated_data": aggregated_data,
        "timeframe": timeframe,
        "aggregated_types": aggregated_types,
        "aggregated_networks": aggregated_networks,
        "aggregated_items": aggregated_items,
        "aggregated_items_x_second": aggregated_items_x_second,
        "aggregated_items_x_day": aggregated_items_x_day,
        "aggregated_items_x_month": aggregated_items_x_month,
    }
