import contextlib
import re
import sys
import os
import logging


from datetime import datetime, timedelta

from datetime import timezone
from decimal import Decimal
from apps.feeds.queue.queue_item import QueueItem

from bins.general.enums import Chain, queueItemType

if __name__ == "__main__":
    # append parent directory pth
    CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
    PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
    sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION

from bins.database.common.db_collections_common import database_local, database_global
from bins.database.db_user_status import (
    user_status,
    user_status_hypervisor_builder,
)
from bins.general import general_utilities, file_utilities
from bins.apis.thegraph_utilities import gamma_scraper


from bins.database.helpers import get_default_localdb, get_from_localdb
from .database_checker import get_all_logfiles, load_logFile


def print_status(status: user_status, symbol: str = "", network: str = ""):
    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info(
        f" Status of {status.address} at {datetime.fromtimestamp(status.timestamp)} block:{status.block}"
    )
    logging.getLogger(__name__).info(f" {symbol}   {network}")
    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info("\tAbsolute situation:  ( all in USD )")
    logging.getLogger(__name__).info(
        "\tMarket value (tvl):\t {:,.2f}\t ".format(status.total_underlying_in_usd or 0)
    )

    logging.getLogger(__name__).info(
        "\t   HODL token0:    \t {:,.2f}\t [{:+,.2%} vs market value]".format(
            status.total_investment_qtty_in_token0 * status.usd_price_token0,
            (
                (status.total_investment_qtty_in_token0 * status.usd_price_token0)
                - status.total_underlying_in_usd
            )
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   HODL token1:    \t {:,.2f}\t [{:+,.2%} vs market value]".format(
            status.total_investment_qtty_in_token1 * status.usd_price_token1,
            (
                (status.total_investment_qtty_in_token1 * status.usd_price_token1)
                - status.total_underlying_in_usd
            )
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tFees generated:     \t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_collected_in_usd
            + status.total_fees_uncollected_in_usd
            + status.total_fees_owed_in_usd,
            (
                status.total_fees_collected_in_usd
                + status.total_fees_uncollected_in_usd
                + status.total_fees_owed_in_usd
            )
            / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   Fees collected:  \t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_collected_in_usd,
            status.total_fees_collected_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   Fees owed:       \t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_owed_in_usd,
            status.total_fees_owed_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   Fees uncollected:\t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_uncollected_in_usd,
            status.total_fees_uncollected_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tInvestment:        \t {:,.2f}".format(status.total_investment_qtty_in_usd)
    )
    logging.getLogger(__name__).info(
        "\t   total in token0:\t {:,.2f}   [at usdprice: {:,.2f}]".format(
            status.total_investment_qtty_in_token0,
            (
                status.total_investment_qtty_in_usd
                / status.total_investment_qtty_in_token0
            )
            if status.total_investment_qtty_in_token0 > 0
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   total in token1:\t {:,.2f}   [at usdprice: {:,.2f}]".format(
            status.total_investment_qtty_in_token1,
            (
                status.total_investment_qtty_in_usd
                / status.total_investment_qtty_in_token1
            )
            if status.total_investment_qtty_in_token1 > 0
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tNet market gains:\t {:,.2f}\t [{:+,.2%} vs investment]".format(
            status.total_current_result_in_usd,
            status.total_current_result_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tShares:\t {:,.2f}\t [{:,.2%} over total]".format(
            status.shares_qtty, status.shares_percent
        )
    )
    logging.getLogger(__name__).info("\tImpermanent loss:")
    logging.getLogger(__name__).info(
        "\t   LP vs HOLDING USD:   \t {:,.2f}\t [{:,.2%}]".format(
            status.impermanent_lp_vs_hodl_usd,
            status.impermanent_lp_vs_hodl_usd / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   LP vs HOLDING token0:\t {:,.2f}\t [{:,.2%}]".format(
            status.impermanent_lp_vs_hodl_token0,
            status.impermanent_lp_vs_hodl_token0 / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   LP vs HOLDING token1:\t {:,.2f}\t [{:,.2%}]".format(
            status.impermanent_lp_vs_hodl_token1,
            status.impermanent_lp_vs_hodl_token1 / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info("\tRelative situation:  ( all in USD )")

    # collected + owed + divested / total seconds  +  uncollected / uncollected seconds
    second_fees_collected = (
        (
            (
                status.total_fees_collected_in_usd
                + status.total_fees_owed_in_usd
                + status.total_divestment_fee_qtty_in_usd
            )
            / status.secPassed
        )
        if status.secPassed
        else 0
    )
    second_fees_uncollected = (
        (status.total_fees_uncollected_in_usd / status.fees_uncollected_secPassed)
        if status.fees_uncollected_secPassed
        else 0
    )
    anual_fees = (second_fees_collected + second_fees_uncollected) * (
        60 * 60 * 24 * 365
    )

    anual_roi = (
        (status.total_current_result_in_usd / status.secPassed)
        if status.secPassed
        else 0
    ) * (60 * 60 * 24 * 365)

    yearly_fee_yield = (
        (
            (
                (
                    (
                        (
                            status.total_fees_collected_in_usd
                            + status.total_fees_owed_in_usd
                        )
                        / status.secPassed
                    )
                    if status.secPassed
                    else 0
                )
                + (
                    status.total_fees_uncollected_in_usd
                    / status.fees_uncollected_secPassed
                )
                if status.fees_uncollected_secPassed
                else 0
            )
            * (60 * 60 * 24 * 365)
        )
        / status.total_underlying_in_usd
        if status.total_underlying_in_usd
        else 0
    )

    logging.getLogger(__name__).info(
        "\tAnualized fees:\t {:,.2%} vs market value".format(
            anual_fees / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0
        )
    )
    logging.getLogger(__name__).info(
        "\tAnualized return on investment:\t {:,.2%}".format(
            anual_roi / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0
        )
    )
    logging.getLogger(__name__).info(
        "\tAnualized fee yield:\t {:,.2%}".format(yearly_fee_yield)
    )

    logging.getLogger(__name__).info("")


def user_status_to_csv(status_list: list[dict], folder: str, network: str, symbol: str):
    """save data to csv file

    Args:
        status_list (list[dict]): list of user status converted to dict
        folder (str): where to save
        network (str):
        symbol (str): hypervisor symbol
    """
    # result = list()
    # for r in status_list:
    #     result.append(convert_to_dict(status=r))

    csv_columns = [
        "address",
        "block",
        "timestamp",
        "usd_price_token0",
        "usd_price_token1",
        "shares_qtty",
        "shares_percent",
        "secPassed",
        "investment_qtty_token0",
        "investment_qtty_token1",
        "total_investment_qtty_in_usd",
        "total_investment_qtty_in_token0",
        "total_investment_qtty_in_token1",
        "tvl_token0",
        "tvl_token1",
        "total_tvl_in_usd",
        "underlying_token0",
        "underlying_token1",
        "total_underlying_in_usd",
        "fees_collected_token0",
        "fees_collected_token1",
        "total_fees_collected_in_usd",
        "fees_owed_token0",
        "fees_owed_token1",
        "total_fees_owed_in_usd",
        "fees_uncollected_token0",
        "fees_uncollected_token1",
        "total_fees_uncollected_in_usd",
        "fees_uncollected_secPassed",
        "current_result_token0",
        "current_result_token1",
        "total_current_result_in_usd",
        "impermanent_lp_vs_hodl_usd",
        "impermanent_lp_vs_hodl_token0",
        "impermanent_lp_vs_hodl_token1",
    ]
    csv_columns.extend(
        [x for x in list(status_list[-1].keys()) if x not in csv_columns]
    )
    # topic
    # closed_investment_return_token0	closed_investment_return_token1	current_result_token0		divestment_base_qtty_token0	divestment_base_qtty_token1	divestment_fee_qtty_token0	divestment_fee_qtty_token1							total_closed_investment_return_in_token0	total_closed_investment_return_in_token1	total_closed_investment_return_in_usd	total_current_result_in_token0	total_current_result_in_token1		total_divestment_base_qtty_in_token0	total_divestment_base_qtty_in_token1	total_divestment_base_qtty_in_usd	otal_divestment_fee_qtty_in_usd		impermanent_lp_vs_hodl_usd		total_underlying_in_token0	total_underlying_in_token1

    # set filename
    csv_filename = f'{network}_{symbol}_{status_list[-1]["address"]}_from_{status_list[0]["block"]}_{status_list[-1]["block"]}.csv'

    csv_filename = os.path.join(folder, csv_filename)

    # remove file
    with contextlib.suppress(Exception):
        os.remove(csv_filename)
    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=status_list)


def get_hypervisor_addresses(
    network: str, protocol: str, user_address: str | None = None
) -> list[str]:
    result: list[str] = []
    # get database configuration
    mongo_url: str = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name: str = f"{network}_{protocol}"

    # get blacklisted hypervisors
    blacklisted: list[str] = (
        CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get(protocol, {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(network, [])
    )

    # check n clean
    if blacklisted is None:
        blacklisted = []

    # retrieve all addresses from database
    local_db_manager: database_local = get_default_localdb(network=network)

    if user_address:
        result = local_db_manager.get_distinct_items_from_database(
            collection_name="user_status",
            field="hypervisor_address",
            condition={"address": user_address},
        )
    else:
        result = local_db_manager.get_distinct_items_from_database(
            collection_name="static", field="address"
        )

    # apply black list
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process before applying filters: {len(result)}"
    )
    # filter blcaklisted
    result = [x for x in result if x not in blacklisted]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after applying filters: {len(result)}"
    )

    # filter nulls
    result = [x for x in result if x is not None]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after removing None: {len(result)}"
    )

    # filter empty strings
    result = [x for x in result if x != ""]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after removing empty strings: {len(result)}"
    )

    return result


def sumary_network(
    network: str,
    protocol: str,
    ini_date: datetime | None = None,
    end_date: datetime | None = None,
):
    # set timeframe
    if end_date is None:
        end_date = datetime.now(timezone.utc)
    if ini_date is None or ini_date >= end_date:
        ini_date = end_date - timedelta(days=7)

    # convert dates to timestamps
    ini_timestamp = ini_date.timestamp()
    end_timestamp = end_date.timestamp()

    # get all hypervisors
    hypervisor_addresses = get_hypervisor_addresses(network=network, protocol=protocol)

    for address in hypervisor_addresses:
        try:
            # create helper
            hype_new = user_status_hypervisor_builder(
                hypervisor_address=address, network=network, protocol=protocol
            )

            summary = hype_new.sumary_result(
                t_ini=int(ini_timestamp), t_end=int(end_timestamp)
            )

            logging.getLogger(__name__).info(" ")
            logging.getLogger(__name__).info(
                "{}  period from {:%Y-%m-%d %H:%M:%S} to {:%Y-%m-%d %H:%M:%S}  [ {:,.1f} days]".format(
                    hype_new.symbol,
                    datetime.fromtimestamp(summary["date_from"]),
                    datetime.fromtimestamp(summary["date_to"]),
                    (summary["date_to"] - summary["date_from"]) / (60 * 60 * 24),
                )
            )
            logging.getLogger(__name__).info(
                "\t return: {:,.2%}  ->  feeAPY: {:,.2%}   feeAPR: {:,.2%}".format(
                    summary["current_return_percent"],
                    summary["feeAPY"],
                    summary["feeAPR"],
                )
            )
            # get all last user status
            positive_result = 0
            negative_result = 0
            break_result = 0

            for user_s in hype_new.last_user_status_list():
                # check profits
                if user_s.total_current_result_in_usd > Decimal("1"):
                    positive_result += 1
                elif user_s.total_current_result_in_usd < Decimal("0"):
                    negative_result += 1
                else:
                    break_result += 1

            total_users = positive_result + negative_result + break_result
            logging.getLogger(__name__).info(f" From a total of {total_users} users:")
            logging.getLogger(__name__).info(
                " \t  {} [{:,.2%}] have positive results and {} [{:,.2%}] negative".format(
                    positive_result,
                    positive_result / total_users,
                    negative_result,
                    negative_result / total_users,
                )
            )

        except Exception:
            logging.getLogger(__name__).error(
                f" can't analyze {address}  (  may not have value locked ) --> err: {sys.exc_info()[0]}"
            )


def sumary_user(network, protocol, user_address, ini_date=None, end_date=None):
    hypervisor_addresses = get_hypervisor_addresses(
        network, protocol, user_address.lower()
    )

    # set timeframe
    if end_date is None:
        end_date = datetime.now(timezone.utc)
    if ini_date is None or ini_date >= end_date:
        ini_date = end_date - timedelta(days=7)

    # convert dates to timestamps
    ini_timestamp = ini_date.timestamp()
    end_timestamp = end_date.timestamp()

    for address in hypervisor_addresses:
        logging.getLogger(__name__).info(
            f" --->  Starting analysis for {network}'s {address} (user address: {user_address})"
        )

        hype_new = user_status_hypervisor_builder(
            hypervisor_address=address, network=network, protocol=protocol
        )
        try:
            hype_status_list = hype_new.account_result_list(address=user_address)

            user_status_to_csv(
                status_list=[
                    hype_new.convert_user_status_to_dict(r) for r in hype_status_list
                ],
                folder="tests",
                network=network,
                symbol=hype_new.symbol,
            )

            print_status(
                hype_status_list[-1], symbol=hype_new.symbol, network=hype_new.network
            )
        except Exception:
            logging.getLogger(__name__).exception(" error ")


# queue analysis
def queue_analysis():
    # get number of items in queues, per network

    # read benchmark logs to sumarize queue statistics per network ( average processing time, average waiting time, etc )
    for log_file in get_all_logfiles(log_names=["benchmark"]):
        pass


# benchmark analysis
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
    # process logs
    for log_file in log_files:
        # analize log file
        logging.getLogger(__name__).debug(f" analyzing {log_file}")
        if result := analize_benchmark_log(log_file=load_logFile(log_file)):
            # check if there is data in result
            if not result["total_items"] > 0:
                logging.getLogger(__name__).info(
                    f"    - no data in {log_file}  [skipping]"
                )
                continue

            aggregated_data.append(result)

            items_x_second = (
                result["total_items"]
                / (
                    result["timeframe"]["end"] - result["timeframe"]["ini"]
                ).total_seconds()
            )
            items_x_day = items_x_second * 60 * 60 * 24
            items_x_month = items_x_day * 30

            logging.getLogger(__name__).info(
                f"    - calculated {items_x_day:,.0f} [ processed {result['total_items']} from {result['timeframe']['ini']} to {result['timeframe']['end']}] "
            )

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

    # log aggregated data
    logging.getLogger(__name__).info(
        f"Aggregated data from {timeframe['ini']} to {timeframe['end']}"
    )
    logging.getLogger(__name__).info(
        f"    - processed {len(aggregated_data)} log files"
    )
    logging.getLogger(__name__).info(
        f"    - processed {sum([x['total_items'] for x in aggregated_data]):,.0f} items"
    )
    aggregated_items_x_second = (
        sum([x["total_items"] for x in aggregated_data])
        / (timeframe["end"] - timeframe["ini"]).total_seconds()
    )
    aggregated_items_x_day = aggregated_items_x_second * 60 * 60 * 24
    aggregated_items_x_month = aggregated_items_x_day * 30
    logging.getLogger(__name__).info(
        f"    - calculated {aggregated_items_x_day:,.0f} items per day"
    )
    logging.getLogger(__name__).info(
        f"    - calculated {aggregated_items_x_month:,.0f} items per month"
    )


def get_list_failing_queue_items(chain: Chain, find: dict | None = None):
    """Get a detailed list of failing queue items for the specified network"""

    result = {}

    for queue_item_db in get_from_localdb(
        network=chain.database_name,
        collection="queue",
        find=find or {"count": {"$gt": 8}},
    ):
        # transform
        queue_item = QueueItem(**queue_item_db)

        # prepare result
        if queue_item.type not in result:
            result[queue_item.type] = []

        # check type
        if queue_item.type == queueItemType.LATEST_MULTIFEEDISTRIBUTION:
            rewards_static = get_from_localdb(
                network=chain.database_name,
                collection="rewards_static",
                find={"rewarder_registry": queue_item.address},
            )
            if not rewards_static:
                raise Exception(
                    f"Can't find rewards_static using rewarder_registry {queue_item.address}"
                )

            hypervisor_static = get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"address": rewards_static[0]["hypervisor_address"]},
            )[0]

            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": hypervisor_static["symbol"],
                    "dex": hypervisor_static["dex"],
                    "hypervisor_address": hypervisor_static["address"],
                    "rewards_static": [
                        {"token": x["rewardToken_symbol"], "address": x["rewardToken"]}
                        for x in rewards_static
                    ]
                    if rewards_static
                    else [],
                    "block": queue_item.block,
                }
            )

        elif queue_item.type == queueItemType.PRICE:
            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": "",
                    "hypervisor_address": "",
                    "rewards_static": [],
                    "block": queue_item.block,
                }
            )
        elif queue_item.type == queueItemType.REWARD_STATUS:
            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": queue_item.data["hypervisor_status"]["symbol"],
                    "hypervisor_address": queue_item.data["hypervisor_status"][
                        "address"
                    ],
                    "dex": queue_item.data["hypervisor_status"]["dex"],
                    "rewards_static": [
                        {
                            "token": queue_item.data.get("reward_static", {}).get(
                                "rewardToken_symbol", ""
                            ),
                            "address": queue_item.data.get("reward_static", {}).get(
                                "rewardToken", ""
                            ),
                        }
                    ],
                    "block": queue_item.block,
                }
            )
        elif queue_item.type == queueItemType.HYPERVISOR_STATUS:
            hypervisor_static = get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"address": queue_item.address},
            )[0]
            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": hypervisor_static["symbol"],
                    "hypervisor_address": queue_item.address,
                    "dex": hypervisor_static["dex"],
                    "rewards_static": [],
                    "block": queue_item.block,
                }
            )

    return result


def main(option: str, **kwargs):
    # get dates range from command line
    try:
        ini_datetime = CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime
    except Exception:
        ini_datetime = None
    try:
        end_datetime = CONFIGURATION["_custom_"]["cml_parameters"].end_datetime
    except Exception:
        end_datetime = None

    # check if user address to analyze
    if CONFIGURATION["_custom_"]["cml_parameters"].user_address:
        sumary_user(
            network=option,
            protocol="gamma",
            user_address=CONFIGURATION["_custom_"][
                "cml_parameters"
            ].user_address.lower(),
            ini_date=ini_datetime,
            end_date=end_datetime,
        )
    # elif not CONFIGURATION["_custom_"]["cml_parameters"].hypervisor_address:
    #     # TODO:
    else:
        # execute summary
        sumary_network(
            network=option,
            protocol="gamma",
            ini_date=ini_datetime,
            end_date=end_datetime,
        )
