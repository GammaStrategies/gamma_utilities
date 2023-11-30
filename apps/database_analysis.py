import contextlib
import json
import re
import sys
import os
import logging


from datetime import datetime, timedelta

from datetime import timezone
from decimal import Decimal
from hexbytes import HexBytes

import tqdm
from apps.feeds.queue.queue_item import QueueItem
from bins.database.common.database_ids import create_id_report

from bins.general.enums import Chain, queueItemType, reportType, text_to_chain
from bins.general.exports import telegram_send_file
from bins.performance.benchmark_logs import (
    analyze_benchmark_info_log,
    analyze_benchmark_log,
)
from bins.w3.builders import build_erc20_helper

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


from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)
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


def analyze_queues(chains: list[Chain] | None = None):
    """Analize queues

    Args:
        chains (list[Chain], optional): list of chains to analyze. Defaults to None.
    """
    # get chains
    chains = chains or list(Chain)

    # get list of failing queue items
    for chain in chains:
        # load previous results
        folder_path = CONFIGURATION.get("cache", {}).get("save_path", "data/cache")
        previous_results = file_utilities.load_json(
            filename=f"{chain.database_name}_queue_analysis.json",
            folder_path=folder_path,
        )

        # create queue summary from database info
        queue_summary = {
            "timestamp": datetime.now(timezone.utc).timestamp(),
            "types": [],
        }
        query = [
            {
                "$group": {
                    "_id": {"type": "$type", "count": "$count"},
                    "qtty": {"$sum": 1},
                }
            },
            {"$project": {"type": "$_id.type", "count": "$_id.count", "qtty": "$qtty"}},
            {"$sort": {"type": 1, "count": 1, "qtty": 1}},
            {"$project": {"_id": 0}},
        ]
        for item in get_from_localdb(
            network=chain.database_name, collection="queue", aggregate=query
        ):
            queue_summary["types"].append(item)

        # construct result
        pintable_format = {}
        for item in queue_summary["types"]:
            # defne pintable key
            if item["count"] > 0:
                key_name = f"{item['type']}>0"
            else:
                key_name = f"{item['type']}"

            # set key value
            if not key_name in pintable_format:
                pintable_format[key_name] = item["qtty"]
            else:
                pintable_format[key_name] += item["qtty"]

        # log result
        logging.getLogger(__name__).info(f"{chain.fantasy_name} queue content:")
        for key, value in pintable_format.items():
            logging.getLogger(__name__).info(f"     {key:30s}: {value:4.1f}")

        # compare with previous results, if any
        if previous_results:
            seconds_passed = queue_summary["timestamp"] - previous_results["timestamp"]

            # create comparison printable version
            printable_comparison = {}
            for item in previous_results["types"]:
                # defne pintable key
                if item["count"] > 0:
                    key_name = f"{item['type']}>0"
                else:
                    key_name = f"{item['type']}"

                # set key value
                if not key_name in printable_comparison:
                    printable_comparison[key_name] = item["qtty"]
                else:
                    printable_comparison[key_name] += item["qtty"]

            # add keys not present in both printable reports
            for k, v in pintable_format.items():
                if k not in printable_comparison:
                    printable_comparison[k] = 0
            for k, v in printable_comparison.items():
                if k not in pintable_format:
                    pintable_format[k] = 0

            # log differences
            logging.getLogger(__name__).info(
                f"  compared with previous results {general_utilities.log_time_passed.get_timepassed_string(start_time=datetime.fromtimestamp(previous_results['timestamp']), end_time=datetime.fromtimestamp(queue_summary['timestamp']))} before:"
            )
            for key, value in pintable_format.items():
                if key in printable_comparison:
                    _calculated_value = value - printable_comparison[key]
                else:
                    _calculated_value = value

                logging.getLogger(__name__).info(
                    f"     {key:30s}: {_calculated_value:4.1f}  [{(_calculated_value/seconds_passed)*60*60*24:,.1f} items/day]"
                )

        logging.getLogger(__name__).debug(f" Saving results to file")
        # save result to file
        file_utilities.save_json(
            filename=f"{chain.database_name}_queue_analysis.json",
            folder_path=folder_path,
            data=queue_summary,
        )


def analyze_user_transactions(
    chains: list[Chain] | None = None,
    hypervisor_addresses: list[str] | None = None,
    user_addresses: list[str] | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    send_to_telegram: bool = False,
    save_to_db: bool = True,
) -> dict:
    if user_addresses:
        raise NotImplementedError("user_addresses not implemented yet")

    addresses = hypervisor_addresses or user_addresses or None

    if not ini_timestamp and CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime:
        ini_timestamp = CONFIGURATION["_custom_"][
            "cml_parameters"
        ].ini_datetime.timestamp()

    if not end_timestamp and CONFIGURATION["_custom_"]["cml_parameters"].end_datetime:
        end_timestamp = CONFIGURATION["_custom_"][
            "cml_parameters"
        ].end_datetime.timestamp()

    # control vars to define if we should set timestamps when no value is provided
    _set_ini_timestamp = True if not ini_timestamp else False
    _set_end_timestamp = True if not end_timestamp else False

    # set known proxy addresses
    proxy_addresses = {
        Chain.ARBITRUM: ["0x851b3Fb3c3178Cd3FBAa0CdaAe0175Efa15a30f1".lower()]
    }

    # set chains to process
    chains = (
        chains
        or [
            text_to_chain(x)
            for x in (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
            )
        ]
        or list(Chain)
    )

    # set result var
    result = {}

    # analyze
    for chain in chains:
        logging.getLogger(__name__).info(
            f" Analyzing user deposits in {chain.fantasy_name}"
        )
        # get static hypervisor data
        hypervisors_static_list = {
            x["address"]: x
            for x in get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"address": {"$in": addresses}} if addresses else {},
                batch_size=10000,
                projection={"_id": 0, "address": 1, "pool": 1, "symbol": 1, "dex": 1},
            )
        }

        token_current_prices = {
            x["address"]: x
            for x in get_default_globaldb().get_items_from_database(
                collection_name="current_usd_prices",
                find={"network": chain.database_name},
            )
        }

        # get all deposits
        find = {
            "$and": [
                {"topic": {"$in": ["deposit", "withdraw"]}},
            ],
        }
        if addresses:
            find["$and"].append({"address": {"$in": addresses}})
        if ini_timestamp:
            find["$and"].append({"timestamp": {"$gte": ini_timestamp}})
        if end_timestamp:
            find["$and"].append({"timestamp": {"$lte": end_timestamp}})

        # get operations from database
        database_operations = get_from_localdb(
            network=chain.database_name,
            collection="operations",
            find=find,
            batch_size=10000,
            projection={
                "_id": 0,
                "address": 1,
                "sender": 1,
                "to": 1,
                "blockNumber": 1,
                "transactionHash": 1,
                "timestamp": 1,
                "qtty_token0": 1,
                "qtty_token1": 1,
                "topic": 1,
            },
        )
        with tqdm.tqdm(total=len(database_operations)) as progress_bar:
            for transaction in database_operations:
                # for deposit in database_operations:
                # set easy to access vars
                _hypervisor_address = transaction["address"].lower()
                _tx_token0_qtty = int(transaction["qtty_token0"])
                _tx_token1_qtty = int(transaction["qtty_token1"])
                _token0_address = hypervisors_static_list[_hypervisor_address]["pool"][
                    "token0"
                ]["address"].lower()
                _token1_address = hypervisors_static_list[_hypervisor_address]["pool"][
                    "token1"
                ]["address"].lower()
                _token0_price = token_current_prices[_token0_address]["price"]
                _token1_price = token_current_prices[_token1_address]["price"]

                # find out if this transaction is identified as proxied
                if (
                    transaction["topic"] == "withdraw"
                    and transaction["to"] in proxy_addresses[chain]
                ):
                    raise ValueError("withdraws should not be proxied")
                elif (
                    transaction["topic"] == "deposit"
                    and transaction["sender"] in proxy_addresses[chain]
                ):
                    # PROXIED TRANSACTION
                    progress_bar.set_description(
                        f" proxied  {transaction['topic']} {hypervisors_static_list[_hypervisor_address]['symbol']}"
                    )
                    progress_bar.refresh()
                    analyze_user_transaction_process_proxied(
                        chain=chain,
                        transaction=transaction,
                        _uniproxy_address=transaction["sender"],
                        _hypervisor_address=_hypervisor_address,
                        _tx_token0_qtty=_tx_token0_qtty,
                        _tx_token1_qtty=_tx_token1_qtty,
                        _token0_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token0"]["decimals"],
                        _token1_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token1"]["decimals"],
                        _token0_address=_token0_address,
                        _token1_address=_token1_address,
                        _token0_price=_token0_price,
                        _token1_price=_token1_price,
                        result=result,
                    )
                else:
                    # NOT PROXIED
                    # (withdraws are not proxied)
                    progress_bar.set_description(
                        f" {transaction['topic']} {hypervisors_static_list[_hypervisor_address]['symbol']}"
                    )
                    progress_bar.refresh()
                    analyze_user_transaction_process(
                        chain=chain,
                        transaction=transaction,
                        _hypervisor_address=_hypervisor_address,
                        _tx_token0_qtty=_tx_token0_qtty,
                        _tx_token1_qtty=_tx_token1_qtty,
                        _token0_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token0"]["decimals"],
                        _token1_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token1"]["decimals"],
                        _token0_address=_token0_address,
                        _token1_address=_token1_address,
                        _token0_price=_token0_price,
                        _token1_price=_token1_price,
                        result=result,
                    )

                # set timestamps
                if _set_ini_timestamp:
                    ini_timestamp = (
                        transaction["timestamp"]
                        if not ini_timestamp
                        else min(ini_timestamp, transaction["timestamp"])
                    )
                if _set_end_timestamp:
                    end_timestamp = (
                        transaction["timestamp"]
                        if not end_timestamp
                        else max(end_timestamp, transaction["timestamp"])
                    )

                progress_bar.update(1)

        if send_to_telegram:
            # build caption message
            caption = (
                f"users net position for {' '.join([x for x in chains])} {len(hypervisor_addresses) if hypervisor_addresses else 'all'} hypes from {datetime.fromtimestamp(ini_timestamp,timezone.utc)} to {datetime.fromtimestamp(end_timestamp,timezone.utc)}",
            )

            # send to telegram
            telegram_send_file(
                input_file_content=json.dumps(result).encode("utf-8"),
                full_filename=f"users_net_position.json",
                mime_type="application/json",
                telegram_file_type="Document",
                caption=caption,
            )

        if save_to_db:
            # save to reports collection
            result["id"] = create_id_report(
                chain=chain,
                report_type=reportType.USERS_ACTIVITY,
                customId="users_net_position_Galxe",
            )
            db_return = get_default_localdb(
                network=chain.database_name
            ).replace_item_to_database(collection_name="reports", data=result)
            logging.getLogger(__name__).debug(
                f" Save result of users net position report to database: {db_return.raw_result}"
            )

    return result


def analyze_user_transaction_process_proxied(
    chain: Chain,
    transaction: dict,
    _uniproxy_address: str,
    _hypervisor_address: str,
    _tx_token0_qtty: int,
    _tx_token1_qtty: int,
    _token0_decimals: int,
    _token1_decimals: int,
    _token0_price: float,
    _token1_price: float,
    _token0_address: str,
    _token1_address: str,
    result: dict,
):
    """add user address

    Args:
        chain (Chain):
        transaction (dict):
        _uniproxy_address (str):
        _hypervisor_address (str):
        _tx_token0_qtty (int):
        _tx_token1_qtty (int):
        _token0_price (float):
        _token1_price (float):
        _token0_address (str):
        _token1_address (str):
        result (dict):
    """

    if transaction["topic"] == "withdraw":
        raise ValueError("withdraws should not be proxied")

    ercHelper = build_erc20_helper(chain=chain)
    receipt = ercHelper._getTransactionReceipt(transaction["transactionHash"])
    decoded_logs = ercHelper.contract.events.Transfer().processReceipt(receipt)

    found_addresses = []
    # find hypervisor address deposit (to)
    for log in decoded_logs:
        _contract_address = log.address.lower()
        _current_qtty = int(log.args["value"])
        _current_from = log.args["from"].lower()
        _current_to = log.args["to"].lower()
        # make sure this is not a transfer to the hypervisor nor a transfer from 0x0000
        if (
            _contract_address in [_token0_address, _token1_address]
            and _current_to == _uniproxy_address
            and _contract_address != _hypervisor_address
        ):
            if _tx_token0_qtty == _current_qtty:
                # this is the one
                found_addresses.append(_current_from)
            elif _tx_token1_qtty == _current_qtty:
                # this is the one
                found_addresses.append(_current_from)
            else:
                # this is not the one
                pass

    # check if found and if they are equal to each other
    if found_addresses:
        if len(found_addresses) == 2:
            # set easy to access vars
            _user_address = found_addresses[0]
            _tx_token0_qtty /= 10**_token0_decimals
            _tx_token1_qtty /= 10**_token1_decimals
            _deposit_usd_value = (
                _tx_token0_qtty * _token0_price + _tx_token1_qtty * _token1_price
            )

            # check if they are equal
            if found_addresses[0] != found_addresses[1]:
                # not equal! check if one of them is minted
                if "0x0000000000000000000000000000000000000000" in found_addresses:
                    # this may be a wrapped eth transaction, or another token wraped by the user directly to proxy contract.
                    # because it matches qtty, we assume it is correct and use the other address as user address.
                    logging.getLogger(__name__).debug(
                        f" Found wrapped eth {transaction['topic']}!!  "
                    )
                    # select the other address
                    _user_address = [
                        x.lower()
                        for x in found_addresses
                        if x != "0x0000000000000000000000000000000000000000"
                    ][0]
                else:
                    logging.getLogger(__name__).debug(
                        f" Found TWO different addresses!!  {transaction['address']} {transaction['blockNumber']} -> {found_addresses}"
                    )
                    return

            # create user address in result, if needed
            if _user_address not in result:
                result[_user_address] = {
                    "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
                    "hypervisors": {},
                }

            # add usd value and qtty to user address
            result[_user_address]["total_net_position"]["usd"] += _deposit_usd_value
            result[_user_address]["total_net_position"]["token0"] += _tx_token0_qtty
            result[_user_address]["total_net_position"]["token1"] += _tx_token1_qtty

            # add hypervisor
            if _hypervisor_address not in result[_user_address]["hypervisors"]:
                result[_user_address]["hypervisors"][_hypervisor_address] = {
                    "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
                    "transactions": [],
                }

            # add hype usd value
            result[_user_address]["hypervisors"][_hypervisor_address][
                "total_net_position"
            ]["usd"] += _deposit_usd_value
            # add qtty to user hype address
            result[_user_address]["hypervisors"][_hypervisor_address][
                "total_net_position"
            ]["token0"] += _tx_token0_qtty
            result[_user_address]["hypervisors"][_hypervisor_address][
                "total_net_position"
            ]["token1"] += _tx_token1_qtty
            # add transaction
            result[_user_address]["hypervisors"][_hypervisor_address][
                "transactions"
            ].append(
                {
                    "block": transaction["blockNumber"],
                    "txHash": transaction["transactionHash"],
                    "timestamp": transaction["timestamp"],
                    "token0": _tx_token0_qtty,
                    "token1": _tx_token1_qtty,
                    "usd": _deposit_usd_value,
                    "user_address": _user_address,
                    "from": _current_from,
                    "to": _current_to,
                    "topic": transaction["topic"],
                }
            )
        else:
            logging.getLogger(__name__).debug(
                f" ERROR !!  {transaction['address']} {transaction['blockNumber']} -> {found_addresses}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" Not found {transaction['address']} {transaction['blockNumber']} -> {found_addresses}"
        )


def analyze_user_transaction_process(
    chain: Chain,
    transaction: dict,
    _hypervisor_address: str,
    _tx_token0_qtty: int,
    _tx_token1_qtty: int,
    _token0_decimals: int,
    _token1_decimals: int,
    _token0_price: float,
    _token1_price: float,
    _token0_address: str,
    _token1_address: str,
    result: dict,
):
    # set easy to access vars
    _user_address = transaction["sender"].lower()
    _tx_token0_qtty /= 10**_token0_decimals
    _tx_token1_qtty /= 10**_token1_decimals
    _tx_usd_value = (_tx_token0_qtty * _token0_price) + (
        _tx_token1_qtty * _token1_price
    )

    # make sure first user transaction is not a withdraw ( to avoid a negative starting point)
    if transaction["topic"] == "withdraw" and (
        _user_address not in result
        or _hypervisor_address not in result[_user_address]["hypervisors"]
    ):
        logging.getLogger(__name__).debug(
            f" First transaction is a withdraw, skipping {transaction['address']} {transaction['blockNumber']} -> {transaction['topic']}"
        )
        return

    # create user address in result, if needed
    if _user_address not in result:
        result[_user_address] = {
            "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
            "hypervisors": {},
        }
    if _hypervisor_address not in result[_user_address]["hypervisors"]:
        result[_user_address]["hypervisors"][_hypervisor_address] = {
            "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
            "transactions": [],
        }

    # define if this is a deposit or withdraw
    _sign = 1 if transaction["topic"] == "deposit" else -1

    # add to result
    # add usd value
    result[_user_address]["total_net_position"]["usd"] += _tx_usd_value * _sign
    # add qtty to user address
    result[_user_address]["total_net_position"]["token0"] += _tx_token0_qtty * _sign
    result[_user_address]["total_net_position"]["token1"] += _tx_token1_qtty * _sign
    # add qtty to hypervisor address
    result[_user_address]["hypervisors"][_hypervisor_address]["total_net_position"][
        "usd"
    ] += (_tx_usd_value * _sign)
    result[_user_address]["hypervisors"][_hypervisor_address]["total_net_position"][
        "token0"
    ] += (_tx_token0_qtty * _sign)
    result[_user_address]["hypervisors"][_hypervisor_address]["total_net_position"][
        "token1"
    ] += (_tx_token1_qtty * _sign)
    # add transaction
    result[_user_address]["hypervisors"][_hypervisor_address]["transactions"].append(
        {
            "block": transaction["blockNumber"],
            "txHash": transaction["transactionHash"],
            "timestamp": transaction["timestamp"],
            "token0": _tx_token0_qtty,
            "token1": _tx_token1_qtty,
            "usd": _tx_usd_value,
            "user_address": _user_address,
            "from": transaction["sender"].lower(),
            "to": transaction["to"].lower(),
            "topic": transaction["topic"],
        }
    )


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

    if option == "user":
        # check if user address to analyze
        if CONFIGURATION["_custom_"]["cml_parameters"].user_address:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    sumary_user(
                        network=network,
                        protocol="gamma",
                        user_address=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].user_address.lower(),
                        ini_date=ini_datetime,
                        end_date=end_datetime,
                    )
        else:
            raise ValueError("user address not provided. Use --user_address <address>")
    elif option == "network":
        for protocol in CONFIGURATION["script"]["protocols"]:
            # override networks if specified in cml
            networks = (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"][protocol]["networks"]
            )
            for network in networks:
                # execute summary
                sumary_network(
                    network=network,
                    protocol=protocol,
                    ini_date=ini_datetime,
                    end_date=end_datetime,
                )
    elif option == "benchmark_logs":
        benchmark_logs_analysis()

    elif option == "queue":
        for protocol in CONFIGURATION["script"]["protocols"]:
            # override networks if specified in cml
            networks = (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"][protocol]["networks"]
            )

            analyze_queues(chains=[text_to_chain(network) for network in networks])

    elif option == "user_deposits":
        # hypervisor addresses must be provided

        print(
            analyze_user_transactions(
                hypervisor_addresses=CONFIGURATION["_custom_"][
                    "cml_parameters"
                ].hypervisor_addresses,
                user_addresses=CONFIGURATION["_custom_"]["cml_parameters"].user_address,
                send_to_telegram=CONFIGURATION["_custom_"][
                    "cml_parameters"
                ].send_to_telegram,
            )
        )
        # elif CONFIGURATION["_custom_"]["cml_parameters"].user_address:
        #     # TODO: replace with user addresses
        #     print(
        #         analyze_user_deposits(

        #             send_to_telegram=CONFIGURATION["_custom_"][
        #                 "cml_parameters"
        #             ].send_to_telegram,
        #         )
        #     )

        # else:
        #     analyze_user_deposits(
        #             send_to_telegram=CONFIGURATION["_custom_"][
        #                 "cml_parameters"
        #             ].send_to_telegram,
        #         )
        #     raise ValueError(
        #         "no addresses provided to analyze. Use --hypervisor_addresses '<address> <address> ...'   or --user_addresses '<address> <address> ...'"
        #     )
