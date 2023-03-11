import sys
import os
import logging
import tqdm
import concurrent.futures
import uuid

from web3 import Web3
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict, InitVar
from decimal import Decimal, getcontext

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
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
)


def print_status(status: user_status, symbol: str = "", network: str = ""):
    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info(
        f" Status of {status.address} at {datetime.fromtimestamp(status.timestamp)} block:{status.block}"
    )
    logging.getLogger(__name__).info(f" {symbol}   {network}")
    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info("\tAbsolute situation:  ( all in USD )")
    logging.getLogger(__name__).info(
        "\tMarket value (tvl):\t {:,.2f}\t ".format(
            status.total_underlying_in_usd if status.total_underlying_in_usd else 0,
        )
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
    csv_filename = "{}_{}_{}_from_{}_{}.csv".format(
        network,
        symbol,
        status_list[-1]["address"],
        status_list[0]["block"],
        status_list[-1]["block"],
    )
    csv_filename = os.path.join(folder, csv_filename)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass

    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=status_list)


def get_hypervisor_addresses(
    network: str, protocol: str, user_address: str = None
) -> list[str]:

    result = list()
    # get database configuration
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    # get blacklisted hypervisors
    blacklisted = (
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
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

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
    # filter blcacklisted
    result = [x for x in result if not x in blacklisted]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after applying filters: {len(result)}"
    )

    return result


def test_new(network: str):
    protocol = "gamma"

    b_ini = 0
    b_end = 0
    hypervisor_addresses = get_hypervisor_addresses(network=network, protocol=protocol)

    for address in hypervisor_addresses:
        logging.getLogger(__name__).info(
            f" --->  Starting analysis for {network}'s {address}"
        )

        hype_new = user_status_hypervisor_builder(
            hypervisor_address=address, network=network, protocol=protocol
        )
        hype_status_list = list()
        try:

            hype_new._process_operations()

            hype_status_list = hype_new.result_list()

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
        except:
            logging.getLogger(__name__).exception(" yeeep ")

        try:
            fees_direct = get_fees(
                network=network,
                hype_address=address,
                max_block=hype_status_list[-1].block,
            )
            fees_collected_usd = (
                hype_status_list[-1].usd_price_token0 * fees_direct["qtty_token0"]
                + hype_status_list[-1].usd_price_token1 * fees_direct["qtty_token1"]
            )

            comparable_info = hype_status_list[-1]._get_comparable()

            if fees_direct["qtty_token0"] - comparable_info[
                "fees_collected_token0"
            ] != Decimal("0"):
                logging.getLogger(__name__).error(
                    " Total collected fees 0 do not match with database data -> {} vs {}".format(
                        comparable_info["fees_collected_token0"],
                        fees_direct["qtty_token0"],
                    )
                )
            if fees_direct["qtty_token1"] - comparable_info[
                "fees_collected_token1"
            ] != Decimal("0"):
                logging.getLogger(__name__).error(
                    " Total collected fees 1 do not match with database data -> {} vs {}".format(
                        comparable_info["fees_collected_token1"],
                        fees_direct["qtty_token1"],
                    )
                )
        except:
            pass

        # OLD TEMA

        # hype_old = hypervisor_db_reader(
        #     hypervisor_address=address, network=network, protocol=protocol
        # )
        # try:
        #     hype_old._process_operations()
        #     hype_status_old_atBlock = hype_old.result(block=hype_status_list[-1].block)
        #     print_status(
        #         hype_status_old_atBlock,
        #         symbol=hype_old.symbol,
        #         network=hype_old.network,
        #     )

        # except:
        #     logging.getLogger(__name__).exception(" yeeep OLD")


def sumary_network(
    network: str, protocol: str, ini_date: datetime = None, end_date: datetime = None
):

    # set timeframe
    if end_date is None:
        end_date = datetime.utcnow()
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

            summary = hype_new.sumary_result(t_ini=ini_timestamp, t_end=end_timestamp)

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
            logging.getLogger(__name__).info(
                " From a total of {} users:".format(
                    total_users,
                )
            )
            logging.getLogger(__name__).info(
                " \t  {} [{:,.2%}] have positive results and {} [{:,.2%}] negative".format(
                    positive_result,
                    positive_result / total_users,
                    negative_result,
                    negative_result / total_users,
                )
            )

        except:
            logging.getLogger(__name__).error(
                " can't analyze {}  (  may not have value locked ) --> err: {}".format(
                    address, sys.exc_info()[0]
                )
            )


def sumary_user(
    network: str,
    protocol: str,
    user_address: str,
    ini_date: datetime = None,
    end_date: datetime = None,
):

    hypervisor_addresses = get_hypervisor_addresses(
        network=network, protocol=protocol, user_address=user_address.lower()
    )

    # set timeframe
    if end_date is None:
        end_date = datetime.utcnow()
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
        except:
            logging.getLogger(__name__).exception(" error ")


def main(option: str, **kwargs):

    # get dates range from command line
    try:
        ini_datetime = CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime
    except:
        ini_datetime = None
    try:
        end_datetime = CONFIGURATION["_custom_"]["cml_parameters"].end_datetime
    except:
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

    elif CONFIGURATION["_custom_"]["cml_parameters"].hypervisor_address:
        pass

    else:
        # execute summary
        sumary_network(
            network=option,
            protocol="gamma",
            ini_date=ini_datetime,
            end_date=end_datetime,
        )
