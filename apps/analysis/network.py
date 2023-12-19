from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import sys

from bins.configuration import CONFIGURATION
from bins.database.db_user_status import user_status_hypervisor_builder
from bins.database.helpers import get_default_localdb


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
    local_db_manager = get_default_localdb(network=network)

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
