import json
import logging
from apps.analysis.revenue import build_global_revenue_report


from bins.database.common.database_ids import create_id_report
from bins.database.helpers import get_default_globaldb
from bins.general.general_utilities import convert_keys_to_str


def feed_global_reports():
    """Execute all reports for feeds"""

    feed_global_revenue_report()


def feed_global_revenue_report():
    """Execute revenue report for feeds"""
    logging.getLogger(__name__).info(f" Feeding database with the revenue report ")

    # build report
    revenue = build_global_revenue_report()

    # convert keys to strings to avoid errors
    revenue = convert_keys_to_str(revenue)

    # add id to report
    revenue["id"] = "revenue"

    # create database helper
    # db_helper = get_default_globaldb()
    # increase connectTimeoutMS to 2 minutes
    # db_helper.db_manager.mongo_client._timeout = 120000

    # save it to database
    if db_update := get_default_globaldb().replace_item_to_database(
        data=revenue, collection_name="reports"
    ):
        logging.getLogger(__name__).info(
            f" Saved revenue report msg: {db_update.raw_result}"
        )
    else:
        logging.getLogger(__name__).error(
            f" Error saving revenue report. No msg returned."
        )
        if db_update := get_default_globaldb().save_item_to_database(
            data=revenue, collection_name="reports"
        ):
            logging.getLogger(__name__).info(
                f" Saved revenue report msg: {db_update.raw_result}"
            )
        else:
            logging.getLogger(__name__).error(
                f" Error saving revenue report. No msg returned."
            )
