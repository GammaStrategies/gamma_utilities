
import logging

from apps.checks.helpers.endpoint import get_revenue_data_from_endpoint
from bins.log.telegram_logger import send_to_telegram


def telegram_checks_revenue():

    current_revenue_data = get_revenue_data_from_endpoint()
    if not current_revenue_data:
        logging.getLogger(__name__).error("No revenue data found")
        return

    # is sorted by datetime vars, so pick last item as most recent
    last_revenue = current_revenue_data[-1]
    last_month = last_revenue["items"][-1]

    # define general checks
    total_relative_revenue_vs_fees = (
        last_revenue["total_revenue"] / last_revenue["total_fees"]
    )
    total_relative_revenue_vs_volume = (
        last_revenue["total_revenue"] / last_revenue["total_volume"]
    )

    last_data_datetime = last_month["datetime"]
    last_relative_revenue_vs_fees = (
        last_month["total_revenue"] / last_month["total_fees"]
    )
    last_relative_revenue_vs_volume = (
        last_month["total_revenue"] / last_month["total_volume"]
    )

    # send information to telegram
    send_to_telegram.info(
        msg=[
            f"<b>\n Revenue at {last_data_datetime} </b>",
            f"<i>\n this month revenue: {last_month["total_revenue"]:,.0f}  this year revenue: {last_revenue["total_revenue"]:,.0f}   [ this year potential: {last_revenue["total_revenue_potential"]:,.0f}] </i>",
            f"<b>\n this year revenue vs fees</b> : {total_relative_revenue_vs_fees:,.2%}       <b>this year revenue vs volume</b> : {total_relative_revenue_vs_volume:,.2%}",
            f"<b>\n last month revenue vs fees</b> : {last_relative_revenue_vs_fees:,.2%}       <b>last month revenue vs volume</b> : {last_relative_revenue_vs_volume:,.2%}",
            f" ",
        ] + [f" <b><i>{itm['exchange']}</i></b> revenue: {itm["total_revenue"]:,.2f}   [ vs fees: {(itm["total_revenue"] / itm["total_fees"]):,.2%}  vs volume: {(itm["total_revenue"] / itm["total_volume"]):,.2%} ]" for itm in last_revenue["items"]],
        topic="revenue",
        dtime=False,
    )
