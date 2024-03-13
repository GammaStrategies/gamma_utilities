import calendar
import logging

from apps.checks.helpers.endpoint import get_revenue_data_from_endpoint
from bins.general.general_utilities import millify
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

    last_relative_revenue_vs_fees = (
        last_month["total_revenue"] / last_month["total_fees"]
    )
    last_relative_revenue_vs_volume = (
        last_month["total_revenue"] / last_month["total_volume"]
    )

    # aggregate values
    aggregated_values = {}
    for itm in last_month["items"]:
        if itm["exchange"] not in aggregated_values:
            aggregated_values[itm["exchange"]] = {
                "total_revenue": 0,
                "total_fees": 0,
                "total_volume": 0,
            }
        aggregated_values[itm["exchange"]]["total_revenue"] += itm["total_revenue"]
        aggregated_values[itm["exchange"]]["total_fees"] += itm["total_fees"]
        aggregated_values[itm["exchange"]]["total_volume"] += itm["total_volume"]

    # send information to telegram
    send_to_telegram.info(
        msg=[
            f"<b>\n Revenue at {calendar.month_name[last_month['month']]} {last_month['year']}: </b>",
            f"<i> ${millify(last_month['total_revenue'])}  [ {last_relative_revenue_vs_fees:,.2%} of fees  |  {last_relative_revenue_vs_volume:,.4%} of vol] </i>",
            f"<i>\n ( revenue potential for {calendar.month_name[last_month['month']]} {last_month['year']}: ${millify(last_revenue['total_revenue_potential']/12)} )</i>",
            f"<b>\n Revenue in {last_month['year']}: </b>",
            f"<i> ${millify(last_revenue['total_revenue'])}  [ {total_relative_revenue_vs_fees:,.2%} of fees  |  {total_relative_revenue_vs_volume:,.4%} of vol] </i>",
            f" ",
            f" Revenue by exchange [{len(aggregated_values)}]:",
        ]
        + [
            f" <b><i>{exchange}</i></b>:  ${millify(values['total_revenue'])}  [ {(values['total_revenue'] / values['total_fees']):,.2%} of fees | {(values['total_revenue'] / values['total_volume']):,.4%} of vol]"
            for exchange, values in aggregated_values.items()
        ],
        topic="revenue",
        dtime=False,
    )
