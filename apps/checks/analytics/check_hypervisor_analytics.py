from datetime import datetime
from decimal import Decimal
import logging
from apps.checks.helpers.database import get_hypervisor_last_status
from apps.checks.helpers.endpoint import get_csv_analytics_data_from_endpoint
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol


# MAIN CHECK #####################################################################################################################
def check_hypervisors_analytics(
    chain: Chain,
    protocols: list[Protocol] | None = None,
    hypervisor_address: str | None = None,
    periods: list[int] = [1, 7, 14, 30, 90, 180, 365],
):
    """Execute a check on the hypervisors analytics for a chain and protocol using the gamma endpoint

    Args:
        chain (Chain): network chain
        protocols (list[Protocol] | None, optional): protocol. Defaults to all.
        hypervisor_address (str | None, optional): address. Defaults to all.
        periods (list[int], optional): periods as days accepted by endpoint. Defaults to [1, 7, 14, 30, 90, 180, 365].
    """

    if not hypervisor_address:
        _find = (
            {}
            if not protocols
            else {"dex": {"$in": [x.database_name for x in protocols]}}
        )

        hypervisors_static_list = get_from_localdb(
            network=chain.database_name,
            collection="static",
            find=_find,
            projection={"address": 1, "_id": 0},
        )
    else:
        hypervisors_static_list = [{"address": hypervisor_address}]

    logging.getLogger(__name__).info(
        f" Testing {len(hypervisors_static_list)} hypervisors for {chain.fantasy_name} chain"
    )
    for hypervisor in hypervisors_static_list:
        for period in periods:
            # get csv data
            csv_data = get_csv_analytics_data_from_endpoint(
                chain=chain, hypervisor_address=hypervisor["address"], period=period
            )
            # check if csv data is empty
            if not csv_data:
                logging.getLogger(__name__).info(
                    f"      ERROR: No csv data found for hypervisor {hypervisor['address']}"
                )
                continue
            # analyze
            for message in analyze_csv(
                chain=chain,
                hypervisor_address=hypervisor["address"],
                period=period,
                csv_data=csv_data,
            ):
                # log messages
                logging.getLogger(__name__).info(message)


# CHECK ####################################################################################################################
def analyze_csv(
    chain: Chain, hypervisor_address: str, period: int, csv_data: list[dict]
) -> list[str]:

    # get hypervisor last status
    hypervisor_status = get_hypervisor_last_status(
        chain=chain, hypervisor_address=hypervisor_address
    )
    if not hypervisor_status:
        logging.getLogger(__name__).error(
            f" No status found for hypervisor {hypervisor_address}"
        )
        return

    logging.getLogger(__name__).info(
        f" Analyzing hypervisor {hypervisor_status['name']} for period {period} days [ {hypervisor_status['address']} ]"
    )

    # get all rebalances for the period
    rebalances = {
        x["blockNumber"]: x
        for x in get_from_localdb(
            network=chain.database_name,
            collection="operations",
            find={
                "address": hypervisor_address,
                "topic": "rebalance",
                "blockNumber": {
                    "$gte": csv_data[0]["block"],
                    "$lte": csv_data[-1]["block"],
                },
            },
            sort=[("block", 1)],
        )
    }

    # analyze csv_data ( first row is the header)
    messages = []
    for idx, row in enumerate(csv_data[1:]):
        if idx + 1 < len(csv_data):
            # price analysis
            messages += check_prices(row1=row, row2=csv_data[idx + 1], threshold=0.15)
            # weights analysis
            messages += check_weights(row1=row, row2=csv_data[idx + 1], threshold=10)
            # rebalance analysis
            if row["block"] in rebalances:
                messages.append(
                    f"  -> A rebalance occured in {datetime.fromtimestamp(rebalances[row['block']]['timestamp'])}"
                )
            elif csv_data[idx + 1]["block"] in rebalances:
                messages.append(
                    f"  -> A rebalance occured in {datetime.fromtimestamp(rebalances[csv_data[idx + 1]['block']]['timestamp'])}"
                )

    return messages


def check_prices(row1: dict, row2: dict, threshold: float = 0.3) -> list[str]:
    """Check if prices have a great difference"""

    messages = []

    # get initial and end datetimes
    datetime_ini1 = datetime.fromtimestamp(row1["timestamp_from"])
    datetime_end1 = datetime.fromtimestamp(row1["timestamp"])
    datetime_ini2 = datetime.fromtimestamp(row2["timestamp_from"])
    datetime_end2 = datetime.fromtimestamp(row2["timestamp"])

    # check if initial and end prices within a row have a great difference
    change_token0_within_period = (
        row1["status.end.prices.token0"] - row1["status.ini.prices.token0"]
    ) / row1["status.ini.prices.token0"]
    change_token1_within_period = (
        row1["status.end.prices.token1"] - row1["status.ini.prices.token1"]
    ) / row1["status.ini.prices.token1"]
    if abs(change_token0_within_period) >= threshold:
        messages.append(
            f" PRICE: token0 has changed by {change_token0_within_period:,.1%} within the period from {datetime_ini1} [${row1['status.ini.prices.token0']:,.2f}] to {datetime_end2} [${row1['status.end.prices.token0']:,.2f}]"
        )
    if abs(change_token1_within_period) >= threshold:
        messages.append(
            f" PRICE: token1 has changed by {change_token1_within_period:,.1%} within the period from {datetime_ini1} [${row1['status.ini.prices.token1']:,.2f}] to {datetime_end2} [${row1['status.end.prices.token1']:,.2f}]"
        )

    # check if initial and end prices between rows have a great difference ( ini vs ini, end vs end)
    change_token0_between_ini_rows = (
        row2["status.ini.prices.token0"] - row1["status.ini.prices.token0"]
    ) / row1["status.ini.prices.token0"]
    change_token1_between_ini_rows = (
        row2["status.ini.prices.token1"] - row1["status.ini.prices.token1"]
    ) / row1["status.ini.prices.token1"]
    if abs(change_token0_between_ini_rows) >= threshold:
        messages.append(
            f" PRICE: token0 has changed by {change_token0_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [${row1['status.ini.prices.token0']:,.2f}] and {datetime_ini2} [${row2['status.ini.prices.token0']:,.2f}]"
        )
    if abs(change_token1_between_ini_rows) >= threshold:
        messages.append(
            f" PRICE: token1 has changed by {change_token1_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [${row1['status.ini.prices.token1']:,.2f}] and {datetime_ini2} [${row2['status.ini.prices.token1']:,.2f}]"
        )
    change_token0_between_end_rows = (
        row2["status.end.prices.token0"] - row1["status.end.prices.token0"]
    ) / row1["status.end.prices.token0"]
    change_token1_between_end_rows = (
        row2["status.end.prices.token1"] - row1["status.end.prices.token1"]
    ) / row1["status.end.prices.token1"]
    if abs(change_token0_between_end_rows) >= threshold:
        messages.append(
            f" PRICE: token0 has changed by {change_token0_between_end_rows:,.1%} within 2 periods between {datetime_end1} [${row1['status.end.prices.token0']:,.2f}] and {datetime_end2} [${row2['status.end.prices.token0']:,.2f}]"
        )
    if abs(change_token1_between_end_rows) >= threshold:
        messages.append(
            f" PRICE: token1 has changed by {change_token1_between_end_rows:,.1%} within 2 periods between {datetime_end1} [${row1['status.end.prices.token1']:,.2f}] and {datetime_end2} [${row2['status.end.prices.token1']:,.2f}]"
        )

    # return result
    return messages


def check_weights(row1: dict, row2: dict, threshold: float = 0.3) -> list[str]:
    """Check if weights have a great difference"""

    messages = []

    # get initial and end datetimes
    datetime_ini1 = datetime.fromtimestamp(row1["timestamp_from"])
    datetime_end1 = datetime.fromtimestamp(row1["timestamp"])
    datetime_ini2 = datetime.fromtimestamp(row2["timestamp_from"])
    datetime_end2 = datetime.fromtimestamp(row2["timestamp"])

    # define tvls
    token0_ini1 = (
        row1["status.ini.prices.token0"] * row1["status.ini.underlying.qtty.token0"]
    )
    token1_ini1 = (
        row1["status.ini.prices.token1"] * row1["status.ini.underlying.qtty.token1"]
    )
    token0_end1 = (
        row1["status.end.prices.token0"] * row1["status.end.underlying.qtty.token0"]
    )
    token1_end1 = (
        row1["status.end.prices.token1"] * row1["status.end.underlying.qtty.token1"]
    )
    tvl_ini1 = token0_ini1 + token1_ini1
    tvl_end1 = token0_end1 + token1_end1
    token0_ini2 = (
        row2["status.ini.prices.token0"] * row2["status.ini.underlying.qtty.token0"]
    )
    token1_ini2 = (
        row2["status.ini.prices.token1"] * row2["status.ini.underlying.qtty.token1"]
    )
    token0_end2 = (
        row2["status.end.prices.token0"] * row2["status.end.underlying.qtty.token0"]
    )
    token1_end2 = (
        row2["status.end.prices.token1"] * row2["status.end.underlying.qtty.token1"]
    )
    tvl_ini2 = token0_ini2 + token1_ini2
    tvl_end2 = token0_end2 + token1_end2

    # define weights
    weight_token0_ini1 = token0_ini1 / tvl_ini1
    weight_token1_ini1 = token1_ini1 / tvl_ini1
    weight_token0_end1 = token0_end1 / tvl_end1
    weight_token1_end1 = token1_end1 / tvl_end1
    weight_token0_ini2 = token0_ini2 / tvl_ini2
    weight_token1_ini2 = token1_ini2 / tvl_ini2
    weight_token0_end2 = token0_end2 / tvl_end2
    weight_token1_end2 = token1_end2 / tvl_end2

    # check if initial weights within a row have a great difference
    change_weight_token0_within_period = (
        weight_token0_end1 - weight_token0_ini1
    ) / weight_token0_ini1
    change_weight_token1_within_period = (
        weight_token1_end1 - weight_token1_ini1
    ) / weight_token1_ini1
    if abs(change_weight_token0_within_period) >= threshold:
        messages.append(
            f" WEIGHT token0 has changed by {change_weight_token0_within_period:,.1%} within the period from {datetime_ini1} [{weight_token0_ini1:,.2f}] to {datetime_end1} [{weight_token0_end1:,.2f}]"
        )
    if abs(change_weight_token1_within_period) >= threshold:
        messages.append(
            f" WEIGHT token1 has changed by {change_weight_token1_within_period:,.1%} within the period from {datetime_ini1} [{weight_token1_ini1:,.2f}] to {datetime_end1} [{weight_token1_end1:,.2f}]"
        )

    # check if initial weights between rows have a great difference ( ini vs ini, end vs end)
    change_weight_token0_between_ini_rows = (
        weight_token0_ini2 - weight_token0_ini1
    ) / weight_token0_ini1
    change_weight_token1_between_ini_rows = (
        weight_token1_ini2 - weight_token1_ini1
    ) / weight_token1_ini1
    if abs(change_weight_token0_between_ini_rows) >= threshold:
        messages.append(
            f" WEIGHT token0 has changed by {change_weight_token0_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [{weight_token0_ini1:,.2f}] and {datetime_ini2} [{weight_token0_ini2:,.2f}]"
        )
    if abs(change_weight_token1_between_ini_rows) >= threshold:
        messages.append(
            f" WEIGHT token1 has changed by {change_weight_token1_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [{weight_token1_ini1:,.2f}] and {datetime_ini2} [{weight_token1_ini2:,.2f}]"
        )
    change_weight_token0_between_end_rows = (
        weight_token0_end2 - weight_token0_end1
    ) / weight_token0_end1
    change_weight_token1_between_end_rows = (
        weight_token1_end2 - weight_token1_end1
    ) / weight_token1_end1
    if abs(change_weight_token0_between_end_rows) >= threshold:
        messages.append(
            f" WEIGHT token0 has changed by {change_weight_token0_between_end_rows:,.1%} within 2 periods between {datetime_end1} [{weight_token0_end1:,.2f}] and {datetime_end2} [{weight_token0_end2:,.2f}]"
        )
    if abs(change_weight_token1_between_end_rows) >= threshold:
        messages.append(
            f" WEIGHT token1 has changed by {change_weight_token1_between_end_rows:,.1%} within 2 periods between {datetime_end1} [{weight_token1_end1:,.2f}] and {datetime_end2} [{weight_token1_end2:,.2f}]"
        )

    # return result
    return messages
