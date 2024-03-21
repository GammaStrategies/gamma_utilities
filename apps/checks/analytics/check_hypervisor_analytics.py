from datetime import datetime
from decimal import Decimal
import logging

import tqdm
from apps.checks.helpers.database import get_hypervisor_last_status
from apps.checks.helpers.endpoint import get_csv_analytics_data_from_endpoint
from bins.apis.etherscan_utilities import etherscan_helper
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, text_to_chain
from bins.w3.protocols.gamma.collectors import generic_transfer_collector


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
        # we should only analyze csv of the highest period available
        # so that no warning messages are repeated
        _highest_period = 1
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

            _highest_period = max(_highest_period, period)

        # analyze the highest period
        for message in analyze_csv(
            chain=chain,
            hypervisor_address=hypervisor["address"],
            period=_highest_period,
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
    for idx, row in enumerate(csv_data):
        if idx + 1 < len(csv_data):

            # price analysis
            messages += check_prices(
                row1=row,
                row2=csv_data[idx + 1],
                threshold=0.6,
                hypervisor_status=hypervisor_status,
            )
            # weights analysis
            # messages += check_weights(row1=row, row2=csv_data[idx + 1], threshold=15)
            # divergence analysis
            messages += check_divergence(
                row1=row,
                row2=csv_data[idx + 1],
                threshold=0.2,
                hypervisor_status=hypervisor_status,
            )
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


def calculate_tvl(row: dict) -> Decimal:
    """Calculate the TVL of a row"""
    return row["status.end.prices.share"] * row["status.end.supply"]


def check_prices(
    row1: dict,
    row2: dict,
    threshold: float = 0.7,
    hypervisor_status: dict | None = None,
) -> list[str]:
    """Check if prices have a great difference."""

    messages = []

    # get initial and end datetimes
    datetime_ini1 = datetime.fromtimestamp(row1["timestamp_from"])
    datetime_end1 = datetime.fromtimestamp(row1["timestamp"])
    datetime_ini2 = datetime.fromtimestamp(row2["timestamp_from"])
    datetime_end2 = datetime.fromtimestamp(row2["timestamp"])

    # ease
    token0_name = (
        hypervisor_status["pool"]["token0"]["symbol"] if hypervisor_status else "token0"
    )
    token1_name = (
        hypervisor_status["pool"]["token1"]["symbol"] if hypervisor_status else "token1"
    )
    token0_address = (
        hypervisor_status["pool"]["token0"]["address"] if hypervisor_status else ""
    )
    token1_address = (
        hypervisor_status["pool"]["token1"]["address"] if hypervisor_status else ""
    )

    #
    seconds_passed_1 = row1["timestamp"] - row1["timestamp_from"]

    # check if initial and end prices within a row have a great difference
    change_token0_within_period = (
        row1["status.end.prices.token0"] - row1["status.ini.prices.token0"]
    ) / row1["status.ini.prices.token0"]
    change_token1_within_period = (
        row1["status.end.prices.token1"] - row1["status.ini.prices.token1"]
    ) / row1["status.ini.prices.token1"]

    change_rate_token0_within_period_perDay = (
        ((change_token0_within_period / seconds_passed_1) * 86400)
        if seconds_passed_1
        else 0
    )
    change_rate_token1_within_period_perDay = (
        ((change_token1_within_period / seconds_passed_1) * 86400)
        if seconds_passed_1
        else 0
    )

    if (
        abs(change_token0_within_period) >= threshold
        and abs(change_rate_token0_within_period_perDay) >= threshold
    ):
        messages.append(
            f" PRICE: {token0_name} has changed by {change_token0_within_period:,.1%} within the period from {datetime_ini1} [${row1['status.ini.prices.token0']:,.2f}] to {datetime_end2} [${row1['status.end.prices.token0']:,.2f}]. blocks: {row1['block']} to {row2['block']}  {token0_address}"
        )
    if (
        abs(change_token1_within_period) >= threshold
        and abs(change_rate_token1_within_period_perDay) >= threshold
    ):
        messages.append(
            f" PRICE: {token1_name} has changed by {change_token1_within_period:,.1%} within the period from {datetime_ini1} [${row1['status.ini.prices.token1']:,.2f}] to {datetime_end2} [${row1['status.end.prices.token1']:,.2f}] blocks: {row1['block']} to {row2['block']}  {token1_address}"
        )

    seconds_passed_2 = row2["timestamp"] - row2["timestamp_from"]
    seconds_passed_btween_inis = row2["timestamp_from"] - row1["timestamp_from"]

    # check if initial and end prices between rows have a great difference ( ini vs ini, end vs end)
    change_token0_between_ini_rows = (
        row2["status.ini.prices.token0"] - row1["status.ini.prices.token0"]
    ) / row1["status.ini.prices.token0"]
    change_token1_between_ini_rows = (
        row2["status.ini.prices.token1"] - row1["status.ini.prices.token1"]
    ) / row1["status.ini.prices.token1"]

    change_rate_token0_between_ini_rows_perDay = (
        ((change_token0_between_ini_rows / seconds_passed_btween_inis) * 86400)
        if seconds_passed_btween_inis
        else 0
    )
    change_rate_token1_between_ini_rows_perDay = (
        ((change_token1_between_ini_rows / seconds_passed_btween_inis) * 86400)
        if seconds_passed_btween_inis
        else 0
    )

    if (
        abs(change_token0_between_ini_rows) >= threshold
        and abs(change_rate_token0_between_ini_rows_perDay) >= threshold
    ):
        messages.append(
            f" PRICE: {token0_name} has changed by {change_token0_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [${row1['status.ini.prices.token0']:,.2f}] and {datetime_ini2} [${row2['status.ini.prices.token0']:,.2f}] blocks: {row1['block']} to {row2['block']}  {token0_address}"
        )
    if (
        abs(change_token1_between_ini_rows) >= threshold
        and abs(change_rate_token1_between_ini_rows_perDay) >= threshold
    ):
        messages.append(
            f" PRICE: {token1_name} has changed by {change_token1_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [${row1['status.ini.prices.token1']:,.2f}] and {datetime_ini2} [${row2['status.ini.prices.token1']:,.2f}] blocks: {row1['block']} to {row2['block']}  {token1_address}"
        )
    change_token0_between_end_rows = (
        row2["status.end.prices.token0"] - row1["status.end.prices.token0"]
    ) / row1["status.end.prices.token0"]
    change_token1_between_end_rows = (
        row2["status.end.prices.token1"] - row1["status.end.prices.token1"]
    ) / row1["status.end.prices.token1"]

    seconds_passed_btween_ends = row2["timestamp"] - row1["timestamp"]
    change_rate_token0_between_end_rows_perDay = (
        ((change_token0_between_end_rows / seconds_passed_btween_ends) * 86400)
        if seconds_passed_btween_ends
        else 0
    )
    change_rate_token1_between_end_rows_perDay = (
        ((change_token1_between_end_rows / seconds_passed_btween_ends) * 86400)
        if seconds_passed_btween_ends
        else 0
    )

    if (
        abs(change_token0_between_end_rows) >= threshold
        and abs(change_rate_token0_between_end_rows_perDay) >= threshold
    ):
        messages.append(
            f" PRICE: {token0_name} has changed by {change_token0_between_end_rows:,.1%} within 2 periods between {datetime_end1} [${row1['status.end.prices.token0']:,.2f}] and {datetime_end2} [${row2['status.end.prices.token0']:,.2f}] blocks: {row1['block']} to {row2['block']}  {token0_address}"
        )
    if (
        abs(change_token1_between_end_rows) >= threshold
        and abs(change_rate_token1_between_end_rows_perDay) >= threshold
    ):
        messages.append(
            f" PRICE: {token1_name} has changed by {change_token1_between_end_rows:,.1%} within 2 periods between {datetime_end1} [${row1['status.end.prices.token1']:,.2f}] and {datetime_end2} [${row2['status.end.prices.token1']:,.2f}] blocks: {row1['block']} to {row2['block']} {token1_address}"
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

    seconds_passed_1 = row1["timestamp"] - row1["timestamp_from"]
    seconds_passed_2 = row2["timestamp"] - row2["timestamp_from"]

    seconds_passed_btween_inis = row2["timestamp_from"] - row1["timestamp_from"]
    seconds_passed_btween_ends = row2["timestamp"] - row1["timestamp"]

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

    if not tvl_ini1 or not tvl_end1:
        messages.append(
            f" TVL is zero in timestamps from {datetime_ini1} to {datetime_end1}"
        )
    if not tvl_ini2 or not tvl_end2:
        messages.append(
            f" TVL is zero in timestamps from {datetime_ini2} to {datetime_end2}"
        )

    if not tvl_ini1 or not tvl_end1 or not tvl_ini2 or not tvl_end2:
        return messages

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
        ((weight_token0_end1 - weight_token0_ini1) / weight_token0_ini1)
        if weight_token0_ini1 != 0
        else 0
    )
    change_weight_token1_within_period = (
        ((weight_token1_end1 - weight_token1_ini1) / weight_token1_ini1)
        if weight_token1_ini1 != 0
        else 0
    )

    # rate of change per day
    change_weight_token0_within_period_perDay = (
        ((change_weight_token0_within_period / seconds_passed_1) * 86400)
        if seconds_passed_1
        else 0
    )
    change_weight_token1_within_period_perDay = (
        ((change_weight_token1_within_period / seconds_passed_1) * 86400)
        if seconds_passed_1
        else 0
    )
    if (
        abs(change_weight_token0_within_period) >= threshold
        and abs(change_weight_token0_within_period_perDay) >= threshold
    ):
        messages.append(
            f" WEIGHT token0 has changed by {change_weight_token0_within_period:,.1%} within the period from {datetime_ini1} [{weight_token0_ini1:,.2f}] to {datetime_end1} [{weight_token0_end1:,.2f}]"
        )
    if (
        abs(change_weight_token1_within_period) >= threshold
        and abs(change_weight_token1_within_period_perDay) >= threshold
    ):
        messages.append(
            f" WEIGHT token1 has changed by {change_weight_token1_within_period:,.1%} within the period from {datetime_ini1} [{weight_token1_ini1:,.2f}] to {datetime_end1} [{weight_token1_end1:,.2f}]"
        )

    # check if initial weights between rows have a great difference ( ini vs ini, end vs end)
    change_weight_token0_between_ini_rows = (
        ((weight_token0_ini2 - weight_token0_ini1) / weight_token0_ini1)
        if weight_token0_ini1 != 0
        else 0
    )
    change_weight_token1_between_ini_rows = (
        ((weight_token1_ini2 - weight_token1_ini1) / weight_token1_ini1)
        if weight_token1_ini1 != 0
        else 0
    )

    # rate of change per day
    change_weight_token0_between_ini_rows_perDay = (
        ((change_weight_token0_between_ini_rows / seconds_passed_btween_inis) * 86400)
        if seconds_passed_btween_inis
        else 0
    )
    change_weight_token1_between_ini_rows_perDay = (
        ((change_weight_token1_between_ini_rows / seconds_passed_btween_inis) * 86400)
        if seconds_passed_btween_inis
        else 0
    )

    if (
        abs(change_weight_token0_between_ini_rows) >= threshold
        and abs(change_weight_token0_between_ini_rows_perDay) >= threshold
    ):
        messages.append(
            f" WEIGHT token0 has changed by {change_weight_token0_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [{weight_token0_ini1:,.2f}] and {datetime_ini2} [{weight_token0_ini2:,.2f}]"
        )
    if (
        abs(change_weight_token1_between_ini_rows) >= threshold
        and abs(change_weight_token1_between_ini_rows_perDay) >= threshold
    ):
        messages.append(
            f" WEIGHT token1 has changed by {change_weight_token1_between_ini_rows:,.1%} within 2 periods between {datetime_ini1} [{weight_token1_ini1:,.2f}] and {datetime_ini2} [{weight_token1_ini2:,.2f}]"
        )
    change_weight_token0_between_end_rows = (
        ((weight_token0_end2 - weight_token0_end1) / weight_token0_end1)
        if weight_token0_end1 != 0
        else 0
    )
    change_weight_token1_between_end_rows = (
        ((weight_token1_end2 - weight_token1_end1) / weight_token1_end1)
        if weight_token1_end1 != 0
        else 0
    )

    # rate of change per day
    change_weight_token0_between_end_rows_perDay = (
        ((change_weight_token0_between_end_rows / seconds_passed_btween_ends) * 86400)
        if seconds_passed_btween_ends
        else 0
    )
    change_weight_token1_between_end_rows_perDay = (
        ((change_weight_token1_between_end_rows / seconds_passed_btween_ends) * 86400)
        if seconds_passed_btween_ends
        else 0
    )

    if (
        abs(change_weight_token0_between_end_rows) >= threshold
        and abs(change_weight_token0_between_end_rows_perDay) >= threshold
    ):
        messages.append(
            f" WEIGHT token0 has changed by {change_weight_token0_between_end_rows:,.1%} within 2 periods between {datetime_end1} [{weight_token0_end1:,.2f}] and {datetime_end2} [{weight_token0_end2:,.2f}]"
        )
    if (
        abs(change_weight_token1_between_end_rows) >= threshold
        and abs(change_weight_token1_between_end_rows_perDay) >= threshold
    ):
        messages.append(
            f" WEIGHT token1 has changed by {change_weight_token1_between_end_rows:,.1%} within 2 periods between {datetime_end1} [{weight_token1_end1:,.2f}] and {datetime_end2} [{weight_token1_end2:,.2f}]"
        )

    # return result
    return messages


def check_divergence(
    row1: dict,
    row2: dict,
    threshold: float = 0.1,
    hypervisor_status: dict | None = None,
) -> list[str]:
    """Check if row1 vs row2 divergence is eq or higher than threshold
    Will check for positive values only
    """

    # set address to check for direct transfers
    wallet_address = "0x71e7d05be74ff748c45402c06a941c822d756dc5".lower()

    messages = []

    # TVL must be low to avoid false positives
    if calculate_tvl(row1) > 100:
        return messages

    try:
        divergence = row2["divergence.period.yield"] - row1["divergence.period.yield"]

        if divergence >= threshold:

            # check if there is actually a direct transfer to this hype
            logging.getLogger(__name__).debug(
                f" Checking if direct transfers exist between {(row2['timestamp']-row1['timestamp'])/(60*60*24):,.1f} days"
            )
            direct_transfers_filtered = []

            # TRY USING the friendly EHTERSCAN
            # create an etherescan helper
            cg_helper = etherscan_helper(api_keys=CONFIGURATION["sources"]["api_keys"])
            if cg_helper._check_network_available(network=row1["chain"]):
                # check if there are direct transfers to this hype
                if direct_transfers := cg_helper.get_wallet_erc20_transactions(
                    network=row1["chain"],
                    wallet_address=wallet_address,
                    startblock=row1["block"],
                    endblock=row2["block"],
                ):
                    # check if its to th hype
                    for direct_transfer in direct_transfers:
                        if direct_transfer["to"].lower() == row1["address"]:
                            # check if not in database
                            if get_from_localdb(
                                network=row1["chain"],
                                collection="operations",
                                find={
                                    "$or": [
                                        {"transactionHash": direct_transfer["hash"]},
                                        {
                                            "$and": [
                                                {
                                                    "blockNumber": int(
                                                        direct_transfer["blockNumber"]
                                                    )
                                                },
                                                {"address": row1["address"]},
                                                {"qtty": direct_transfer["value"]},
                                                {
                                                    "$or": [
                                                        {"src": wallet_address},
                                                        {"dst": wallet_address},
                                                        {"sender": wallet_address},
                                                        {"to": wallet_address},
                                                    ]
                                                },
                                            ],
                                        },
                                    ]
                                },
                            ):
                                # this is a deposit or withdraw
                                continue
                            direct_transfers_filtered.append(direct_transfer)
                else:
                    # no transfers found
                    pass

            # TRY USING DIRECT CALLS TO CHAIN
            elif direct_transfers := scrape_transfers_to_hypes(
                chain=text_to_chain(row1["chain"]),
                from_addresses=[wallet_address],
                to_addresses=[row2["address"]],
                block_ini=row1["block"],
                block_end=row2["block"],
                max_blocks_step=10000,
            ):

                # check that those operations are not in database ( corresponding to deposits or withdraws)
                for direct_transfer in direct_transfers:
                    if get_from_localdb(
                        network=row1["chain"],
                        collection="operations",
                        find={"transactionHash": direct_transfer["transactionHash"]},
                    ):
                        # this is a deposit or withdraw
                        continue
                    direct_transfers_filtered.append(direct_transfer)

            #
            #
            # ADD MESSAGE
            if direct_transfers_filtered:
                messages.append(
                    f" DIRECT TRANSFER: Found {len(direct_transfers_filtered)} transfers from {wallet_address}. Remove items before timestamp {row2['timestamp']}"
                )
            else:
                pass
    except Exception as e:
        logging.getLogger(__name__).exception(f"Error in check_divergence: {e}")

    return messages


def scrape_transfers_to_hypes(
    chain: Chain,
    from_addresses: list[str],
    to_addresses: list[str],
    block_ini: int,
    block_end: int,
    max_blocks_step: int = 1000,
) -> list[dict]:
    transfer_data_helper = generic_transfer_collector(
        network=chain.database_name,
        from_addresses=from_addresses,
        to_addresses=to_addresses,
    )

    transfers_to_hypes = []

    with tqdm.tqdm(total=100) as progress_bar:
        # create callback progress funtion
        def _update_progress(text=None, remaining=None, total=None):
            # set text
            if text:
                progress_bar.set_description(text)
            # set total
            if total:
                progress_bar.total = total
            # update current
            if remaining:
                progress_bar.update(((total - remaining) - progress_bar.n))
            else:
                progress_bar.update(1)
            # refresh
            progress_bar.refresh()

        # set progress callback to data collector
        transfer_data_helper.progress_callback = _update_progress

        for operations in transfer_data_helper.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            max_blocks=max_blocks_step,
        ):
            # filter any transfer that is not a direct transfer ( deposit or withdraw)
            for operation in operations:
                if operation["address"] in to_addresses:
                    # this is a deposit or withdraw,
                    continue
                # process operation
                transfers_to_hypes.append(operation)

    return transfers_to_hypes
