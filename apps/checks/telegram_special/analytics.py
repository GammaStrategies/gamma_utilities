from datetime import datetime, timezone
from decimal import Decimal
import logging

import tqdm
from apps.checks.analytics.check_hypervisor_analytics import analytics_analyzer
from apps.checks.base_objects import analysis_item, base_analyzer_object
from apps.checks.helpers.database import (
    get_hypervisor_last_status,
    get_hypervisor_related_operations,
    get_last_operation,
)
from apps.checks.helpers.endpoint import get_csv_analytics_data_from_endpoint
from bins.apis.etherscan_utilities import etherscan_helper
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, text_to_chain
from bins.log.telegram_logger import send_to_telegram
from bins.w3.protocols.gamma.collectors import generic_transfer_collector


EXCLUDED = {
    Chain.ETHEREUM: [
        "0x85a5326f08c44ec673e4bfc666b737f7f3dc6b37",  # Ethereum uniswapv3's vGRO-ETH3
        "0xa625ea468a4c70f13f9a756ffac3d0d250a5c276",  # Ethereum uniswapv3's vRAW-ETH10
        "0xac0f71f2492daf020f459bd163052b9dae28f159",  # Ethereum uniswapv3's xcbETH-WETH05
        "0xb542f4cb10b7913307e3ed432acd9bf2e709f5fa",  # Ethereum uniswapv3's sOHM-ETH10
        "0x8a9570ec97534277ade6e46d100939fbce4968f0",  # Optimism uniswapv3's xOP-USDC3
    ]
}


# CHECK ####################################################################################################################


def telegram_checks_analytics(
    chain: Chain,
    protocols: list[Protocol] | None = None,
    hypervisor_addresses: list[str] | None = None,
    periods: list[int] = [1, 7, 14, 30, 90, 180, 365],
):

    if not hypervisor_addresses:
        # Get hypervisor addresses from database.
        # do not include blacklisted hypervisors
        # get filters
        filters: dict = (
            CONFIGURATION["script"]["protocols"].get("gamma", {}).get("filters", {})
        )
        exclude_addresses = [
            x.lower()
            for x in filters.get("hypervisors_not_included", {}).get(
                chain.database_name, []
            )
        ] + EXCLUDED.get(chain, [])
        _find = {"address": {"$nin": exclude_addresses}}
        if protocols:
            _find["dex"] = {"$in": [x.database_name for x in protocols]}
        hypervisor_static_list = get_from_localdb(
            network=chain.database_name,
            collection="static",
            find=_find,
            # projection={"address": 1, "_id": 0},
        )
    else:
        hypervisor_static_list = [{"address": x.lower()} for x in hypervisor_addresses]

    logging.getLogger(__name__).info(
        f" Checking {len(hypervisor_static_list)} hypervisors for {chain.fantasy_name} chain "
    )
    # sort periods by descending order
    periods.sort(reverse=True)
    for hypervisor_static in hypervisor_static_list:
        _checks_done = False
        for period in periods:
            # get csv data
            csv_data = get_csv_analytics_data_from_endpoint(
                chain=chain,
                hypervisor_address=hypervisor_static["address"],
                period=period,
            )

            # NO DATA RETURNED
            if not csv_data or (len(csv_data) == 1 and "No data found" in csv_data[0]):
                # get last database operation/event known and telegram log it.
                last_operation = get_last_operation(
                    chain=chain,
                    hypervisor_address=hypervisor_static["address"],
                )

                # queued items exist waiting to be processed? ( search for operations)
                queued_operations = {}
                for qitm in get_hypervisor_related_operations(
                    chain=chain,
                    hypervisor_address=hypervisor_static["address"],
                ):
                    if not qitm["type"] in queued_operations:
                        queued_operations[qitm["type"]] = 0
                    queued_operations[qitm["type"]] += 1

                if not last_operation:
                    # send telegram message
                    response = send_to_telegram.error(
                        msg=[
                            f"<b>\n No analytics data for the period {period}</b>",
                            f"<i>\n {chain.fantasy_name} {hypervisor_static['dex']}'s {hypervisor_static['symbol']} </i>",
                            f"<code> {hypervisor_static['address']} </code>",
                            f"<b>\n Last database event-></b> No database event found",
                            (
                                f"<b>\n Queued items-> </b> {' '.join([f'{k}:{v}' for k, v in queued_operations.items()])}"
                                if queued_operations
                                else ""
                            ),
                        ],
                        topic="analytics",
                        dtime=True,
                    )
                    continue

                # days passed since last operation
                _days_since_last_operation = (
                    datetime.now(timezone.utc)
                    - datetime.fromtimestamp(
                        last_operation["timestamp"], tz=timezone.utc
                    )
                ).days

                # send telegram message
                response = send_to_telegram.error(
                    msg=[
                        f"<b>\n No analytics data for the period {period}</b>",
                        f"<i>\n {chain.fantasy_name} {hypervisor_static['dex']}'s {hypervisor_static['symbol']} </i>",
                        f"<code> {hypervisor_static['address']} </code>",
                        f"<b>\n Last database event-></b> {last_operation['topic']} at block {last_operation['blockNumber']} [{_days_since_last_operation} days ago ] txHash: <code>{last_operation['transactionHash']}</code>",
                        f"<b>\n Queued items-> </b> {' '.join([f'{k}:{v}' for k, v in queued_operations.items()])}",
                    ],
                    topic="analytics",
                    dtime=True,
                )
                # exit this hypervisor period check
                continue

            # RUN CHECKs ONLY ON THE HIGHEST PERIOD
            if not _checks_done:
                _checks_done = True
                # create analyzer helper
                helper = analytics_analyzer()
                helper.analyze_csv(
                    chain=chain,
                    hypervisor_address=hypervisor_static["address"],
                    period=period,
                    csv_data=csv_data,
                )

                if not helper.items:
                    # no problems found
                    continue

                # handle result items
                messages = [f"<b>\n Potential data errors </b>"]
                if "dex" in hypervisor_static:
                    messages.append(
                        f"<i>\n {chain.fantasy_name} {hypervisor_static['dex']}'s hype {hypervisor_static['symbol']}</i>"
                    )
                else:
                    messages.append(f"<i>\n {chain.fantasy_name} hype </i>")
                # add hypervisor address
                messages.append(f"<pre> {hypervisor_static['address']} </pre>")
                # add resulting messages
                messages += [x.telegram_message for x in helper.items]

                # send telegram message
                response = send_to_telegram.warning(
                    msg=messages,
                    topic="analytics",
                    dtime=True,
                )
