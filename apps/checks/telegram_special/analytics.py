from datetime import datetime, timezone
import logging
from apps.checks.analytics.check_hypervisor_analytics import analyze_csv
from apps.checks.helpers.database import (
    get_hypervisor_related_operations,
    get_last_operation,
)
from apps.checks.helpers.endpoint import get_csv_analytics_data_from_endpoint
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol
from bins.log.telegram_logger import send_to_telegram


class monitor_hypervisor_analytics:
    def __init__(self, chain: Chain, hypervisor_static: dict, period: int):
        self.chain = chain
        self.hypervisor_static = hypervisor_static
        self.period = period

    def __call__(self):
        """Monitor and sent do telegram any found inconsistency or error in hypervisor analytics data"""
        # get csv data from endpoint
        csv_data = get_csv_analytics_data_from_endpoint(
            chain=self.chain,
            hypervisor_address=self.hypervisor_static["address"],
            period=self.period,
        )

        # 1) DATA NOT RETURNED ############################
        if not csv_data:
            # get last database operation/event known and telegram log it.
            last_operation = get_last_operation(
                chain=self.chain, hypervisor_address=self.hypervisor_static["address"]
            )

            # queued items exist waiting to be processed? ( search for operations)
            queued_operations = {}
            for qitm in get_hypervisor_related_operations(
                chain=self.chain, hypervisor_address=self.hypervisor_static["address"]
            ):
                if not qitm["type"] in queued_operations:
                    queued_operations[qitm["type"]] = 0
                queued_operations[qitm["type"]] += 1

            # days passed since last operation
            _days_since_last_operation = (
                datetime.now(timezone.utc)
                - datetime.fromtimestamp(last_operation["timestamp"], tz=timezone.utc)
            ).days

            # send telegram message
            send_to_telegram.error(
                msg=[
                    f"<pre>\n No analytics data for the period {self.period}</pre>",
                    f"<pre>\n {self.chain.fantasy_name} {self.hypervisor_static['dex']}'s hype {self.hypervisor_static['address']} {self.hypervisor_static['symbol']} </pre>",
                    f"<pre>\n Last database event-> {last_operation['topic']} at block {last_operation['blockNumber']} [{_days_since_last_operation} days ago ] txHash: {last_operation['transactionHash']}</pre>",
                    f"<pre>\n Queued items-> {' '.join([f'{k}:{v}' for k, v in queued_operations.items()])} </pre>",
                ],
                topic="analytics",
                dtime=True,
            )

            # exit this hypervisor period check
            return

        # 2) DATA CONSISTENCY  ############################
        if messages := analyze_csv(
            chain=self.chain,
            hypervisor_address=self.hypervisor_static["address"],
            period=self.period,
            csv_data=csv_data,
        ):

            # inserts
            messages.insert(0, f"<pre>\n Potential data errors </pre>")
            messages.insert(
                1,
                f"<pre>\n {self.chain.fantasy_name} {self.hypervisor_static['dex']}'s hype {self.hypervisor_static['address']} {self.hypervisor_static['symbol']}</pre>",
            )

            # send telegram message
            send_to_telegram.warning(
                msg=messages,
                topic="analytics",
                dtime=True,
            )

        else:
            # analytic data seems fine
            pass


def telegram_checks(
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
        ]
        _find = {"address": {"$nin": exclude_addresses}}
        if protocols:
            _find["dex"] = {"$in": [x.database_name for x in protocols]}
        hypervisor_static_list = get_from_localdb(
            network=chain.database_name,
            collection="static",
            find=_find,
            # projection={"address": 1, "_id": 0},
        )

    logging.getLogger(__name__).info(
        f" Checking {len(hypervisor_static_list)} hypervisors for {chain.fantasy_name} chain "
    )
    #
    for hypervisor_static in hypervisor_static_list:

        for period in periods:
            # create monitor
            monitor = monitor_hypervisor_analytics(
                chain=chain,
                hypervisor_static=hypervisor_static,
                period=period,
            )
            # call it
            monitor()