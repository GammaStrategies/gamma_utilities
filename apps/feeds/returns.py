from datetime import datetime, timezone
import logging

from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import get_price_from_db
from bins.general.enums import Period


# calculate APR for ---> periods stablished by operations
""" {
    "address": "0x000000000
    "symbol": "ETH-WETH",                               
    "block": 12345678,
    "timestamp": 12345678,
    "fees_apr": 0.00000000000000,               --->  fees APR using the defined period ( not yearly but for the defined period)
    "rewards_apr": 0.00000000000000,            --->  rewards APR using the defined period ( not yearly but for the defined period)
    "lping": 0.00000000000000,                  --->  end-initial  LP value -> Gain/loss from staying in vault, denominated in USD
    "hold_deposited": 0.00000000000000,         --->  deposited qtty value difference between ini&end  ( token0_qtty*token0_price + token1_qtty*token1_price at ini&end )
    "hold_fifty": 0.00000000000000,             --->  50% of deposited value in usd converted to token0 & 50% for token1 as qtty ( token0_qtty*token0_price + token1_qtty*token1_price at ini&end )
    "hold_token0": 0.00000000000000,            --->  100% of deposited value in usd converted to token0 qtty value difference between ini&end
    "hold_token1": 0.00000000000000,            --->  100% of deposited value in usd converted to token1 qtty value difference between ini&end
    "net_apr": 0.00000000000000,                --->  fees_apr + rewards_apr
    "impermanent_result": 0.00000000000000,     --->  lping - fees_apr
    "gamma_vs_hold": 0.00000000000000,          --->  ( (net_apr+1) / (hold_deposited+1) ) - 1
} """

# return period -> 1day 7days 14days 30days 60days 90days 180days 365days
# return points -> 24h   7p     14p    30p    60p    90p    180p    365p

# 1day apr = +24h apr
# 7days apr = +7*24h apr


def feed_returns(network: str | None = None, hypervisors: list[dict] | None = None):
    logging.getLogger(__name__).info(f">Feeding {network} returns information")

    # set local database name and create manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # get static hypervisor information
    for hypervisor in hypervisors or local_db.get_items_from_database(
        collection_name="static", find={}
    ):
        # create control vars
        last_item = None
        # get all hype status changes from creation date
        for idx, data in enumerate(
            local_db.get_items_from_database(
                collection_name="operations",
                aggregate=local_db.query_locs_apr_hypervisor_data_calculation(
                    hypervisor_address=hypervisor["address"]
                ),
            )
        ):
            if not last_item:
                # this is the first item
                last_item = data
                continue

            # zero and par indexes refer to initial values
            if idx == 0 or idx % 2 == 0:
                # this is an initial value
                last_item = data
            else:
                # this is an end value
                fees_uncollected_token0 = int(
                    data["fees_uncollected"]["qtty_token0"]
                ) - int(last_item["fees_uncollected"]["qtty_token0"])
                fees_uncollected_token1 = int(
                    data["fees_uncollected"]["qtty_token1"]
                ) - int(last_item["fees_uncollected"]["qtty_token1"])
                last_item = data


# def calculate_periods(ini_timestamp: int, end_timestamp: int | None = None):
#     """calculate periods from ini_timestamp to end_timestamp"""

#     result = []

#     current_timestamp = ini_timestamp
#     end_timestamp = end_timestamp or int(datetime.now(timezone.utc).timestamp())
#     while current_timestamp < end_timestamp:
#         # calculate periods
#         time_passed = current_timestamp - ini_timestamp
#         for period in result.keys():
#             # only add to result if suficient time is passed since hype creation
#             if time_passed >= period.days * 24 * 60 * 60:
#                 result[period].append(current_timestamp)

#                 # add one hour to current timestamp
#                 current_timestamp += period.value
#                 break


def calculate_period_percentage_yield(
    network: str, ini_hype: dict, end_hype: dict
) -> dict:
    # get token prices at ini and end blocks from database
    token_prices = {
        "token0": {
            "ini": get_price_from_db(
                network=network,
                block=ini_hype["block"],
                token=ini_hype["pool"]["token0"]["address"],
            ),
            "end": get_price_from_db(
                network=network,
                block=end_hype["block"],
                token=end_hype["pool"]["token0"]["address"],
            ),
        },
        "token1": {
            "ini": get_price_from_db(
                network=network,
                block=ini_hype["block"],
                token=ini_hype["pool"]["token1"]["address"],
            ),
            "end": get_price_from_db(
                network=network,
                block=end_hype["block"],
                token=end_hype["pool"]["token1"]["address"],
            ),
        },
    }

    # calculate the fees uncollected on this period
    fees_uncollected_token0 = int(end_hype["fees_uncollected"]["qtty_token0"]) - int(
        ini_hype["fees_uncollected"]["qtty_token0"]
    )
    fees_uncollected_token1 = int(end_hype["fees_uncollected"]["qtty_token1"]) - int(
        ini_hype["fees_uncollected"]["qtty_token1"]
    )
    fees_uncollected_usd = (
        fees_uncollected_token0 * token_prices["token0"]["end"]
        + fees_uncollected_token1 * token_prices["token1"]["end"]
    )

    # value staked at end of period

    return {
        "period_block": end_hype["block"] - ini_hype["block"],
        "period_timestamp": end_hype["timestamp"] - ini_hype["timestamp"],
        "period_days": (end_hype["timestamp"] - ini_hype["timestamp"]) / (24 * 60 * 60),
    }
