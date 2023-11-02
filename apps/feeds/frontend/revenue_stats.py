#################################################################
# FRONTEND DATABASE ITEM:  REVENUE STATS
#  FRONTEND DB ITEM
#  { "id":	"revenue_stats",
#     "creation_date": 1698232837, # timestamp of object creation
#     "volume": {
#         "total": 60.0, # total estimated Gamma volume
#         "items":[
#             {
#                 "chain": 1, # chain id
#                 "timestamp": 1696118400, # like (1696118400) for October 2023
#                 "total": 10.0, # total estimated Gamma volume for this month/period
#                 "exchange": "Uniswap", # exchange fantasy name
#                 "details":{} # reserved for pop up info
#             },
#             {
#                 "chain": xxxx,
#                 "timestamp": 1696118400,
#                 "total": 20.0,
#                 "exchange": "Sushi",
#                 "details":{}
#             },
#             {
#                 "chain": 1,
#                 "timestamp": 1693526400,
#                 "total": 30.0,
#                 "exchange": "Uniswap",
#                 "details":{}
#             },
#         ],
#      },
#     "fees":{}, # same as volume
#     "revenue":{}, # same as volume }
#################################################################


import logging
from apps.feeds.frontend.static_config import DEX_NAMES
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_from_localdb,
    get_default_globaldb,
    get_default_localdb,
)
from bins.formulas.fees import convert_feeProtocol
from bins.general.enums import Chain, Protocol, text_to_protocol


def build_revenue_stats(chain: Chain, ini_timestamp: int, end_timestamp: int) -> dict:
    """Build revenue stats for a chain in a period

    Args:
        chain (Chain): chain to build revenue stats
        ini_timestamp (int): initial timestamp
        end_timestamp (int): end timestamp

    Returns:
        dict: revenue stats
    """

    # define result vars
    total = 0.0
    items = []

    # get revenue stats
    revenue = create_revenue(
        chain=chain, ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
    )
    # get lpFees
    lpFees = create_lpFees(
        chain=chain, ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
    )
    # get volume
    volume = create_volume(lpFees=lpFees)

    # build result
    result = {
        "id": "revenue_stats",
        "creation_date": end_timestamp,
        "volume": volume,
        "fees": lpFees,
        "revenue": revenue,
    }

    return result


def create_revenue(chain: Chain, ini_timestamp: int, end_timestamp: int) -> dict:
    # Gamma fees recieved from LPs ( in wallets using revenue_operations collection)

    # define result vars
    total = 0.0
    items = []

    # build query
    revenue_query = [
        {
            "$match": {
                "dex": {"$exists": True},
                "timestamp": {"$gte": ini_timestamp, "$lte": end_timestamp},
            }
        },
        {
            "$group": {
                "_id": "$dex",
                "total_usd": {"$sum": "$usd_value"},
            }
        },
    ]
    # get revenue operations
    if revenue_summary := get_from_localdb(
        network=chain.database_name,
        collection="revenue_operations",
        aggregate=revenue_query,
    ):
        for revenue in revenue_summary:
            # convert revenue to float
            exchange = DEX_NAMES.get(text_to_protocol(revenue["_id"]), revenue["_id"])
            # build result
            items.append(
                {
                    "chain": chain.id,
                    "timestamp": end_timestamp,
                    "total": revenue["total_usd"],
                    "exchange": exchange,
                    "details": {},
                }
            )

    else:
        logging.getLogger(__name__).debug(
            f" no revenue operations found for {chain} from {ini_timestamp} to {end_timestamp}"
        )

    # return result
    return {"total": total, "items": items}


def create_lpFees(chain: Chain, ini_timestamp: int, end_timestamp: int) -> dict:
    # gross fees ( LP fees using operations collection)

    # define result vars
    total = 0.0
    items = []

    # get hypervisors prices at the end of the period
    token_prices = {}
    try:
        end_block = get_default_globaldb().get_closest_block(
            network=chain.database_name, timestamp=end_timestamp
        )
        query = [
            {
                "$match": {
                    "network": chain.database_name,
                    "block": {"$lte": end_block, "$gte": end_block - 10000},
                }
            },
            {"$sort": {"block": -1}},
            {"$group": {"_id": "$address", "last": {"$first": "$$ROOT"}}},
        ]
        token_prices = {
            x["last"]["address"]: x
            for x in get_default_globaldb().get_items_from_database(
                collection_name="usd_prices", aggregate=query
            )
        }
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Cant get token prices for {chain} at {end_timestamp}. Using current prices instead.  Error: {e}"
        )
        token_prices = {
            x["address"]: x
            for x in get_default_globaldb().get_items_from_database(
                collection_name="current_usd_prices",
                find={"network": chain.database_name},
            )
        }

    # get all hypervisors last status from the database
    last_hypervisor_status = {}
    first_hypervisor_status = {}
    try:
        query_last_hype_status = [
            {
                "$and": [
                    {"timestamp": {"$gte": ini_timestamp}},
                    {"timestamp": {"$lte": end_timestamp}},
                ]
            },
            {"$sort": {"block": -1}},
            {
                "$group": {
                    "_id": "$address",
                    "last": {"$first": "$$ROOT"},
                    "first": {"$last": "$$ROOT"},
                }
            },
        ]

        # last known hype status for each hypervisor at the end of the period

        for status in get_from_localdb(
            network=chain.database_name,
            collection="status",
            aggregate=query_last_hype_status,
        ):
            last_hypervisor_status[status["_id"]] = status["last"]
            first_hypervisor_status[status["_id"]] = status["first"]
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Cant get hypervisors status for {chain} at {end_timestamp}. Error: {e}"
        )

    if not last_hypervisor_status:
        logging.getLogger(__name__).debug(
            f" no hypervisors found for {chain} from {ini_timestamp} to {end_timestamp}"
        )
        return {"total": 0.0, "items": []}

    # get a sumarized data portion for all hypervisors in the database for a period
    for hype_summary in get_from_localdb(
        network=chain.database_name,
        collection="operations",
        aggregate=database_local.query_operations_summary(
            hypervisor_addresses=list(last_hypervisor_status.keys()),
            timestamp_ini=ini_timestamp,
            timestamp_end=end_timestamp,
        ),
    ):
        # convert hype to float
        hype_summary = database_local.convert_decimal_to_float(
            item=database_local.convert_d128_to_decimal(item=hype_summary)
        )

        # ease hypervisor status data access
        hype_status = last_hypervisor_status.get(hype_summary["address"], {})
        hype_status_ini = first_hypervisor_status.get(hype_summary["address"], {})
        if not hype_status:
            logging.getLogger(__name__).error(
                f"Last hype status data not found for {chain.fantasy_name}'s hypervisor {hype_summary['address']}"
            )
            continue
        # ease hypervisor price access
        token0_price = token_prices.get(
            hype_status["pool"]["token0"]["address"], {}
        ).get("price", 0)
        token1_price = token_prices.get(
            hype_status["pool"]["token1"]["address"], {}
        ).get("price", 0)
        if not token0_price or not token1_price:
            logging.getLogger(__name__).error(
                f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
            )
            continue

        # check for price outliers
        if token0_price > 1000000 or token1_price > 1000000:
            logging.getLogger(__name__).error(
                f" Price outlier detected for hypervisor {hype_summary['address']}: token0[{token0_price}] token1[{token1_price}]"
            )

        # calculate protocol fees
        if "globalState" in hype_status["pool"]:
            protocol_fee_0_raw = hype_status["pool"]["globalState"][
                "communityFeeToken0"
            ]
            protocol_fee_1_raw = hype_status["pool"]["globalState"][
                "communityFeeToken1"
            ]
        else:
            # convert from 8 decimals
            protocol_fee_0_raw = hype_status["pool"]["slot0"]["feeProtocol"] % 16
            protocol_fee_1_raw = hype_status["pool"]["slot0"]["feeProtocol"] >> 4

        # convert to percent (0-100)
        protocol_fee_0, protocol_fee_1 = convert_feeProtocol(
            feeProtocol0=protocol_fee_0_raw,
            feeProtocol1=protocol_fee_1_raw,
            hypervisor_protocol=hype_status["dex"],
            pool_protocol=hype_status["pool"]["dex"],
        )

        # get pool fee tier
        pool_fee_tier = calculate_pool_fees(hype_status)

        # get gamma liquidity percentage
        gamma_liquidity_ini, gamma_liquidity_end = calculate_total_liquidity(
            hype_status_ini, hype_status
        )

        # calculate collected fees
        collectedFees_0 = (
            hype_summary["collectedFees_token0"] + hype_summary["zeroBurnFees_token0"]
        )
        collectedFees_1 = (
            hype_summary["collectedFees_token1"] + hype_summary["zeroBurnFees_token1"]
        )
        collectedFees_usd = (
            collectedFees_0 * token0_price + collectedFees_1 * token1_price
        )

        # uncollected fees at the last known database status
        try:
            uncollected_0 = float(hype_status["fees_uncollected"]["qtty_token0"]) / (
                10 ** hype_status["pool"]["token0"]["decimals"]
            )
            uncollected_1 = float(hype_status["fees_uncollected"]["qtty_token1"]) / (
                10 ** hype_status["pool"]["token1"]["decimals"]
            )
        except:
            uncollected_0 = 0
            uncollected_1 = 0

        if protocol_fee_0 > 100 or protocol_fee_1 > 100:
            logging.getLogger(__name__).warning(
                f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
            )

        # calculate gross fees
        if protocol_fee_0 < 100:
            grossFees_0 = collectedFees_0 / (1 - (protocol_fee_0 / 100))
        else:
            grossFees_0 = collectedFees_0

        if protocol_fee_1 < 100:
            grossFees_1 = collectedFees_1 / (1 - (protocol_fee_1 / 100))
        else:
            grossFees_1 = collectedFees_1

        grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

        # days period
        days_period = (
            hype_summary["timestamp_end"] - hype_summary["timestamp_ini"]
        ) / 86400

        # convert revenue to float
        exchange = DEX_NAMES.get(
            text_to_protocol(hype_status["dex"]), hype_status["dex"]
        )

        # build output
        items.append(
            {
                "chain": chain.id,
                "timestamp": end_timestamp,
                "total": grossFees_usd,
                "exchange": exchange,
                "details": {
                    "collectedFees_0": collectedFees_0,
                    "collectedFees_1": collectedFees_1,
                    "collectedFees_usd": collectedFees_usd,
                    "uncollectedFees_0": uncollected_0,
                    "uncollectedFees_1": uncollected_1,
                    "grossFees_0": grossFees_0,
                    "grossFees_1": grossFees_1,
                    "grossFees_usd": grossFees_usd,
                    "gamma_vs_pool_liquidity_ini": gamma_liquidity_ini,
                    "gamma_vs_pool_liquidity_end": gamma_liquidity_end,
                    "feeTier": pool_fee_tier,
                    "eVolume": grossFees_usd / pool_fee_tier,
                    "collecedFees_day": collectedFees_usd / days_period
                    if days_period
                    else 0,
                },
            }
        )

        total += grossFees_usd

    # return result
    return {"total": total, "items": items}


def create_volume(lpFees: dict) -> dict:
    # calculated from gross fees

    # define result vars
    total = 0.0
    items = []

    for itm in lpFees["items"]:
        items.append(
            {
                "chain": itm["chain"],
                "timestamp": itm["timestamp"],
                "total": itm["details"]["eVolume"],
                "exchange": itm["exchange"],
                "details": {},
            }
        )
        total += itm["details"]["eVolume"]

    # return result
    return {"total": total, "items": items}


## HELPER


def calculate_pool_fees(hypervisor_status: dict) -> float:
    """Calculate the fee charged by the pool on every swap

    Args:
        hypervisor_status (dict): hypervisor status

    Returns:
        float: percentage of fees the pool is charging
    """
    protocol = text_to_protocol(hypervisor_status["pool"]["dex"])
    fee_tier = 0

    if protocol == Protocol.CAMELOT:
        try:
            # Camelot:  (pool.globalState().feeZto + pool.globalState().feeOtz)/2
            fee_tier = (
                int(hypervisor_status["pool"]["globalState"]["feeZto"])
                + int(hypervisor_status["pool"]["globalState"]["feeOtz"])
            ) / 2
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")
    elif protocol == Protocol.RAMSES:
        # Ramses:  pool.currentFee()
        try:
            # 'currentFee' in here is actualy the 'fee' field
            fee_tier = int(hypervisor_status["pool"]["fee"])
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")

    elif protocol == Protocol.QUICKSWAP:
        # QuickSwap + StellaSwap (Algebra V1):  pool.globalState().fee
        try:
            fee_tier = int(hypervisor_status["pool"]["globalState"]["fee"])
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")
    else:
        # Uniswap: pool.fee()
        try:
            fee_tier = int(hypervisor_status["pool"]["fee"])
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")

    return fee_tier / 1000000


def calculate_total_liquidity(
    hypervisor_status_ini: dict, hypervisor_status_end: dict
) -> tuple[float, float]:
    """Percentage of liquidity gamma has in the pool

    Args:
        hypervisor_status_ini (dict):  initial hypervisor status
        hypervisor_status_end (dict):  end hypervisor status

    Returns:
        tuple[float,float]:  initial_percentage, end_percentage
    """

    liquidity_ini = calculate_inRange_liquidity(hypervisor_status_ini)
    liquidity_end = calculate_inRange_liquidity(hypervisor_status_end)

    initial_percentage = (
        liquidity_ini / int(hypervisor_status_ini["pool"]["liquidity"])
        if int(hypervisor_status_ini["pool"]["liquidity"])
        else 0
    )
    end_percentage = (
        liquidity_end / int(hypervisor_status_end["pool"]["liquidity"])
        if int(hypervisor_status_end["pool"]["liquidity"])
        else 0
    )

    if end_percentage > 1:
        logging.getLogger(__name__).warning(
            f" liquidity percentage > 1 on {hypervisor_status_end['dex']}  {hypervisor_status_end['address']} hype block {hypervisor_status_end['block']}"
        )
    if initial_percentage > 1:
        logging.getLogger(__name__).warning(
            f" liquidity percentage > 1 on {hypervisor_status_ini['dex']}  {hypervisor_status_ini['address']} hype block {hypervisor_status_ini['block']}"
        )
    return initial_percentage, end_percentage


def calculate_inRange_liquidity(hypervisor_status: dict) -> int:
    """Calculate the liquidity in range of a hypervisor

    Args:
        hypervisor_status (dict):  hypervisor status

    Returns:
        int: liquidity in range
    """

    current_tick = (
        int(hypervisor_status["pool"]["slot0"]["tick"])
        if "slot0" in hypervisor_status["pool"]
        else int(hypervisor_status["pool"]["globalState"]["tick"])
    )

    liquidity = 0
    # check what to add as liquidity ( inRange only )
    if (
        float(hypervisor_status["limitUpper"]) >= current_tick
        and float(hypervisor_status["limitLower"]) <= current_tick
    ):
        liquidity += int(hypervisor_status["limitPosition"]["liquidity"])
    if (
        float(hypervisor_status["baseUpper"]) >= current_tick
        and float(hypervisor_status["baseLower"]) <= current_tick
    ):
        liquidity += int(hypervisor_status["basePosition"]["liquidity"])

    return liquidity
