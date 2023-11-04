#################################################################
# FRONTEND DATABASE ITEM:  REVENUE STATS
#  FRONTEND DB ITEM

#   {
#     "id":	"<frontendType>_<timestamp>_<protocol>_<chain>",
#     "chain_id": 1, # chain id
#     "chain": "Ethereum", # chain name
#     "protocol": "Uniswap", # protocol name
#     "timestamp": 1696118400, # use the last timestamp second of the period
#     "total_revenue": 10.0, # total for this month/period
#     "total_fees": 10.0, # total for this month/period
#     "total_volume": 10.0, # total for this month/period
#     "exchange": "Uniswap", # exchange fantasy name
#     "details":{} # reserved for pop up info
#   },


from datetime import datetime, timezone
import logging

import tqdm
from apps.feeds.frontend.static_config import DEX_NAMES
from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_frontend_revenue_stats
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_from_localdb,
    get_default_globaldb,
    get_default_localdb,
)
from bins.errors.actions import process_error
from bins.errors.general import ProcessingError
from bins.formulas.fees import convert_feeProtocol
from bins.general.enums import (
    Chain,
    Protocol,
    error_identity,
    frontendType,
    text_to_chain,
    text_to_protocol,
)
from bins.general.general_utilities import get_last_timestamp
from dateutil.relativedelta import relativedelta


def feed_revenue_stats(
    chains: list[Chain] | None = None,
    rewrite: bool = False,
) -> None:
    # define chains to process
    networks = (
        CONFIGURATION["_custom_"]["cml_parameters"].networks
        or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
    )
    chains = chains or [x for x in Chain if x.database_name in networks]

    # feed database revenue stats, monthly.
    for chain in chains:
        with tqdm.tqdm(total=len(chains)) as progress_bar:
            try:
                # build revenue stats
                # process all from firts timestamp to now
                for (
                    ini_timestamp,
                    end_timestamp,
                ) in get_all_next_timestamps_revenue_stats(
                    chain=chain, rewrite=rewrite
                ):
                    # update progress bar description
                    progress_bar.set_description(
                        f" {chain.database_name}: {datetime.fromtimestamp(end_timestamp, timezone.utc).strftime('%Y-%m')}"
                    )
                    progress_bar.update(0)

                    # define result vars
                    chain_protocol_time_result = {}
                    # build revenue stats
                    _revenue_items = create_revenue(
                        chain=chain,
                        ini_timestamp=ini_timestamp,
                        end_timestamp=end_timestamp,
                    )
                    # build lpFees
                    _lpFees_items = create_lpFees(
                        chain=chain,
                        ini_timestamp=ini_timestamp,
                        end_timestamp=end_timestamp,
                    )
                    # build volume
                    _volume_items = create_volume(lpFees=_lpFees_items)

                    for revenue in _revenue_items:
                        if not revenue["protocol"] in chain_protocol_time_result:
                            chain_protocol_time_result[revenue["protocol"]] = {
                                "id": create_id_frontend_revenue_stats(
                                    chain=chain,
                                    timestamp=end_timestamp,
                                    protocol=text_to_protocol(revenue["protocol"]),
                                ),
                                "frontend_type": frontendType.REVENUE_STATS,
                                "chain": chain.database_name,
                                "protocol": revenue["protocol"],
                                "chain_id": chain.id,
                                "timestamp": end_timestamp,
                                "total_revenue": revenue["total"],
                                "total_fees": 0.0,
                                "total_volume": 0.0,
                                "exchange": revenue["exchange"],
                                "details": {
                                    "collectedFees_0": 0,
                                    "collectedFees_1": 0,
                                    "collectedFees_usd": 0,
                                    "uncollectedFees_0": 0,
                                    "uncollectedFees_1": 0,
                                    "grossFees_0": 0,
                                    "grossFees_1": 0,
                                    "grossFees_usd": 0,
                                    "gamma_vs_pool_liquidity_ini": 0,
                                    "gamma_vs_pool_liquidity_end": 0,
                                    # "feeTier": lpFees["details"]["feeTier"],
                                    "eVolume": 0,
                                    "collecedFees_day": 0,
                                },
                            }
                        else:
                            chain_protocol_time_result[revenue["protocol"]][
                                "total_revenue"
                            ] += revenue["total"]

                    for lpFees in _lpFees_items:
                        if not lpFees["protocol"] in chain_protocol_time_result:
                            # no revenue but lpfees exist... this should not happen
                            # scrape revenue_operations from ini_timestamp to end_timestamp to make sure we are not missing anything
                            raise ProcessingError(
                                chain=chain,
                                item={
                                    "ini_timestamp": ini_timestamp,
                                    "end_timestamp": end_timestamp,
                                },
                                identity=error_identity.LPFEES_WITHOUT_REVENUE,
                                action="rescrape",
                                message=f" LPFees without revenue for {chain.database_name} from {ini_timestamp} to {end_timestamp}",
                            )
                        else:
                            chain_protocol_time_result[lpFees["protocol"]][
                                "total_fees"
                            ] += lpFees["total"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "collectedFees_0"
                            ] += lpFees["details"]["collectedFees_0"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "collectedFees_1"
                            ] += lpFees["details"]["collectedFees_1"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "collectedFees_usd"
                            ] += lpFees["details"]["collectedFees_usd"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "uncollectedFees_0"
                            ] += lpFees["details"]["uncollectedFees_0"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "uncollectedFees_1"
                            ] += lpFees["details"]["uncollectedFees_1"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "grossFees_0"
                            ] += lpFees["details"]["grossFees_0"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "grossFees_1"
                            ] += lpFees["details"]["grossFees_1"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "grossFees_usd"
                            ] += lpFees["details"]["grossFees_usd"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "gamma_vs_pool_liquidity_ini"
                            ] += lpFees["details"]["gamma_vs_pool_liquidity_ini"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "gamma_vs_pool_liquidity_end"
                            ] += lpFees["details"]["gamma_vs_pool_liquidity_end"]
                            # items_aggregated[itm["exchange"]]["details"]["feeTier"] += itm["details"]["feeTier"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "eVolume"
                            ] += lpFees["details"]["eVolume"]
                            chain_protocol_time_result[lpFees["protocol"]]["details"][
                                "collecedFees_day"
                            ] += lpFees["details"]["collecedFees_day"]

                    for volume in _volume_items:
                        if not volume["protocol"] in chain_protocol_time_result:
                            # no revenue but lpfees exist... this should not happen
                            raise ValueError(
                                f" {volume['protocol']} not found in chain_protocol_time_result"
                            )
                        else:
                            chain_protocol_time_result[volume["protocol"]][
                                "total_volume"
                            ] += volume["total"]

                    # save to database
                    if chain_protocol_time_result:
                        if db_return := get_default_globaldb().replace_items_to_database(
                            data=list(chain_protocol_time_result.values()),
                            collection_name="frontend",
                        ):
                            logging.getLogger(__name__).debug(
                                f" {chain.database_name} -> del:{db_return.deleted_count} ins:{db_return.inserted_count} mod:{db_return.modified_count} ups:{db_return.upserted_count} "
                            )
                        else:
                            logging.getLogger(__name__).error(
                                f" {chain.database_name} -> Error saving revenue stats to database"
                            )

            except ProcessingError as e:
                # process error
                process_error(error=e)

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error feeding frontend revenue stats for {chain.database_name}  err->   {e}"
                )

            # progress
            progress_bar.update(1)


def create_revenue(chain: Chain, ini_timestamp: int, end_timestamp: int) -> dict:
    # Gamma fees recieved from LPs ( in wallets using revenue_operations collection)

    # define result vars
    result = []

    # build query
    revenue_query = [
        {
            "$match": {
                "$and": [
                    {"dex": {"$exists": True}},
                    {"timestamp": {"$gte": ini_timestamp}},
                    {"timestamp": {"$lte": end_timestamp}},
                ]
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
            protocol = text_to_protocol(revenue["_id"])
            exchange = DEX_NAMES.get(protocol, revenue["_id"])
            # build result
            result.append(
                {
                    "id": create_id_frontend_revenue_stats(
                        chain=chain,
                        timestamp=end_timestamp,
                        protocol=protocol,
                    ),
                    "chain": chain.database_name,
                    "protocol": protocol.database_name,
                    "chain_id": chain.id,
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
    return result


def create_lpFees(chain: Chain, ini_timestamp: int, end_timestamp: int) -> dict:
    # gross fees ( LP fees using operations collection)

    # define result vars
    result = []

    # get hypervisors prices at the end of the period
    token_prices = {}
    token_current_prices = {
        x["address"]: x
        for x in get_default_globaldb().get_items_from_database(
            collection_name="current_usd_prices",
            find={"network": chain.database_name},
        )
    }
    try:
        # get a list of blocks close to the end of the period
        end_block = get_default_globaldb().get_closest_block(
            network=chain.database_name, timestamp=end_timestamp
        )
        # assign the first block of the list
        end_block = end_block[0]["doc"]["block"]
        query = [
            {
                "$match": {
                    "$and": [
                        {"network": chain.database_name},
                        {"block": {"$gte": end_block - 10000}},
                        {"block": {"$lte": end_block}},
                    ]
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
        token_prices = token_current_prices

    # get all hypervisors last status from the database
    last_hypervisor_status = {}
    first_hypervisor_status = {}
    try:
        query_last_hype_status = [
            {
                "$match": {
                    "$and": [
                        {"timestamp": {"$gte": ini_timestamp}},
                        {"timestamp": {"$lte": end_timestamp}},
                    ]
                }
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
                f" Database price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}. Using current prices"
            )
            # get price from current prices
            token0_price = token_current_prices.get(
                hype_status["pool"]["token0"]["address"], {}
            ).get("price", 0)
            token1_price = token_current_prices.get(
                hype_status["pool"]["token1"]["address"], {}
            ).get("price", 0)
            if not token0_price or not token1_price:
                logging.getLogger(__name__).error(
                    f" Current price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}. Cant continue."
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
        try:
            gamma_liquidity_ini, gamma_liquidity_end = calculate_total_liquidity(
                hype_status_ini, hype_status
            )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Cant calculate total liquidity for hypervisor {hype_summary['address']}. Error: {e}"
            )
            gamma_liquidity_ini = gamma_liquidity_end = 0

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
        protocol = text_to_protocol(hype_status["dex"])
        exchange = DEX_NAMES.get(protocol, hype_status["dex"])
        # build output
        result.append(
            {
                "id": create_id_frontend_revenue_stats(
                    chain=chain, timestamp=end_timestamp, protocol=protocol
                ),
                "chain": chain.database_name,
                "protocol": protocol.database_name,
                "chain_id": chain.id,
                "timestamp": int(end_timestamp),
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

    # aggregate items by exchange
    items_aggregated = {}
    for itm in result:
        if itm["exchange"] not in items_aggregated:
            items_aggregated[itm["exchange"]] = {
                "id": itm["id"],
                "chain": itm["chain"],
                "protocol": itm["protocol"],
                "chain_id": itm["chain_id"],
                "timestamp": itm["timestamp"],
                "total": itm["total"],
                "exchange": itm["exchange"],
                "details": {
                    "collectedFees_0": itm["details"]["collectedFees_0"],
                    "collectedFees_1": itm["details"]["collectedFees_1"],
                    "collectedFees_usd": itm["details"]["collectedFees_usd"],
                    "uncollectedFees_0": itm["details"]["uncollectedFees_0"],
                    "uncollectedFees_1": itm["details"]["uncollectedFees_1"],
                    "grossFees_0": itm["details"]["grossFees_0"],
                    "grossFees_1": itm["details"]["grossFees_1"],
                    "grossFees_usd": itm["details"]["grossFees_usd"],
                    "gamma_vs_pool_liquidity_ini": itm["details"][
                        "gamma_vs_pool_liquidity_ini"
                    ],
                    "gamma_vs_pool_liquidity_end": itm["details"][
                        "gamma_vs_pool_liquidity_end"
                    ],
                    # "feeTier": itm["details"]["feeTier"],
                    "eVolume": itm["details"]["eVolume"],
                    "collecedFees_day": itm["details"]["collecedFees_day"],
                },
            }
        else:
            items_aggregated[itm["exchange"]]["total"] += itm["total"]
            items_aggregated[itm["exchange"]]["details"]["collectedFees_0"] += itm[
                "details"
            ]["collectedFees_0"]
            items_aggregated[itm["exchange"]]["details"]["collectedFees_1"] += itm[
                "details"
            ]["collectedFees_1"]
            items_aggregated[itm["exchange"]]["details"]["collectedFees_usd"] += itm[
                "details"
            ]["collectedFees_usd"]
            items_aggregated[itm["exchange"]]["details"]["uncollectedFees_0"] += itm[
                "details"
            ]["uncollectedFees_0"]
            items_aggregated[itm["exchange"]]["details"]["uncollectedFees_1"] += itm[
                "details"
            ]["uncollectedFees_1"]
            items_aggregated[itm["exchange"]]["details"]["grossFees_0"] += itm[
                "details"
            ]["grossFees_0"]
            items_aggregated[itm["exchange"]]["details"]["grossFees_1"] += itm[
                "details"
            ]["grossFees_1"]
            items_aggregated[itm["exchange"]]["details"]["grossFees_usd"] += itm[
                "details"
            ]["grossFees_usd"]
            items_aggregated[itm["exchange"]]["details"][
                "gamma_vs_pool_liquidity_ini"
            ] += itm["details"]["gamma_vs_pool_liquidity_ini"]
            items_aggregated[itm["exchange"]]["details"][
                "gamma_vs_pool_liquidity_end"
            ] += itm["details"]["gamma_vs_pool_liquidity_end"]
            # items_aggregated[itm["exchange"]]["details"]["feeTier"] += itm["details"]["feeTier"]
            items_aggregated[itm["exchange"]]["details"]["eVolume"] += itm["details"][
                "eVolume"
            ]
            items_aggregated[itm["exchange"]]["details"]["collecedFees_day"] += itm[
                "details"
            ]["collecedFees_day"]

    # convert aggregated items to list and return result
    return list(items_aggregated.values())


def create_volume(lpFees: dict) -> dict:
    # calculated from gross fees

    # define result vars
    result = []

    for itm in lpFees:
        protocol = text_to_protocol(itm["protocol"])
        chain = text_to_chain(itm["chain"])
        result.append(
            {
                "id": create_id_frontend_revenue_stats(
                    chain=chain, timestamp=itm["timestamp"], protocol=protocol
                ),
                "chain_id": itm["chain_id"],
                "chain": itm["chain"],
                "protocol": itm["protocol"],
                "timestamp": itm["timestamp"],
                "total": itm["details"]["eVolume"],
                "exchange": itm["exchange"],
                "details": {},
            }
        )
    # return result
    return result


## HELPER
def get_next_timestamps_revenue_stats(
    chain: Chain, rewrite: bool = False
) -> tuple[int, int]:
    if not rewrite:
        # define last timestamp for this type
        last_item = get_default_globaldb().get_items_from_database(
            collection_name="frontend",
            find={"chain": chain.database_name},
            sort=[("timestamp", -1)],
            limit=1,
        )
    if rewrite or not last_item:
        try:
            first_revenue_operation = get_from_localdb(
                network=chain.database_name,
                collection="revenue_operations",
                find={},
                sort=[("timestamp", 1)],
                limit=1,
            )
            _datetime = datetime.fromtimestamp(
                first_revenue_operation[0]["timestamp"], timezone.utc
            )
            end_timestamp = int(
                get_last_timestamp(year=_datetime.year, month=_datetime.month)
            )
            ini_timestamp = int(
                datetime(year=_datetime.year, month=_datetime.month, day=1).timestamp()
            )
        except Exception as e:
            # seems there is no revenue operations in the database
            raise ValueError(
                f" Cant find revenue operations for {chain}   error->  {e}"
            )
    else:
        try:
            _last_datetime = datetime.fromtimestamp(
                last_item[0]["timestamp"], timezone.utc
            )
            _next_datetime = _last_datetime + relativedelta(days=10)
            end_timestamp = int(
                get_last_timestamp(year=_next_datetime.year, month=_next_datetime.month)
            )
            ini_timestamp = int(
                datetime(
                    year=_next_datetime.year, month=_next_datetime.month, day=1
                ).timestamp()
            )
        except Exception as e:
            raise ValueError(
                f" Cant calculate ini end timestamp for {chain}   error->  {e}"
            )

    # return result
    return ini_timestamp, end_timestamp


def get_all_next_timestamps_revenue_stats(
    chain: Chain, rewrite: bool = False
) -> list[tuple[int, int]]:
    """Return a list of ini_timestamp, end_timestamp pairs for all periods left to be scraped in the database

    Args:
        chain (Chain): chain
        rewrite (bool, optional): reset. Defaults to False.

    Returns:
        list[tuple[int, int]]: ordered list of ini_timestamp, end_timestamp pairs
    """
    result = []
    # define the first timestamp
    result.append(get_next_timestamps_revenue_stats(chain=chain, rewrite=rewrite))
    # define the last timestamp
    final_datetime = datetime(
        year=datetime.now(timezone.utc).year,
        month=datetime.now(timezone.utc).month,
        day=1,
    ).timestamp()
    _start_time = datetime.now(timezone.utc).timestamp()

    while result[-1][0] != final_datetime:
        # convert last item in result to datetime
        last_datetime = datetime.fromtimestamp(result[-1][1], timezone.utc)
        # add 10 days to last item in result ( making sure we are in the next month, from last timestamp)
        last_datetime = last_datetime + relativedelta(days=10)
        # add to result
        end_timestamp = int(
            get_last_timestamp(year=last_datetime.year, month=last_datetime.month)
        )
        ini_timestamp = int(
            datetime(
                year=last_datetime.year, month=last_datetime.month, day=1
            ).timestamp()
        )

        result.append((ini_timestamp, end_timestamp))

        # avoid infinite loop, 5 seconds max
        if (datetime.now(timezone.utc).timestamp() - _start_time) > 5:
            logging.getLogger(__name__).error(
                f" Cant find next timestamp for {chain} after {result[-1][1]}"
            )
            break

    return result


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
