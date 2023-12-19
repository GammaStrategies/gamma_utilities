# Average TVL:
#           get all hypervisor status from db timestamp day utc 00:00:00 to 23:59:59
#           loop through all hypervisor status getting:
#
#               liquidity in range --> average daily liquidity in range % = average(liquidity in range / total liquidity)
#               liquidity out range --> average daily liquidity out range % = average(liquidity out range / total liquidity)
#               total liquidity
#
#               underlying_token0 --> tvl + uncollected fees * price  --> average daily tvl
#               underlying_token1
#               limitPosition_underlying_token0
#               limitPosition_underlying_token1
#               BasePosition_underlying_token0
#               BasePosition_underlying_token1
#

# transactions:
#           get all operations in withdraw,deposit from db timestamp day utc 00:00:00 to 23:59:59
#           loop through all operations getting:
#               deposits in usd
#               withdrawals in usd
#               rebalances/zeroBurn qtty
#

# collected fees:
#


# volume:


# Unique user addresses interactions:

# Total Value Locked (TVL)

# Volume Through Vaults (VTV)

# Capital Efficiency (CE) = Volume / TVL

# Gross Fee Production (Fees) = (Volume * Fee Tier) / Time

# Revenue Production (Revenue) = (Fees * Revenue Modifier) / Time

# Annual Percentage Rate (APR)= (Fees / Time) * Yearly Modifier / TVL * 100%

# Time In Range (TIR)= Sum of Time In Range / Operating Time * 100%

# Rebalance Frequency (RF) #ReBal = number of Rebalances / Time

# Historical Volatility (HV or σ)= Standard Deviation of Asset Price / Root(Time)

from datetime import datetime, timezone
import logging
import statistics

import tqdm
from bins.database.common.db_collections_common import database_local
from bins.database.common.objects.hypervisor import (
    hypervisor_status_object,
    time_object,
    timeframe_object,
    transformer_hypervisor_status,
)
from bins.database.common.objects.reports import report_object
from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
    get_price_from_db_extended,
    get_prices_from_db_usingHypervisorStatus,
)
from bins.general.enums import Chain, reportType, text_to_protocol
from bins.general.general_utilities import get_last_timestamp_of_day
from bins.w3.builders import (
    build_db_hypervisor,
    build_erc20_helper,
    build_db_hypervisor_multicall,
)


def build_kpis_report():
    # per hypervisor report

    # average tvl -> (status collcection)+(prices collection)
    # average hypervisor share price -> (status collection)+(prices collection)
    # average liquidity in range % -> (status collection)

    # gross fees -> (status collection)+(operations collection)+(prices collection)
    # lp fees -> (status collection)+(operations collection)+(prices collection)
    # Gamma revenue -> (status collection)+(operations collection)+(prices collection)

    # volume -> (operations collection)+ (status collection)+(prices collection)
    ################### cashed fees -> (revenue_operations collection)

    # transactions[withdraw,deposit,transfer,] -> (operations collection)+(prices collection)
    # rebalances -> (operations collection)    database_local.query_hypervisor_rebalances_qtty
    # unique users[unique non hype addresses transactions] -> (operations collection)  avoid 0x000 addresses

    # asset0_volatility -> (prices collection)
    # asset1_volatility -> (prices collection)
    # hypervisor_volatility using average hypervisor share price

    # lpvshold using shareprice(end)-shareprice(ini) / shareprice(ini)  vs tokens qtty(ini) at prices(ini)  - tokens qtty(ini) at prices(end) / tokens qtty(ini) at prices(ini)

    pass


def create_kpi_reports(
    chain: Chain, ini_timestamp: int | None = None, end_timestamp: int | None = None
) -> report_object:
    """Create kpi reports for all hypervisors found in db from ini_timestamp to end_timestamp.
        if ini but no end timestamp is provided, ini will be replaced with  utc 00:00:00 and end will be calculated as ini + 1 day-1 second
    Args:
        chain (Chain):
        ini_timestamp (int | None, optional):Initial timestamp. Defaults to today( utc ).
        end_timestamp (int | None, optional): End timestamp . Defaults to ini_timestamp day last timestamp.

    Returns:
        report_object: Report object with data:
                    {

                    }

    """
    # set timeframe
    ini_timestamp, end_timestamp, ini_block, end_block = create_timestampBlocks(
        chain=chain, ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
    )

    # create root structure
    main_structure = create_kpi_structure(
        chain=chain,
        ini_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        ini_block=ini_block,
        end_block=end_block,
    )

    # result
    result: list[report_object] = []

    for hypervisor_address, hypervisor_data in main_structure.items():
        status_list_length = len(hypervisor_data["status_list"])
        status_list_dataset_seconds = (
            (
                hypervisor_data["status_list"][-1]["timestamp"]
                - hypervisor_data["status_list"][0]["timestamp"]
            )
            if status_list_length
            else 0
        )
        _hypervisor_status_obj = None
        # process status list to get multiple values later used
        for idx in range(status_list_length):
            # define ease to access vars
            hypervisor_status_db = hypervisor_data["status_list"][idx]
            last_hypervisor_status_db = (
                hypervisor_data["status_list"][idx - 1] if idx > 0 else None
            )

            # convert hypervisor dict to object
            _hypervisor_status_obj = hypervisor_status_object(
                transformer=transformer_hypervisor_status, **hypervisor_status_db
            )

            # get closest prices when not found
            if (
                not _hypervisor_status_obj.block
                in hypervisor_data["pool"]["token0"]["prices"]
            ):
                if _tmpPrice := get_price_from_db_extended(
                    network=chain.database_name,
                    token_address=_hypervisor_status_obj.pool.token0.address,
                    block=_hypervisor_status_obj.block,
                    within_timeframe=60,
                ):
                    hypervisor_data["pool"]["token0"]["prices"][
                        _hypervisor_status_obj.block
                    ] = _tmpPrice

            if (
                not _hypervisor_status_obj.block
                in hypervisor_data["pool"]["token1"]["prices"]
            ):
                if _tmpPrice := get_price_from_db_extended(
                    network=chain.database_name,
                    token_address=_hypervisor_status_obj.pool.token1.address,
                    block=_hypervisor_status_obj.block,
                    within_timeframe=60,
                ):
                    hypervisor_data["pool"]["token1"]["prices"][
                        _hypervisor_status_obj.block
                    ] = _tmpPrice

            # ease price vars later access
            _token0_current_price = hypervisor_data["pool"]["token0"]["prices"][
                _hypervisor_status_obj.block
            ]
            _token1_current_price = hypervisor_data["pool"]["token1"]["prices"][
                _hypervisor_status_obj.block
            ]

            # save start and end prices
            if not "ini_price" in hypervisor_data["pool"]["token0"]:
                hypervisor_data["pool"]["token0"]["ini_price"] = _token0_current_price
            if not "ini_price" in hypervisor_data["pool"]["token1"]:
                hypervisor_data["pool"]["token1"]["ini_price"] = _token1_current_price
            hypervisor_data["pool"]["token0"]["end_price"] = _token0_current_price
            hypervisor_data["pool"]["token1"]["end_price"] = _token1_current_price

            # get underlying values ( TVL)
            _underlying_value = _hypervisor_status_obj.get_underlying_value(
                inDecimal=True, feeType="all"
            )
            _underlying_value_usd = (
                float(_underlying_value.token0) * _token0_current_price
                + float(_underlying_value.token1) * _token1_current_price
            )

            # liquidity in range %
            _liquidity_in_range = _hypervisor_status_obj.get_inRange_liquidity()
            _liquidity_base = _hypervisor_status_obj.basePosition.liquidity
            _liquidity_total = _hypervisor_status_obj.get_total_liquidity()
            # define seconds_in_range
            _seconds_in_range = (
                (
                    _hypervisor_status_obj.timestamp
                    - last_hypervisor_status_db["timestamp"]
                )
                if _liquidity_in_range and last_hypervisor_status_db
                else 0
            )

            _hypervisor_sharePrice = float(
                _hypervisor_status_obj.get_share_price(
                    token0_price=_token0_current_price,
                    token1_price=_token1_current_price,
                )
            )

            hypervisor_data["raw_kpi_data"]["underlying_values_usd"].append(
                _underlying_value_usd
            )
            hypervisor_data["raw_kpi_data"]["underlying_0"].append(
                float(_underlying_value.token0)
            )
            hypervisor_data["raw_kpi_data"]["underlying_1"].append(
                float(_underlying_value.token1)
            )
            hypervisor_data["raw_kpi_data"]["liquidity_inrange"].append(
                _liquidity_in_range
            )
            hypervisor_data["raw_kpi_data"]["liquidity_base"].append(_liquidity_base)
            hypervisor_data["raw_kpi_data"]["liquidity_total"].append(_liquidity_total)
            hypervisor_data["raw_kpi_data"]["seconds_inrange"].append(_seconds_in_range)
            hypervisor_data["raw_kpi_data"]["share_prices"].append(
                _hypervisor_sharePrice
            )

        # create data for report
        token0_ini_price = (
            hypervisor_data["pool"]["token0"]["ini_price"]
            if "ini_price" in hypervisor_data["pool"]["token0"]
            else 0
        )
        token1_ini_price = (
            hypervisor_data["pool"]["token1"]["ini_price"]
            if "ini_price" in hypervisor_data["pool"]["token1"]
            else 0
        )
        token0_end_price = (
            hypervisor_data["pool"]["token0"]["end_price"]
            if "end_price" in hypervisor_data["pool"]["token0"]
            else 0
        )
        token1_end_price = (
            hypervisor_data["pool"]["token1"]["end_price"]
            if "end_price" in hypervisor_data["pool"]["token1"]
            else 0
        )

        # get collected fees from operations summary
        _collected_fees0 = (
            (
                hypervisor_data["operations_summary"]["collectedFees_token0"]
                + hypervisor_data["operations_summary"]["zeroBurnFees_token0"]
            )
            if hypervisor_data["operations_summary"]
            else 0
        )
        _collected_fees1 = (
            (
                hypervisor_data["operations_summary"]["collectedFees_token1"]
                + hypervisor_data["operations_summary"]["zeroBurnFees_token1"]
            )
            if hypervisor_data["operations_summary"]
            else 0
        )
        _collected_fees_usd = (
            float(_collected_fees0) * token0_end_price
            + float(_collected_fees1) * token1_end_price
        )
        # calculate gross fees
        (
            _gross_fees_0,
            _gross_fees_1,
        ) = (
            _hypervisor_status_obj.calculate_gross_fees(
                collected_fees0=_collected_fees0, collected_fees1=_collected_fees1
            )
            if _hypervisor_status_obj
            else (0, 0)
        )
        _gross_fees_usd = (
            float(_gross_fees_0) * token0_end_price
            + float(_gross_fees_1) * token1_end_price
        )
        # calculate volume
        _volume_0, _volume_1 = (
            _hypervisor_status_obj.calculate_gross_volume(
                gross_fees0=_gross_fees_0, gross_fees1=_gross_fees_1
            )
            if _hypervisor_status_obj
            else (0, 0)
        )
        _volume_usd = (
            float(_volume_0) * token0_end_price + float(_volume_1) * token1_end_price
        )

        hold_pnl = (
            (
                (
                    (
                        token0_end_price
                        * hypervisor_data["raw_kpi_data"]["underlying_0"][0]
                        + token1_end_price
                        * hypervisor_data["raw_kpi_data"]["underlying_1"][0]
                    )
                    - (
                        token0_ini_price
                        * hypervisor_data["raw_kpi_data"]["underlying_0"][0]
                        + token1_ini_price
                        * hypervisor_data["raw_kpi_data"]["underlying_1"][0]
                    )
                )
                / (
                    token0_ini_price
                    * hypervisor_data["raw_kpi_data"]["underlying_0"][0]
                    + token1_ini_price
                    * hypervisor_data["raw_kpi_data"]["underlying_1"][0]
                )
            )
            if token0_ini_price
            and token1_ini_price
            and len(hypervisor_data["raw_kpi_data"]["underlying_0"])
            else 0
        )
        lp_pnl = (
            (
                (
                    hypervisor_data["raw_kpi_data"]["share_prices"][-1]
                    - hypervisor_data["raw_kpi_data"]["share_prices"][0]
                )
                / hypervisor_data["raw_kpi_data"]["share_prices"][0]
            )
            if hypervisor_data["raw_kpi_data"].get("share_prices", None)
            else 0
        )

        liquidity_inrange = sum(
            hypervisor_data["raw_kpi_data"].get("liquidity_inrange", [0])
        )
        total_liquidity = sum(
            hypervisor_data["raw_kpi_data"].get("liquidity_total", [0])
        )

        data_report = {
            # "seconds": end_timestamp - ini_timestamp,
            "dataset_seconds": (
                (
                    hypervisor_data["status_list"][-1]["timestamp"]
                    - hypervisor_data["status_list"][0]["timestamp"]
                )
                if status_list_length
                else 0
            ),
            "address": hypervisor_address,
            "symbol": hypervisor_data["symbol"],
            "mean_tvl": sum(hypervisor_data["raw_kpi_data"]["underlying_values_usd"])
            / len(hypervisor_data["raw_kpi_data"]["underlying_values_usd"])
            if len(hypervisor_data["raw_kpi_data"].get("underlying_values_usd", []))
            else 0,
            "liquidity_inrange": liquidity_inrange / total_liquidity
            if total_liquidity
            else 0,
            "seconds_in_range": sum(
                hypervisor_data["raw_kpi_data"].get("seconds_inrange", [0])
            ),
            "tokens": {
                "token0": {
                    "symbol": hypervisor_data["pool"]["token0"]["symbol"],
                    "address": hypervisor_data["pool"]["token0"]["address"],
                    "decimals": hypervisor_data["pool"]["token0"]["decimals"],
                    "price_std": statistics.stdev(
                        hypervisor_data["pool"]["token0"]["prices"].values()
                    )
                    if len(hypervisor_data["pool"]["token0"]["prices"]) > 1
                    else 0,
                    "price_ini": token0_ini_price,
                    "price_end": token0_end_price,
                },
                "token1": {
                    "symbol": hypervisor_data["pool"]["token1"]["symbol"],
                    "address": hypervisor_data["pool"]["token1"]["address"],
                    "decimals": hypervisor_data["pool"]["token1"]["decimals"],
                    "price_std": statistics.stdev(
                        hypervisor_data["pool"]["token1"]["prices"].values()
                    )
                    if len(hypervisor_data["pool"]["token1"]["prices"]) > 1
                    else 0,
                    "price_ini": token1_ini_price,
                    "price_end": token1_end_price,
                },
            },
            "share_price_std": statistics.stdev(
                hypervisor_data["raw_kpi_data"]["share_prices"]
            )
            if len(hypervisor_data["raw_kpi_data"].get("share_prices", [])) > 1
            else 0,
            "share_price_ini": hypervisor_data["raw_kpi_data"]["share_prices"][0]
            if len(hypervisor_data["raw_kpi_data"].get("share_prices", [])) > 1
            else 0,
            "share_price_end": hypervisor_data["raw_kpi_data"]["share_prices"][-1]
            if len(hypervisor_data["raw_kpi_data"].get("share_prices", [])) > 1
            else 0,
            "volume": _volume_usd,
            "gross_fees": _gross_fees_usd,
            "collected_fees": _collected_fees_usd,
            "lp_pnl": lp_pnl,
            "hold_pnl": hold_pnl,
            "lpvshold": ((lp_pnl + 1) / (hold_pnl + 1)) - 1,
            "rebalances": hypervisor_data["rebalances_qtty"],
        }
        # create report
        result.append(
            report_object(
                type=reportType.KPI,
                protocol=text_to_protocol(hypervisor_data["dex"]),
                timeframe=timeframe_object(
                    ini=time_object(block=ini_block, timestamp=ini_timestamp),
                    end=time_object(block=end_block, timestamp=end_timestamp),
                ),
                data=data_report,
            )
        )

    return result

    # set timeframe
    if not ini_timestamp and not end_timestamp:
        ini_timestamp, end_timestamp = build_day_timeframe()
    elif ini_timestamp and not end_timestamp:
        ini_timestamp, end_timestamp = build_day_timeframe(timestamp=ini_timestamp)
    elif not ini_timestamp and end_timestamp:
        raise Exception("Can't set end_timestamp without ini_timestamp")

    # calculate blocks from timestamps
    # erchelper = build_erc20_helper(chain=chain)
    ini_block = 0  # erchelper.blockNumberFromTimestamp(timestamp=ini_timestamp)
    end_block = 0  # erchelper.blockNumberFromTimestamp(timestamp=end_timestamp)

    # get hypervisor status from db from ini_timestamp to end_timestamp
    if hypervisor_status_list := get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={"timestamp": {"$gte": ini_timestamp, "$lte": end_timestamp}},
        sort=[("timestamp", 1)],
        batch_size=50000,
    ):
        # get prices from db
        prices = get_prices_from_db_usingHypervisorStatus(
            chain=chain, hypervisor_status_list=hypervisor_status_list
        )

        _hypervisor_addresses = [x["address"] for x in hypervisor_status_list]

        # get the operations summary of each of those hypes
        operations_sumary = {
            x["address"]: database_local.convert_d128_to_decimal(item=x)
            for x in get_from_localdb(
                network=chain.database_name,
                collection="operations",
                aggregate=database_local.query_operations_summary(
                    hypervisor_addresses=_hypervisor_addresses,
                    timestamp_ini=ini_timestamp,
                    timestamp_end=end_timestamp,
                ),
            )
        }
        # get the rebalances qtty of each of those hypes
        rebalances_qtty = {
            x["address"]: x["rebalances"]
            for x in get_from_localdb(
                network=chain.database_name,
                collection="operations",
                aggregate=database_local.query_hypervisor_rebalances_qtty(
                    hypervisor_addresses=_hypervisor_addresses,
                    timestamp_ini=ini_timestamp,
                    timestamp_end=end_timestamp,
                ),
            )
        }

        # store hype data
        hypervisor_results = {}

        for idx in range(len(hypervisor_status_list)):
            # define ease to access vars
            hypervisor_status_db = hypervisor_status_list[idx]
            last_hypervisor_status_db = (
                hypervisor_status_list[idx - 1] if idx > 0 else None
            )

            # convert hypervisor dict to object
            _hypervisor_status_obj = hypervisor_status_object(
                transformer=transformer_hypervisor_status, **hypervisor_status_db
            )

            # dataset_seconds
            _dataset_seconds = (
                0
                if not last_hypervisor_status_db
                else (
                    hypervisor_status_db["timestamp"]
                    - last_hypervisor_status_db["timestamp"]
                )
            )

            # get closest prices when not found
            if (
                not f"{_hypervisor_status_obj.block}_{_hypervisor_status_obj.pool.token0.address}"
                in prices
            ):
                if _tmpPrice := get_price_from_db_extended(
                    network=chain.database_name,
                    token_address=_hypervisor_status_obj.pool.token0.address,
                    block=_hypervisor_status_obj.block,
                    within_timeframe=60,
                ):
                    prices[
                        f"{_hypervisor_status_obj.block}_{_hypervisor_status_obj.pool.token0.address}"
                    ] = _tmpPrice

            if (
                not f"{_hypervisor_status_obj.block}_{_hypervisor_status_obj.pool.token1.address}"
                in prices
            ):
                if _tmpPrice := get_price_from_db_extended(
                    network=chain.database_name,
                    token_address=_hypervisor_status_obj.pool.token1.address,
                    block=_hypervisor_status_obj.block,
                    within_timeframe=60,
                ):
                    prices[
                        f"{_hypervisor_status_obj.block}_{_hypervisor_status_obj.pool.token1.address}"
                    ] = _tmpPrice

            # ease price vars later access
            _token0_price = prices[
                f"{_hypervisor_status_obj.block}_{_hypervisor_status_obj.pool.token0.address}"
            ]
            _token1_price = prices[
                f"{_hypervisor_status_obj.block}_{_hypervisor_status_obj.pool.token1.address}"
            ]

            # get underlying values ( TVL)
            _underlying_value = _hypervisor_status_obj.get_underlying_value(
                inDecimal=True, feeType="all"
            )
            _underlying_value_usd = (
                float(_underlying_value.token0) * _token0_price
                + float(_underlying_value.token1) * _token1_price
            )

            # liquidity in range %
            _liquidity_in_range = _hypervisor_status_obj.get_inRange_liquidity()
            _liquidity_base = _hypervisor_status_obj.basePosition.liquidity
            _liquidity_total = _hypervisor_status_obj.get_total_liquidity()
            _liquidity_seconds_in_range = _hypervisor_status_obj.timestamp - (
                last_hypervisor_status_db["timestamp"]
                if last_hypervisor_status_db
                else ini_timestamp
            )

            _hypervisor_sharePrice = float(
                _hypervisor_status_obj.get_share_price(
                    token0_price=_token0_price,
                    token1_price=_token1_price,
                )
            )

            if not _hypervisor_status_obj.address in hypervisor_results:
                # get collected fees from operations summary
                _collected_fees0 = (
                    operations_sumary[_hypervisor_status_obj.address][
                        "collectedFees_token0"
                    ]
                    + operations_sumary[_hypervisor_status_obj.address][
                        "zeroBurnFees_token0"
                    ]
                )
                _collected_fees1 = (
                    operations_sumary[_hypervisor_status_obj.address][
                        "collectedFees_token1"
                    ]
                    + operations_sumary[_hypervisor_status_obj.address][
                        "zeroBurnFees_token1"
                    ]
                )
                _collected_fees_usd = (
                    float(_collected_fees0) * _token0_price
                    + float(_collected_fees1) * _token1_price
                )
                # calculate gross fees
                (
                    _gross_fees_0,
                    _gross_fees_1,
                ) = _hypervisor_status_obj.calculate_gross_fees(
                    collected_fees0=_collected_fees0, collected_fees1=_collected_fees1
                )
                _gross_fees_usd = (
                    float(_gross_fees_0) * _token0_price
                    + float(_gross_fees_1) * _token1_price
                )
                # calculate volume
                _volume_0, _volume_1 = _hypervisor_status_obj.calculate_gross_volume(
                    gross_fees0=_gross_fees_0, gross_fees1=_gross_fees_1
                )
                _volume_usd = (
                    float(_volume_0) * _token0_price + float(_volume_1) * _token1_price
                )

                hypervisor_results[_hypervisor_status_obj.address] = {
                    "symbol": _hypervisor_status_obj.symbol,
                    "protocol": text_to_protocol(_hypervisor_status_obj.dex),
                    "underlying_values_usd": [_underlying_value_usd],
                    "liquidity_inrange": [_liquidity_in_range],
                    "liquidity_base": [_liquidity_base],
                    "liquidity_total": [_liquidity_total],
                    "liquidity_seconds_inrange": [_liquidity_seconds_in_range],
                    "dataset_timestamps": [_hypervisor_status_obj.timestamp],
                    "dataset_seconds": [_dataset_seconds],
                    "tokens": {
                        "token0": {
                            "symbol": _hypervisor_status_obj.pool.token0.address,
                            "address": _hypervisor_status_obj.pool.token0.address,
                            "decimals": _hypervisor_status_obj.pool.token0.decimals,
                        },
                        "token1": {
                            "symbol": _hypervisor_status_obj.pool.token1.address,
                            "address": _hypervisor_status_obj.pool.token1.address,
                            "decimals": _hypervisor_status_obj.pool.token1.decimals,
                        },
                    },
                    "prices_0": [_token0_price],
                    "prices_1": [_token1_price],
                    "share_prices": [_hypervisor_sharePrice],
                    "volume": _volume_usd,
                    "gross_fees": _gross_fees_usd,
                    "collected_fees": _collected_fees_usd,
                    "ini": {
                        "price0": _token0_price,
                        "price1": _token1_price,
                        "underlying0": float(_underlying_value.token0),
                        "underlying1": float(_underlying_value.token1),
                        "share": _hypervisor_sharePrice,
                    },
                    "end": {
                        "price0": _token0_price,
                        "price1": _token1_price,
                        "underlying0": float(_underlying_value.token0),
                        "underlying1": float(_underlying_value.token1),
                        "share": _hypervisor_sharePrice,
                    },
                }
            else:
                # append values for averages
                hypervisor_results[_hypervisor_status_obj.address][
                    "underlying_values_usd"
                ].append(_underlying_value_usd)
                hypervisor_results[_hypervisor_status_obj.address][
                    "liquidity_inrange"
                ].append(_liquidity_in_range)
                hypervisor_results[_hypervisor_status_obj.address][
                    "liquidity_base"
                ].append(_liquidity_base)
                hypervisor_results[_hypervisor_status_obj.address][
                    "liquidity_total"
                ].append(_liquidity_total)
                hypervisor_results[_hypervisor_status_obj.address][
                    "liquidity_seconds_inrange"
                ].append(_liquidity_seconds_in_range)
                hypervisor_results[_hypervisor_status_obj.address][
                    "dataset_seconds"
                ].append(_dataset_seconds)
                hypervisor_results[_hypervisor_status_obj.address]["prices_0"].append(
                    _token0_price
                )
                hypervisor_results[_hypervisor_status_obj.address]["prices_1"].append(
                    _token1_price
                )
                hypervisor_results[_hypervisor_status_obj.address][
                    "share_prices"
                ].append(_hypervisor_sharePrice)
                # update end values
                hypervisor_results[_hypervisor_status_obj.address]["end"] = {
                    "price0": _token0_price,
                    "price1": _token1_price,
                    "underlying0": float(_underlying_value.token0),
                    "underlying1": float(_underlying_value.token1),
                    "share": _hypervisor_sharePrice,
                    "dataset_timestamp": _hypervisor_status_obj.timestamp,
                }

        # Build report ######
        result: list[report_object] = []
        for address, hype_data in hypervisor_results.items():
            hold_pnl = (
                (
                    hype_data["end"]["price0"] * hype_data["ini"]["underlying0"]
                    + hype_data["end"]["price1"] * hype_data["ini"]["underlying1"]
                )
                - (
                    hype_data["ini"]["price0"] * hype_data["ini"]["underlying0"]
                    + hype_data["ini"]["price1"] * hype_data["ini"]["underlying1"]
                )
            ) / (
                hype_data["ini"]["price0"] * hype_data["ini"]["underlying0"]
                + hype_data["ini"]["price1"] * hype_data["ini"]["underlying1"]
            )
            lp_pnl = (
                hype_data["end"]["share"] - hype_data["ini"]["share"]
            ) / hype_data["ini"]["share"]

            # calculate time from last dataset known till end_timeframe: This will be added to dataset_seconds and others related
            # _dataset_extra_seconds = end_timestamp - hype_data["dataset_timestamps"][-1]

            result.append(
                report_object(
                    type=reportType.KPI,
                    protocol=hype_data["protocol"],
                    timeframe=timeframe_object(
                        ini=time_object(block=ini_block, timestamp=ini_timestamp),
                        end=time_object(block=end_block, timestamp=end_timestamp),
                    ),
                    data={
                        # "seconds": end_timestamp - ini_timestamp,
                        "dataset_seconds": sum(hype_data["dataset_seconds"]),
                        "address": address,
                        "symbol": hype_data["symbol"],
                        "mean_tvl": sum(hype_data["underlying_values_usd"])
                        / len(hype_data["underlying_values_usd"])
                        if len(hype_data["underlying_values_usd"])
                        else 0,
                        "average_liquidity_inrange": sum(hype_data["liquidity_inrange"])
                        / sum(hype_data["liquidity_total"])
                        if sum(hype_data["liquidity_total"])
                        else 0,
                        "average_liquidity_inrange vs base": sum(
                            hype_data["liquidity_inrange"]
                        )
                        / sum(hype_data["liquidity_base"])
                        if sum(hype_data["liquidity_base"])
                        else 0,
                        # add extra seconds only if last known dataset is inrange.
                        "liquidity_seconds_in_range": sum(
                            hype_data["liquidity_seconds_inrange"]
                        ),
                        "tokens": {
                            "token0": {
                                "symbol": hype_data["tokens"]["token0"]["symbol"],
                                "address": hype_data["tokens"]["token0"]["address"],
                                "decimals": hype_data["tokens"]["token0"]["decimals"],
                                "price_std": statistics.stdev(hype_data["prices_0"])
                                if len(hype_data["prices_0"]) > 1
                                else 0,
                                "price": hype_data["prices_0"][-1],
                            },
                            "token1": {
                                "symbol": hype_data["tokens"]["token1"]["symbol"],
                                "address": hype_data["tokens"]["token1"]["address"],
                                "decimals": hype_data["tokens"]["token1"]["decimals"],
                                "price_std": statistics.stdev(hype_data["prices_1"])
                                if len(hype_data["prices_1"]) > 1
                                else 0,
                                "price": hype_data["prices_1"][-1],
                            },
                        },
                        "share_price_std": statistics.stdev(hype_data["share_prices"])
                        if len(hype_data["share_prices"]) > 1
                        else 0,
                        "share_price": hype_data["share_prices"][-1],
                        "volume": hype_data["volume"],
                        "gross_fees": hype_data["gross_fees"],
                        "collected_fees": hype_data["collected_fees"],
                        "hypervisor_share_price": hype_data["end"]["share"],
                        "lp_pnl": lp_pnl,
                        "hold_pnl": hold_pnl,
                        "lpvshold": ((lp_pnl + 1) / (hold_pnl + 1)) - 1,
                        "rebalances": rebalances_qtty.get(address, 0),
                    },
                )
            )

        # return reports
        return result


def create_kpi_structure(
    chain: Chain,
    ini_timestamp: int,
    end_timestamp: int,
    ini_block: int | None = None,
    end_block: int | None = None,
) -> dict:
    """

    Args:
        chain (Chain):
        ini_timestamp (int | None, optional): Initial timestamp. Defaults to today( utc ).
        end_timestamp (int | None, optional): End timestamp . Defaults to ini_timestamp day last timestamp.
        ini_block (int | None, optional): When supplied, scrape hypervisors at block. Defaults to None.
        end_block (int | None, optional):  When supplied, scrape hypervisors at block. Defaults to None.
    Returns:
        dict: {
            <hypervisor address>:{
                "address": ,
                "symbol": ,
                "pool": {
                    "address": ,
                    "symbol": ,
                    "token0": {
                        "address": ,
                        "symbol": ,
                        "decimals": ,
                        "prices": {},
                    },
                    "token1": {
                        "address": ,
                        "symbol": ,
                        "decimals": ,
                        "prices": {},
                    },
                },
                "status_list": [],
                "operations_summary": {},
                "rebalances_qtty": 0,
                "raw_kpi_data":{
                    "underlying_values_usd": [],
                    "underlying_0": [],
                    "underlying_1": [],
                    "liquidity_inrange": [],
                    "liquidity_base": [],
                    "liquidity_total": [],
                    "seconds_inrange": [],
                    "share_prices": [],
                }
               }
        }
    """

    # prepare result
    result = {}

    # fill with hypervisor static info
    for static in get_from_localdb(
        network=chain.database_name,
        collection="static",
        find={},
        batch_size=50000,
    ):
        if not static["address"] in result:
            result[static["address"]] = {
                "address": static["address"],
                "symbol": static["symbol"],
                "dex": static["dex"],
                "pool": {
                    "address": static["pool"]["address"],
                    "dex": static["pool"]["dex"],
                    "token0": {
                        "address": static["pool"]["token0"]["address"],
                        "symbol": static["pool"]["token0"]["symbol"],
                        "decimals": static["pool"]["token0"]["decimals"],
                        "prices": {},
                    },
                    "token1": {
                        "address": static["pool"]["token1"]["address"],
                        "symbol": static["pool"]["token1"]["symbol"],
                        "decimals": static["pool"]["token1"]["decimals"],
                        "prices": {},
                    },
                },
                "status_list": [],
                "operations_summary": {},
                "rebalances_qtty": 0,
                "raw_kpi_data": {
                    "underlying_values_usd": [],
                    "underlying_0": [],
                    "underlying_1": [],
                    "liquidity_inrange": [],
                    "liquidity_base": [],
                    "liquidity_total": [],
                    "seconds_inrange": [],
                    "share_prices": [],
                },
            }
        else:
            raise Exception(f"Repeated hypervisor address {static['address']}")

    # fill result with status list
    temp_hypervisor_status_list = []
    for hype in get_from_localdb(
        network=chain.database_name,
        collection="status",
        aggregate=[
            {
                "$match": {
                    "$and": [
                        {"timestamp": {"$gte": ini_timestamp}},
                        {"timestamp": {"$lte": end_timestamp}},
                    ]
                }
            },
            {"$sort": {"timestamp": 1}},
            {
                "$group": {
                    "_id": "$address",
                    "items": {"$push": "$$ROOT"},
                }
            },
        ],
        batch_size=50000,
    ):
        result[hype["_id"]]["status_list"] = hype["items"]

        temp_hypervisor_status_list.extend(hype["items"])

    # get all prices
    prices = get_prices_from_db_usingHypervisorStatus(
        chain=chain, hypervisor_status_list=temp_hypervisor_status_list
    )

    for address, hype in tqdm.tqdm(result.items()):
        # add hype snapshots at ini_block
        if ini_block:
            # add at the beginning of the list the status at ini_block
            try:
                hype["status_list"].insert(
                    0,
                    build_db_hypervisor_multicall(
                        address=address,
                        network=chain.database_name,
                        block=ini_block,
                        dex=hype["dex"],
                        pool_address=hype["pool"]["address"],
                        token0_address=hype["pool"]["token0"]["address"],
                        token1_address=hype["pool"]["token1"]["address"],
                    ),
                )
            except Exception as e:
                pass

        if end_block:
            # add at the end of the list the status at end_block
            try:
                hype["status_list"].append(
                    build_db_hypervisor_multicall(
                        address=address,
                        network=chain.database_name,
                        block=end_block,
                        dex=hype["dex"],
                        pool_address=hype["pool"]["address"],
                        token0_address=hype["pool"]["token0"]["address"],
                        token1_address=hype["pool"]["token1"]["address"],
                    ),
                )
            except Exception as e:
                pass

        # fill result with prices
        for k, v in prices.items():
            if k.endswith(hype["pool"]["token0"]["address"]):
                __block = int(k.split("_")[0])
                hype["pool"]["token0"]["prices"][__block] = v
            elif k.endswith(hype["pool"]["token1"]["address"]):
                __block = int(k.split("_")[0])
                hype["pool"]["token1"]["prices"][__block] = v

    # get the operations summary of each of those hypes
    for x in get_from_localdb(
        network=chain.database_name,
        collection="operations",
        aggregate=database_local.query_operations_summary(
            hypervisor_addresses=list(result.keys()),
            timestamp_ini=ini_timestamp,
            timestamp_end=end_timestamp,
        ),
    ):
        if len(result[x["address"]]["operations_summary"]) > 0:
            raise ValueError(
                f" repeated hypervisor address {x['address']} in operations summary"
            )
        result[x["address"]][
            "operations_summary"
        ] = database_local.convert_d128_to_decimal(item=x)

    # get the rebalances qtty of each of those hypes
    for x in get_from_localdb(
        network=chain.database_name,
        collection="operations",
        aggregate=database_local.query_hypervisor_rebalances_qtty(
            hypervisor_addresses=list(result.keys()),
            timestamp_ini=ini_timestamp,
            timestamp_end=end_timestamp,
        ),
    ):
        result[x["address"]]["rebalances_qtty"] = x["rebalances"]

    return result


def create_timestampBlocks(
    chain: Chain, ini_timestamp: int | None = None, end_timestamp: int | None = None
) -> tuple[int, int, int, int]:
    """Convert or create suitable timestamps and blocks for a given chain and timeframe
        ** if ini_timestamp but no end_timestamp is provided, ini_timestamp will be replaced with utc 00:00:00 and end_timestamp will be calculated as ini_timestamp + 1 day-1 second
        ** if ini_timestamp and end_timestamp are provided, will be used as is.

        Blocks are calculated from timestamps using erc20helper.blockNumberFromTimestamp

    Args:
        chain (Chain): _description_
        ini_timestamp (int | None, optional): Start timestamp. Defaults to None.
        end_timestamp (int | None, optional): End timestamp. Defaults to None.

    Returns:
        tuple[int,int,int,int]: ini_timestamp, end_timestamp, ini_block, end_block
    """

    # set timeframe
    if not ini_timestamp and not end_timestamp:
        ini_timestamp, end_timestamp = build_day_timeframe()
    elif ini_timestamp and not end_timestamp:
        ini_timestamp, end_timestamp = build_day_timeframe(timestamp=ini_timestamp)
    elif not ini_timestamp and end_timestamp:
        raise Exception("Can't set end_timestamp without ini_timestamp")

    # calculate blocks from timestamps
    erchelper = build_erc20_helper(chain=chain)
    ini_block = erchelper.blockNumberFromTimestamp(timestamp=ini_timestamp)
    end_block = erchelper.blockNumberFromTimestamp(timestamp=end_timestamp)

    return ini_timestamp, end_timestamp, ini_block, end_block


def build_day_timeframe(
    timestamp: int | None = None,
) -> tuple[int, int]:
    """Get ini_timestamp and end_timestamp of a day, given a timestamp or today( utc ).

    Args:
        timestamp (int | None, optional): _description_. Defaults to now( UTC ).

    Returns:
        tuple[int,int]: ini_timestamp, end_timestamp
    """
    # set timeframe
    if not timestamp:
        # set ini_timestamp to today utc 00:00:00
        ini_timestamp = (
            datetime.now(timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
        )
    else:
        # set ini_timestamp to timestamp utc 00:00:00
        ini_timestamp = (
            datetime.fromtimestamp(timestamp, timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
        )

    # set end_timestamp to ini_timestamp day last timestamp
    end_timestamp = get_last_timestamp_of_day(timestamp=ini_timestamp)

    # return timeframe
    return int(ini_timestamp), int(end_timestamp)
