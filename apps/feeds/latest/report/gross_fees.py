from datetime import datetime
import logging
from bins.database.common.db_collections_common import db_collections_common
from bins.database.common.objects.hypervisor import time_object, timeframe_object
from bins.database.common.objects.reports import report_object
from bins.database.helpers import (
    get_latest_prices_from_db,
    get_from_localdb,
    get_default_localdb,
)
from bins.formulas.fees import convert_feeProtocol
from bins.general.enums import Chain, Protocol, reportType
from bins.w3.builders import build_erc20_helper, build_hypervisor


# reports calculated gross fees using the protocol's fee switch data


# RAMSES
# block start: epoch start block
# block end: epoch end block ( next epoch start block - 1 )
def feed_report_ramses_gross_fees(
    chain: Chain,
    periods_back: int = 3,
) -> list[report_object]:
    """

    Args:
        chain (Chain): _description_
        periods_back (int, optional): _description_. Defaults to 3.

    Returns:
        list[report]: reports
    """
    # output = []

    # get hypervisors current prices
    token_prices = get_latest_prices_from_db(network=chain)

    # construct start end dates for each epoch
    period_timestamps = []
    # current epoch is utc timestamp now // 1 week
    current_period = int(int(datetime.utcnow().timestamp()) // (7 * 24 * 60 * 60))
    period_timestamps.append(
        {
            "period": current_period,
            "ini": current_period * 7 * 24 * 60 * 60,
            # now ( end of current epoch is utc now )
            "end": int(datetime.utcnow().timestamp()),
        }
    )
    for period in range(1, periods_back + 1):
        _period = current_period - period
        period_timestamps.append(
            {
                "period": _period,
                "ini": _period * 7 * 24 * 60 * 60,
                "end": (_period + 1) * 7 * 24 * 60 * 60 - 1,
            }
        )

    # sort period_timestamps by descending period
    period_timestamps = sorted(
        period_timestamps, key=lambda x: x["period"], reverse=False
    )

    for period in period_timestamps:
        try:
            if report_data := create_report_data_ramses(
                chain=chain,
                ini_timestamp=period["ini"],
                end_timestamp=period["end"],
                token_prices=token_prices,
            ):
                rep = report_object(
                    type=reportType.GROSSFEES,
                    protocol=Protocol.RAMSES,
                    timeframe=timeframe_object(
                        ini=time_object(
                            block=report_data["ini_block"],
                            timestamp=report_data["ini_timestamp"],
                        ),
                        end=time_object(
                            block=report_data["end_block"],
                            timestamp=report_data["end_timestamp"],
                        ),
                    ),
                    data=report_data,
                )
                # output.append(rep)

                # convert to d128
                converted_item = get_default_localdb(
                    network=Chain.ARBITRUM.database_name
                ).convert_decimal_to_d128(rep.to_dict())

                # save to database
                if db_result := get_default_localdb(
                    network=Chain.ARBITRUM.database_name
                ).save_item_to_database(data=converted_item, collection_name="reports"):
                    logging.getLogger(__name__).debug(f" Saved report {rep.id} to db")
                else:
                    logging.getLogger(__name__).error(
                        f" Can't save report {rep.id} to db"
                    )

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error processing ramses report for period {period}. Error: {e} "
            )


def create_report_data_ramses___OOO(
    chain: Chain,
    ini_timestamp: int,
    end_timestamp: int,
    token_prices: dict[str, float] | None = None,
):
    """Calculated gross fees within a Ramses epoch
        Negative fees may occur because uncollected fees from block ini -1 are bubtracted from block end's (uncollected fees).

    Args:
        chain (Chain):
        ini_timestamp (int):
        end_timestamp (int):
        token_prices (dict[str, float] | None, optional): . Defaults to None.

    Returns:
        dict: { period:
            "ini_timestamp": ,
            "end_timestamp": ,
            "ini_block": 0,
            "end_block": 0,
            "total_fees_0": 0,
            "total_fees_1": 0,
            "total_usd": 0,
            "protocol_fee_0": 0,
            "protocol_fee_1": 0,
            "grossFees_0": 0,
            "grossFees_1": 0,
            "grossFees_usd": 0,
            "breakdown": [],}
    """
    # calculate period
    period = ini_timestamp // (7 * 24 * 60 * 60)

    # initialize output
    result = {
        "period": period,
        "ini_timestamp": ini_timestamp,
        "end_timestamp": end_timestamp,
        "ini_block": 0,
        "end_block": 0,
        "total_fees_0": 0,
        "total_fees_1": 0,
        "total_usd": 0,
        "protocol_fee_0": 0,
        "protocol_fee_1": 0,
        "grossFees_0": 0,
        "grossFees_1": 0,
        "grossFees_usd": 0,
        "breakdown": [],
        "total_fees_breakdown": {
            "collectedFees_0": 0,
            "collectedFees_1": 0,
            "collectedFees_usd": 0,
            "uncollectedFees_0": 0,
            "uncollectedFees_1": 0,
            "uncollectedFees_usd": 0,
        },
    }

    if not token_prices:
        # get hypervisors current prices
        token_prices = get_latest_prices_from_db(network=chain)

    # convert timestamp to block
    er2_dumm = build_erc20_helper(chain=chain)
    # calculate initial block from ini_timestamp
    start_block = er2_dumm.blockNumberFromTimestamp(timestamp=ini_timestamp)
    end_block = er2_dumm.blockNumberFromTimestamp(timestamp=end_timestamp)
    # add blocks to result
    result["ini_block"] = start_block
    result["end_block"] = end_block

    # get operations summary
    for hype_summary in get_fee_operations_summary_data_from_db(
        chain=chain,
        protocol=Protocol.RAMSES,
        start_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
    ):
        # convert to float
        hype_summary = db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=hype_summary)
        )

        # create local start end block vars
        _start_block = start_block
        _end_block = end_block
        try:
            # ease hypervisor static data access
            hype_status = hype_summary["hypervisor_status"]
            if not hype_status:
                logging.getLogger(__name__).error(
                    f"Static data not found for hypervisor {hype_summary['address']}"
                )
                continue

            if _start_block < hype_summary["hypervisor_static"]["block"]:
                # check if end block is higher than hypervisor creation block
                if _end_block >= hype_summary["hypervisor_static"]["block"]:
                    _start_block = hype_summary["hypervisor_static"]["block"]
                    logging.getLogger(__name__).warning(
                        f" Start block is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}. Setting start block to {_start_block}"
                    )
                else:
                    logging.getLogger(__name__).error(
                        f"End block {_end_block} is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}"
                    )
                    continue
            if _end_block < hype_summary["hypervisor_static"]["block"]:
                logging.getLogger(__name__).error(
                    f"End block {_end_block} is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}"
                )
                continue

            # get first item fees collected ( sum all fees collected at block ini)
            first_item_collected_fees_token0 = 0
            first_item_collected_fees_token1 = 0
            for item in hype_summary["items"]:
                if item["block"] == hype_summary["block_ini"]:
                    first_item_collected_fees_token0 += (
                        float(str(item["collectedFees_token0"]))
                        + float(str(item["zeroBurnFees_token0"]))
                    ) / 10 ** item["decimals_token0"]
                    first_item_collected_fees_token1 += (
                        float(str(item["collectedFees_token1"]))
                        + float(str(item["zeroBurnFees_token1"]))
                    ) / 10 ** item["decimals_token1"]

            # get uncollected fees at start and end block
            hype_status_ini = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=_start_block,
                hypervisor_address=hype_summary["address"],
                cached=True,
            )
            hype_status_ini_end = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=hype_summary["block_ini"],
                hypervisor_address=hype_summary["address"],
                cached=True,
            )
            # check if we need to calc initial uncollected fees
            if hype_summary["block_ini"] > _start_block:
                logging.getLogger(__name__).debug(
                    f" Calculating initial uncollected fees 1 of 2"
                )
                ini_uncollected_fees = hype_status_ini.get_fees_uncollected()
                logging.getLogger(__name__).debug(
                    f" Calculating initial uncollected fees 2 of 2"
                )
                ini_uncollected_fees_end = hype_status_ini_end.get_fees_uncollected()
                ini_uncollected_fees_token0 = (
                    ini_uncollected_fees_end["qtty_token0"]
                    - ini_uncollected_fees["qtty_token0"]
                )
                ini_uncollected_fees_token1 = (
                    ini_uncollected_fees_end["qtty_token1"]
                    - ini_uncollected_fees["qtty_token1"]
                )
            else:
                ini_uncollected_fees_token0 = 0
                ini_uncollected_fees_token1 = 0

            hype_status_end = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=_end_block,
                hypervisor_address=hype_summary["address"],
                cached=True,
            )
            hype_status_end_end = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=hype_summary["block_end"],
                hypervisor_address=hype_summary["address"],
                cached=True,
            )
            if hype_summary["block_end"] < _end_block:
                logging.getLogger(__name__).debug(
                    f" Calculating end uncollected fees 1 of 2"
                )
                end_uncollected_fees = hype_status_end.get_fees_uncollected()
                logging.getLogger(__name__).debug(
                    f" Calculating end uncollected fees 2 of 2"
                )
                end_uncollected_fees_ini = hype_status_end_end.get_fees_uncollected()
                end_uncollected_fees_token0 = (
                    end_uncollected_fees["qtty_token0"]
                    - end_uncollected_fees_ini["qtty_token0"]
                )
                end_uncollected_fees_token1 = (
                    end_uncollected_fees["qtty_token1"]
                    - end_uncollected_fees_ini["qtty_token1"]
                )
            else:
                end_uncollected_fees_token0 = 0
                end_uncollected_fees_token1 = 0

            # ease hypervisor price access
            token0_price = token_prices.get(hype_status["pool"]["token0"]["address"], 0)
            token1_price = token_prices.get(hype_status["pool"]["token1"]["address"], 0)
            if not token0_price or not token1_price:
                logging.getLogger(__name__).error(
                    f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
                )
                continue

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

            # calc uncollected fees
            calculate_uncollected_fees_period(
                hypervisors_status=hype_summary["items"],
                ini_token0=ini_uncollected_fees["qtty_token0"],
                ini_token1=ini_uncollected_fees["qtty_token1"],
                end_token0=end_uncollected_fees["qtty_token0"],
                end_token1=end_uncollected_fees["qtty_token1"],
            )

            uncollectedFees_0 = float(
                str(end_uncollected_fees_token0 + ini_uncollected_fees_token0)
            )
            uncollectedFees_1 = float(
                str(end_uncollected_fees_token1 + ini_uncollected_fees_token1)
            )

            # calculate collected fees
            collectedFees_0 = (
                hype_summary["collectedFees_token0"]
                + hype_summary["zeroBurnFees_token0"]
                - first_item_collected_fees_token0
            )
            collectedFees_1 = (
                hype_summary["collectedFees_token1"]
                + hype_summary["zeroBurnFees_token1"]
                - first_item_collected_fees_token1
            )
            collectedFees_usd = (
                collectedFees_0 * token0_price + collectedFees_1 * token1_price
            )
            uncollectedFees_usd = (
                uncollectedFees_0 * token0_price + uncollectedFees_1 * token1_price
            )

            if protocol_fee_0 > 100 or protocol_fee_1 > 100:
                logging.getLogger(__name__).warning(
                    f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
                )

            # calculate gross fees
            if protocol_fee_0 < 100:
                grossFees_0 = (collectedFees_0 + uncollectedFees_0) / (
                    1 - (protocol_fee_0 / 100)
                )
            else:
                grossFees_0 = collectedFees_0 + uncollectedFees_0

            if protocol_fee_1 < 100:
                grossFees_1 = (collectedFees_1 + uncollectedFees_1) / (
                    1 - (protocol_fee_1 / 100)
                )
            else:
                grossFees_1 = collectedFees_1 + uncollectedFees_1

            grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

            # build output
            result["total_fees_0"] += collectedFees_0 + uncollectedFees_0
            result["total_fees_1"] += collectedFees_1 + uncollectedFees_1
            result["total_usd"] += collectedFees_usd + uncollectedFees_usd
            # result["protocol_fee_0"] = protocol_fee_0
            # result["protocol_fee_1"] = protocol_fee_1
            result["grossFees_0"] += grossFees_0
            result["grossFees_1"] += grossFees_1
            result["grossFees_usd"] += grossFees_usd

            result["total_fees_breakdown"]["collectedFees_0"] += collectedFees_0
            result["total_fees_breakdown"]["collectedFees_1"] += collectedFees_1
            result["total_fees_breakdown"]["collectedFees_usd"] += collectedFees_usd
            result["total_fees_breakdown"]["uncollectedFees_0"] += uncollectedFees_0
            result["total_fees_breakdown"]["uncollectedFees_1"] += uncollectedFees_1
            result["total_fees_breakdown"]["uncollectedFees_usd"] += uncollectedFees_usd

            result["breakdown"].append(
                {
                    "hypervisor": hype_summary["address"],
                    "start_block": _start_block,
                    "end_block": _end_block,
                    "start_timestamp": hype_status_ini._timestamp,
                    "end_timestamp": hype_status_end._timestamp,
                    "collectedFees_0": collectedFees_0,
                    "collectedFees_1": collectedFees_1,
                    "collectedFees_usd": collectedFees_usd,
                    "uncollectedFees_0": uncollectedFees_0,
                    "uncollectedFees_1": uncollectedFees_1,
                    "uncollectedFees_usd": uncollectedFees_usd,
                    "protocol_fee_0": protocol_fee_0,
                    "protocol_fee_1": protocol_fee_1,
                    "grossFees_0": grossFees_0,
                    "grossFees_1": grossFees_1,
                    "grossFees_usd": grossFees_usd,
                }
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error processing ramses report for hypervisor {hype_summary['address']}. Error: {e} "
            )

    # calculate total protocol fee
    result["protocol_fee_0"] = (
        result["total_fees_0"] / result["grossFees_0"]
        if result["grossFees_0"] > 0
        else 0
    )
    result["protocol_fee_1"] = (
        result["total_fees_1"] / result["grossFees_1"]
        if result["grossFees_1"] > 0
        else 0
    )

    #
    return result


def create_report_data_ramses(
    chain: Chain,
    ini_timestamp: int,
    end_timestamp: int,
    token_prices: dict[str, float] | None = None,
):
    """Calculated gross fees within a Ramses epoch
        Negative fees may occur because uncollected fees from block ini -1 are bubtracted from block end's (uncollected fees).

    Args:
        chain (Chain):
        ini_timestamp (int):
        end_timestamp (int):
        token_prices (dict[str, float] | None, optional): . Defaults to None.

    Returns:
        dict: { period:
            "ini_timestamp": ,
            "end_timestamp": ,
            "ini_block": 0,
            "end_block": 0,
            "total_fees_0": 0,
            "total_fees_1": 0,
            "total_usd": 0,
            "protocol_fee_0": 0,
            "protocol_fee_1": 0,
            "grossFees_0": 0,
            "grossFees_1": 0,
            "grossFees_usd": 0,
            "breakdown": [],}
    """
    # calculate period
    period = ini_timestamp // (7 * 24 * 60 * 60)

    # initialize output
    result = {
        "period": period,
        "ini_timestamp": ini_timestamp,
        "end_timestamp": end_timestamp,
        "ini_block": 0,
        "end_block": 0,
        "total_fees_0": 0,
        "total_fees_1": 0,
        "total_usd": 0,
        "protocol_fee_0": 0,
        "protocol_fee_1": 0,
        "grossFees_0": 0,
        "grossFees_1": 0,
        "grossFees_usd": 0,
        "breakdown": [],
        "total_fees_breakdown": {
            "collectedFees_0": 0,
            "collectedFees_1": 0,
            "collectedFees_usd": 0,
            "uncollectedFees_0": 0,
            "uncollectedFees_1": 0,
            "uncollectedFees_usd": 0,
        },
    }

    if not token_prices:
        # get hypervisors current prices
        token_prices = get_latest_prices_from_db(network=chain)

    # convert timestamp to block
    er2_dumm = build_erc20_helper(chain=chain)
    # calculate initial block from ini_timestamp
    start_block = er2_dumm.blockNumberFromTimestamp(timestamp=ini_timestamp)
    end_block = er2_dumm.blockNumberFromTimestamp(timestamp=end_timestamp)
    # add blocks to result
    result["ini_block"] = start_block
    result["end_block"] = end_block

    # get operations summary
    for hype_summary in get_fee_operations_summary_data_from_db(
        chain=chain,
        protocol=Protocol.RAMSES,
        start_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
    ):
        # convert to float
        hype_summary = db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=hype_summary)
        )

        ###########################################################################
        ###########################################################################
        ###########################################################################
        if hype_summary["address"] != "0xeae2b3f864d1307d003375aef141b773a7bbc6e0":
            continue
        ###########################################################################
        ###########################################################################
        ###########################################################################
        ###########################################################################

        # create local start end block vars
        _start_block = start_block
        _end_block = end_block
        try:
            # ease hypervisor static data access
            hype_status = hype_summary["hypervisor_status"]
            if not hype_status:
                logging.getLogger(__name__).error(
                    f"Static data not found for hypervisor {hype_summary['address']}"
                )
                continue

            if _start_block < hype_summary["hypervisor_static"]["block"]:
                # check if end block is higher than hypervisor creation block
                if _end_block >= hype_summary["hypervisor_static"]["block"]:
                    _start_block = hype_summary["hypervisor_static"]["block"]
                    logging.getLogger(__name__).warning(
                        f" Start block is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}. Setting start block to {_start_block}"
                    )
                else:
                    logging.getLogger(__name__).error(
                        f"End block {_end_block} is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}"
                    )
                    continue
            if _end_block < hype_summary["hypervisor_static"]["block"]:
                logging.getLogger(__name__).error(
                    f"End block {_end_block} is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}"
                )
                continue

            # get uncollected fees at start and end block
            hype_status_ini = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=_start_block,
                hypervisor_address=hype_summary["address"],
                cached=True,
            )
            hype_status_ini222 = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=hype_summary["block_ini"] - 1,
                hypervisor_address=hype_summary["address"],
                cached=True,
            )
            ini_uncollected_fees2222 = hype_status_ini222.get_fees_uncollected()
            # check if we need to calc initial uncollected fees
            if hype_summary["block_ini"] > _start_block:
                logging.getLogger(__name__).debug(
                    f" Calculating initial uncollected fees"
                )
                ini_uncollected_fees = hype_status_ini.get_fees_uncollected()

            hype_status_end = build_hypervisor(
                network=chain.database_name,
                protocol=Protocol.RAMSES,
                block=_end_block,
                hypervisor_address=hype_summary["address"],
                cached=True,
            )

            if hype_summary["block_end"] < _end_block:
                logging.getLogger(__name__).debug(f" Calculating end uncollected fees")
                end_uncollected_fees = hype_status_end.get_fees_uncollected()

            # ease hypervisor price access
            token0_price = token_prices.get(hype_status["pool"]["token0"]["address"], 0)
            token1_price = token_prices.get(hype_status["pool"]["token1"]["address"], 0)
            if not token0_price or not token1_price:
                logging.getLogger(__name__).error(
                    f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
                )
                continue

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

            # calc uncollected fees
            uncollected_token0, uncollected_token1 = calculate_uncollected_fees_period(
                hypervisors_status=hype_summary["items"],
                ini_token0=ini_uncollected_fees["qtty_token0"],
                ini_token1=ini_uncollected_fees["qtty_token1"],
                end_token0=end_uncollected_fees["qtty_token0"],
                end_token1=end_uncollected_fees["qtty_token1"],
            )

            uncollectedFees_0 = float(
                str(end_uncollected_fees_token0 + ini_uncollected_fees_token0)
            )
            uncollectedFees_1 = float(
                str(end_uncollected_fees_token1 + ini_uncollected_fees_token1)
            )

            # calculate collected fees
            collectedFees_0 = (
                hype_summary["collectedFees_token0"]
                + hype_summary["zeroBurnFees_token0"]
                - first_item_collected_fees_token0
            )
            collectedFees_1 = (
                hype_summary["collectedFees_token1"]
                + hype_summary["zeroBurnFees_token1"]
                - first_item_collected_fees_token1
            )
            collectedFees_usd = (
                collectedFees_0 * token0_price + collectedFees_1 * token1_price
            )
            uncollectedFees_usd = (
                uncollectedFees_0 * token0_price + uncollectedFees_1 * token1_price
            )

            if protocol_fee_0 > 100 or protocol_fee_1 > 100:
                logging.getLogger(__name__).warning(
                    f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
                )

            # calculate gross fees
            if protocol_fee_0 < 100:
                grossFees_0 = (collectedFees_0 + uncollectedFees_0) / (
                    1 - (protocol_fee_0 / 100)
                )
            else:
                grossFees_0 = collectedFees_0 + uncollectedFees_0

            if protocol_fee_1 < 100:
                grossFees_1 = (collectedFees_1 + uncollectedFees_1) / (
                    1 - (protocol_fee_1 / 100)
                )
            else:
                grossFees_1 = collectedFees_1 + uncollectedFees_1

            grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

            # build output
            result["total_fees_0"] += collectedFees_0 + uncollectedFees_0
            result["total_fees_1"] += collectedFees_1 + uncollectedFees_1
            result["total_usd"] += collectedFees_usd + uncollectedFees_usd
            # result["protocol_fee_0"] = protocol_fee_0
            # result["protocol_fee_1"] = protocol_fee_1
            result["grossFees_0"] += grossFees_0
            result["grossFees_1"] += grossFees_1
            result["grossFees_usd"] += grossFees_usd

            result["total_fees_breakdown"]["collectedFees_0"] += collectedFees_0
            result["total_fees_breakdown"]["collectedFees_1"] += collectedFees_1
            result["total_fees_breakdown"]["collectedFees_usd"] += collectedFees_usd
            result["total_fees_breakdown"]["uncollectedFees_0"] += uncollectedFees_0
            result["total_fees_breakdown"]["uncollectedFees_1"] += uncollectedFees_1
            result["total_fees_breakdown"]["uncollectedFees_usd"] += uncollectedFees_usd

            result["breakdown"].append(
                {
                    "hypervisor": hype_summary["address"],
                    "start_block": _start_block,
                    "end_block": _end_block,
                    "start_timestamp": hype_status_ini._timestamp,
                    "end_timestamp": hype_status_end._timestamp,
                    "collectedFees_0": collectedFees_0,
                    "collectedFees_1": collectedFees_1,
                    "collectedFees_usd": collectedFees_usd,
                    "uncollectedFees_0": uncollectedFees_0,
                    "uncollectedFees_1": uncollectedFees_1,
                    "uncollectedFees_usd": uncollectedFees_usd,
                    "protocol_fee_0": protocol_fee_0,
                    "protocol_fee_1": protocol_fee_1,
                    "grossFees_0": grossFees_0,
                    "grossFees_1": grossFees_1,
                    "grossFees_usd": grossFees_usd,
                }
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error processing ramses report for hypervisor {hype_summary['address']}. Error: {e} "
            )

    # calculate total protocol fee
    result["protocol_fee_0"] = (
        result["total_fees_0"] / result["grossFees_0"]
        if result["grossFees_0"] > 0
        else 0
    )
    result["protocol_fee_1"] = (
        result["total_fees_1"] / result["grossFees_1"]
        if result["grossFees_1"] > 0
        else 0
    )

    #
    return result


def calculate_uncollected_fees_period(
    hypervisors_status: list[dict],
    ini_token0: float = 0,
    ini_token1: float = 0,
    end_token0: float = 0,
    end_token1: float = 0,
) -> tuple[float, float]:
    total_token0 = 0
    total_token1 = 0

    # set decimal divisor
    dec_div_token0 = 10 ** hypervisors_status[0]["status"]["pool"]["token0"]["decimals"]
    dec_div_token1 = 10 ** hypervisors_status[0]["status"]["pool"]["token1"]["decimals"]

    # ini uncollected fees
    if ini_token0:
        total_token0 += (
            float(hypervisors_status[0]["status"]["fees_uncollected"]["qtty_token0"])
            / dec_div_token0
        ) - float(str(ini_token0))
    if ini_token1:
        total_token1 += (
            float(hypervisors_status[0]["status"]["fees_uncollected"]["qtty_token1"])
            / dec_div_token1
        ) - float(str(ini_token1))

    # uncollected fees data
    for i in range(1, len(hypervisors_status)):
        total_token0 += (
            float(hypervisors_status[i]["status"]["fees_uncollected"]["qtty_token0"])
            - float(
                hypervisors_status[i - 1]["status"]["fees_uncollected"]["qtty_token0"]
            )
        ) / dec_div_token0
        total_token1 += (
            float(hypervisors_status[i]["status"]["fees_uncollected"]["qtty_token1"])
            - float(
                hypervisors_status[i - 1]["status"]["fees_uncollected"]["qtty_token1"]
            )
        ) / dec_div_token1

    # end uncollected fees
    if end_token0:
        total_token0 += float(str(end_token0)) - (
            float(hypervisors_status[-1]["status"]["fees_uncollected"]["qtty_token0"])
            / dec_div_token0
        )
    if end_token1:
        total_token1 += float(str(end_token1)) - (
            float(hypervisors_status[-1]["status"]["fees_uncollected"]["qtty_token1"])
            / dec_div_token1
        )

    return total_token0, total_token1


# def create_report_data_ramses(
#     chain: Chain,
#     ini_timestamp: int,
#     end_timestamp: int,
#     token_prices: dict[str, float] | None = None,
# ):
#     """Calculated gross fees within a Ramses epoch
#         Add uncollected fees at block end

#     Args:
#         chain (Chain):
#         ini_timestamp (int):
#         end_timestamp (int):
#         token_prices (dict[str, float] | None, optional): . Defaults to None.

#     Returns:
#         dict: { period:
#             "ini_timestamp": ,
#             "end_timestamp": ,
#             "ini_block": 0,
#             "end_block": 0,
#             "total_fees_0": 0,
#             "total_fees_1": 0,
#             "total_usd": 0,
#             "protocol_fee_0": 0,
#             "protocol_fee_1": 0,
#             "grossFees_0": 0,
#             "grossFees_1": 0,
#             "grossFees_usd": 0,
#             "breakdown": [],}
#     """
#     # calculate period
#     period = ini_timestamp // (7 * 24 * 60 * 60)

#     # initialize output
#     result = {
#         "period": period,
#         "ini_timestamp": ini_timestamp,
#         "end_timestamp": end_timestamp,
#         "ini_block": 0,
#         "end_block": 0,
#         "total_fees_0": 0,
#         "total_fees_1": 0,
#         "total_usd": 0,
#         "protocol_fee_0": 0,
#         "protocol_fee_1": 0,
#         "grossFees_0": 0,
#         "grossFees_1": 0,
#         "grossFees_usd": 0,
#         "breakdown": [],
#         "total_fees_breakdown": {
#             "collectedFees_0": 0,
#             "collectedFees_1": 0,
#             "collectedFees_usd": 0,
#             "uncollectedFees_0": 0,
#             "uncollectedFees_1": 0,
#             "uncollectedFees_usd": 0,
#         },
#     }

#     if not token_prices:
#         # get hypervisors current prices
#         token_prices = get_latest_prices_from_db(network=chain)

#     # convert timestamp to block
#     er2_dumm = build_erc20_helper(chain=chain)
#     # calculate initial block from ini_timestamp
#     # start_block = er2_dumm.blockNumberFromTimestamp(timestamp=ini_timestamp)
#     end_block = er2_dumm.blockNumberFromTimestamp(timestamp=end_timestamp)
#     # add blocks to result
#     # result["ini_block"] = start_block
#     result["end_block"] = end_block

#     # get operations summary
#     for hype_summary in get_operations_summary_data_from_db(
#         chain=chain,
#         protocol=Protocol.RAMSES,
#         start_timestamp=ini_timestamp,
#         end_timestamp=end_timestamp,
#     ):
#         # convert to float
#         hype_summary = db_collections_common.convert_decimal_to_float(
#             item=db_collections_common.convert_d128_to_decimal(item=hype_summary)
#         )

#         # create local start end block vars
#         # _start_block = start_block
#         _end_block = end_block
#         try:
#             # ease hypervisor static data access
#             hype_status = hype_summary["hypervisor_status"]
#             if not hype_status:
#                 logging.getLogger(__name__).error(
#                     f"Static data not found for hypervisor {hype_summary['address']}"
#                 )
#                 continue

#             # if _start_block < hype_summary["hypervisor_static"]["block"]:
#             #     # check if end block is lower than hypervisor creation block
#             #     if _end_block >= hype_summary["hypervisor_static"]["block"]:
#             #         _start_block = hype_summary["hypervisor_static"]["block"]
#             #         logging.getLogger(__name__).warning(
#             #             f" Start block is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}. Setting start block to {_start_block}"
#             #         )
#             #     else:
#             #         logging.getLogger(__name__).error(
#             #             f"End block {_end_block} is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}"
#             #         )
#             #         continue
#             if _end_block < hype_summary["hypervisor_static"]["block"]:
#                 logging.getLogger(__name__).error(
#                     f"End block {_end_block} is lower than hypervisor {hype_summary['address']} creation block {hype_summary['hypervisor_static']['block']}"
#                 )
#                 continue

#             # get uncollected fees at start and end block
#             # hype_status_ini = build_hypervisor(
#             #     network=chain.database_name,
#             #     protocol=Protocol.RAMSES,
#             #     block=_start_block,
#             #     hypervisor_address=hype_summary["address"],
#             # )
#             # ini_uncollected_fees = hype_status_ini.get_fees_uncollected()
#             hype_status_end = build_hypervisor(
#                 network=chain.database_name,
#                 protocol=Protocol.RAMSES,
#                 block=_end_block,
#                 hypervisor_address=hype_summary["address"],
#             )
#             end_uncollected_fees = hype_status_end.get_fees_uncollected()

#             # ease hypervisor price access
#             token0_price = token_prices.get(hype_status["pool"]["token0"]["address"], 0)
#             token1_price = token_prices.get(hype_status["pool"]["token1"]["address"], 0)
#             if not token0_price or not token1_price:
#                 logging.getLogger(__name__).error(
#                     f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
#                 )
#                 continue

#             # calculate protocol fees
#             if "globalState" in hype_status["pool"]:
#                 protocol_fee_0_raw = hype_status["pool"]["globalState"][
#                     "communityFeeToken0"
#                 ]
#                 protocol_fee_1_raw = hype_status["pool"]["globalState"][
#                     "communityFeeToken1"
#                 ]
#             else:
#                 # convert from 8 decimals
#                 protocol_fee_0_raw = hype_status["pool"]["slot0"]["feeProtocol"] % 16
#                 protocol_fee_1_raw = hype_status["pool"]["slot0"]["feeProtocol"] >> 4

#             # convert to percent (0-100)
#             protocol_fee_0, protocol_fee_1 = convert_feeProtocol(
#                 feeProtocol0=protocol_fee_0_raw,
#                 feeProtocol1=protocol_fee_1_raw,
#                 hypervisor_protocol=hype_status["dex"],
#                 pool_protocol=hype_status["pool"]["dex"],
#             )

#             uncollectedFees_0 = float(
#                 str(
#                     end_uncollected_fees["qtty_token0"]
#                     # - ini_uncollected_fees["qtty_token0"]
#                 )
#             )
#             uncollectedFees_1 = float(
#                 str(
#                     end_uncollected_fees["qtty_token1"]
#                     # - ini_uncollected_fees["qtty_token1"]
#                 )
#             )

#             # calculate collected fees
#             collectedFees_0 = (
#                 hype_summary["collectedFees_token0"]
#                 + hype_summary["zeroBurnFees_token0"]
#             )
#             collectedFees_1 = (
#                 hype_summary["collectedFees_token1"]
#                 + hype_summary["zeroBurnFees_token1"]
#             )
#             collectedFees_usd = (
#                 collectedFees_0 * token0_price + collectedFees_1 * token1_price
#             )
#             uncollectedFees_usd = (
#                 uncollectedFees_0 * token0_price + uncollectedFees_1 * token1_price
#             )

#             if protocol_fee_0 > 100 or protocol_fee_1 > 100:
#                 logging.getLogger(__name__).warning(
#                     f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
#                 )

#             # calculate gross fees
#             if protocol_fee_0 < 100:
#                 grossFees_0 = (collectedFees_0 + uncollectedFees_0) / (
#                     1 - (protocol_fee_0 / 100)
#                 )
#             else:
#                 grossFees_0 = collectedFees_0 + uncollectedFees_0

#             if protocol_fee_1 < 100:
#                 grossFees_1 = (collectedFees_1 + uncollectedFees_1) / (
#                     1 - (protocol_fee_1 / 100)
#                 )
#             else:
#                 grossFees_1 = collectedFees_1 + uncollectedFees_1

#             grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

#             # build output
#             result["total_fees_0"] += collectedFees_0 + uncollectedFees_0
#             result["total_fees_1"] += collectedFees_1 + uncollectedFees_1
#             result["total_usd"] += collectedFees_usd + uncollectedFees_usd
#             # result["protocol_fee_0"] = protocol_fee_0
#             # result["protocol_fee_1"] = protocol_fee_1
#             result["grossFees_0"] += grossFees_0
#             result["grossFees_1"] += grossFees_1
#             result["grossFees_usd"] += grossFees_usd

#             result["total_fees_breakdown"]["collectedFees_0"] += collectedFees_0
#             result["total_fees_breakdown"]["collectedFees_1"] += collectedFees_1
#             result["total_fees_breakdown"]["collectedFees_usd"] += collectedFees_usd
#             result["total_fees_breakdown"]["uncollectedFees_0"] += uncollectedFees_0
#             result["total_fees_breakdown"]["uncollectedFees_1"] += uncollectedFees_1
#             result["total_fees_breakdown"]["uncollectedFees_usd"] += uncollectedFees_usd

#             result["breakdown"].append(
#                 {
#                     "hypervisor": hype_summary["address"],
#                     # "start_block": _start_block,
#                     "end_block": _end_block,
#                     # "start_timestamp": hype_status_ini._timestamp,
#                     "end_timestamp": hype_status_end._timestamp,
#                     "collectedFees_0": collectedFees_0,
#                     "collectedFees_1": collectedFees_1,
#                     "collectedFees_usd": collectedFees_usd,
#                     "uncollectedFees_0": uncollectedFees_0,
#                     "uncollectedFees_1": uncollectedFees_1,
#                     "uncollectedFees_usd": uncollectedFees_usd,
#                     "protocol_fee_0": protocol_fee_0,
#                     "protocol_fee_1": protocol_fee_1,
#                     "grossFees_0": grossFees_0,
#                     "grossFees_1": grossFees_1,
#                     "grossFees_usd": grossFees_usd,
#                 }
#             )
#         except Exception as e:
#             logging.getLogger(__name__).exception(
#                 f" Error processing ramses report for hypervisor {hype_summary['address']}. Error: {e} "
#             )

#     # calculate total protocol fee
#     result["protocol_fee_0"] = result["total_fees_0"] / result["grossFees_0"]
#     result["protocol_fee_1"] = result["total_fees_1"] / result["grossFees_1"]

#     #
#     return result


def get_fee_operations_summary_data_from_db(
    chain: Chain,
    protocol: Protocol | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> list[dict]:
    """get a sumarized data portion for all hypervisors in the database for a period
        when no period is specified, it will return all available data

    Args:
        chain (Chain):
        protocol (Protocol | None, optional):  . Defaults to None.
        start_timestamp (int | None, optional):  . Defaults to None.
        end_timestamp (int | None, optional):  . Defaults to None.
        start_block (int | None, optional):  . Defaults to None.
        end_block (int | None, optional):  . Defaults to None.

    Returns:
        list :
    """

    # get all hypervisors last status from the database
    query_hype_status = [
        {"$sort": {"block": -1}},
        {
            "$group": {
                "_id": "$address",
                "data": {"$first": "$$ROOT"},
            }
        },
    ]
    # filter by protocol
    if protocol:
        query_hype_status.insert(0, {"$match": {"dex": protocol.database_name}})

    # get the last hypervisors status
    hypervisor_status = {
        x["data"]["address"]: x["data"]
        for x in get_from_localdb(
            network=chain.database_name,
            collection="status",
            aggregate=query_hype_status,
        )
    }

    # get hypervisors static
    hypervisor_static = {
        x["address"]: x
        for x in get_from_localdb(
            network=chain.database_name,
            collection="static",
            find=dict(dex=protocol.database_name) if protocol else None,
        )
    }

    # get a sumarized data portion for all hypervisors in the database for a period
    # when no period is specified, it will return all available data
    hypervisors_summary = get_from_localdb(
        network=chain.database_name,
        collection="operations",
        aggregate=get_default_localdb(
            network=chain.database_name
        ).query_fee_operations_summary(
            hypervisor_addresses=list(hypervisor_status.keys()),
            timestamp_ini=start_timestamp,
            timestamp_end=end_timestamp,
            block_ini=start_block,
            block_end=end_block,
        ),
    )
    # add hypervisor status to summary
    for hype_summary in hypervisors_summary:
        hype_summary["hypervisor_status"] = hypervisor_status[hype_summary["address"]]
        hype_summary["hypervisor_static"] = hypervisor_static[hype_summary["address"]]

    # return summary
    return hypervisors_summary
