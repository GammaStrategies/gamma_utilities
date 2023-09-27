from decimal import Decimal
import logging
import math

from bins.general.enums import Chain
from .objects import period_yield_data


def returns_sumary(yield_data: list[period_yield_data]) -> dict:
    """_summary_

    Args:
        yield_data (list[period_yield_data]): _description_

    Returns:
        dict: {
            total_period_seconds: seconds conforming the period,
            aggregated: sumup data field ( net result)
            fees: fees data field
            impermanent: underlying token value changes without fees collected
            rewards: rewards data field
            }

    """

    result = {
        "roi": Decimal("0"),
        "aggregated": {
            "period": {
                "percentage_yield": Decimal("0"),
                "token0_qtty": Decimal("0"),
                "token1_qtty": Decimal("0"),
                "usd_qtty": Decimal("0"),
            },
        },
        "fees": {
            "period": {
                "percentage_yield": Decimal("0"),
                "token0_qtty": Decimal("0"),
                "token1_qtty": Decimal("0"),
                "usd_qtty": Decimal("0"),
                "seconds": 0,
            },
            "year": {
                "apr": Decimal("0"),
                "apy": Decimal("0"),
            },
            "extra": {
                "token0_collected_within": Decimal("0"),
                "token1_collected_within": Decimal("0"),
            },
        },
        "impermanent": {
            "period": {
                "percentage_yield": Decimal("0"),
                "token0_qtty": Decimal("0"),
                "token1_qtty": Decimal("0"),
                "usd_qtty": Decimal("0"),
                "token0_qtty_usd": Decimal("0"),
                "token1_qtty_usd": Decimal("0"),
                "seconds": 0,
            },
        },
        "rewards": {
            "period": {
                "percentage_yield": Decimal("0"),
                "usd_qtty": Decimal("0"),
                "seconds": 0,
            },
        },
    }

    #     "chain": "celo",
    #     "address": "0x002e2a8215e892e77681e2568f85c8f720ce63db",
    #     "symbol": "xPACT-CELO3",
    #     "block": 21593485,
    #     "timestamp": 1695635990,
    #     "period": 1,
    #     "year_feeApr": 0,
    #     "year_feeApy": 0,
    #     "year_allRewards2": 0,
    #     "period_feeApr": 0,
    #     "period_rewardsApr": 0,
    #     "period_lping": 8.36195166288179e-7,
    #     "period_hodl_deposited": 0.008339392081189901,
    #     "period_hodl_fifty": 0.00942349677500146,
    #     "period_hodl_token0": 0,
    #     "period_hodl_token1": 0.01884699355000292,
    #     "period_netApr": 8.36195166288179e-7,
    #     "period_impermanentResult": 8.36195166288179e-7,
    #     "gamma_vs_hodl": -0.008269592511716772
    #   },

    # calculate totals: sumarize data
    if yield_data:
        # time var control
        seconds_yield_data = 0

        price_per_share = {
            "ini_price_per_share": yield_data[0].price_per_share,
            "end_price_per_share": yield_data[-1].price_per_share,
        }

        ini_timestamp = yield_data[0].timeframe.ini.timestamp
        end_timestamp = yield_data[-1].timeframe.end.timestamp
        ini_prices = yield_data[0].status.ini.prices
        end_prices = yield_data[-1].status.end.prices

        # deposit control var
        deposit = {
            "qtty": yield_data[0].status.ini.underlying.qtty,
            "allValue_in_token0": ini_prices.token1
            / ini_prices.token0
            * yield_data[0].status.ini.underlying.qtty.token1
            + yield_data[0].status.ini.underlying.qtty.token0,
            "allValue_in_token1": ini_prices.token0
            / ini_prices.token1
            * yield_data[0].status.ini.underlying.qtty.token0
            + yield_data[0].status.ini.underlying.qtty.token1,
        }

        for yield_item in yield_data:
            # time var control
            seconds_yield_data += yield_item.period_seconds

            # period price per share
            if yield_item.timeframe.ini.timestamp < price_per_share["ini_timestamp"]:
                # modify control vars
                ini_timestamp = yield_item.timeframe.ini.timestamp
                price_per_share["ini_price_per_share"] = yield_item.price_per_share
                ini_prices = yield_item.status.ini.prices
                deposit = {
                    "qtty": yield_item.status.ini.underlying.qtty,
                    "allValue_in_token0": ini_prices.token1
                    / ini_prices.token0
                    * yield_item.status.ini.underlying.qtty.token1
                    + yield_item.status.ini.underlying.qtty.token0,
                    "allValue_in_token1": ini_prices.token0
                    / ini_prices.token1
                    * yield_item.status.ini.underlying.qtty.token0
                    + yield_item.status.ini.underlying.qtty.token1,
                }

            if yield_item.timeframe.end.timestamp > price_per_share["end_timestamp"]:
                # modify control vars
                end_timestamp = yield_item.timeframe.end.timestamp
                price_per_share["end_price_per_share"] = yield_item.price_per_share

            if tmp := yield_item.fees.period_yield:
                if result["fees"]["period"]["percentage_yield"]:
                    result["fees"]["period"]["percentage_yield"] *= 1 + tmp
                else:
                    result["fees"]["period"]["percentage_yield"] = 1 + tmp

                # add to total period seconds
                result["fees"]["period"]["seconds"] += yield_item.period_seconds

                result["fees"]["period"][
                    "token0_qtty"
                ] += yield_item.fees.qtty.token0 or Decimal("0")
                result["fees"]["period"][
                    "token1_qtty"
                ] += yield_item.fees.qtty.token1 or Decimal("0")
                result["fees"]["period"][
                    "usd_qtty"
                ] += yield_item.period_fees_usd or Decimal("0")

            if yield_item.fees_collected_within:
                if yield_item.fees_collected_within.qtty.token0:
                    result["fees"]["extra"][
                        "token0_collected_within"
                    ] += yield_item.fees_collected_within.qtty.token0
                if yield_item.fees_collected_within.qtty.token1:
                    result["fees"]["extra"][
                        "token1_collected_within"
                    ] += yield_item.fees_collected_within.qtty.token1

            # impermanent

            result["impermanent"]["period"][
                "token0_qtty"
            ] += yield_item.period_impermanent_token0
            result["impermanent"]["period"][
                "token1_qtty"
            ] += yield_item.period_impermanent_token1
            result["impermanent"]["period"][
                "usd_qtty"
            ] += yield_item.period_impermanent_usd
            #
            result["impermanent"]["period"][
                "token0_qtty_usd"
            ] += yield_item.period_impermanent_token0_usd
            result["impermanent"]["period"][
                "token1_qtty_usd"
            ] += yield_item.period_impermanent_token1_usd

            result["impermanent"]["period"]["seconds"] += yield_item.period_seconds

            # rewards
            if tmp := yield_item.rewards.period_yield:
                if result["rewards"]["period"]["percentage_yield"]:
                    result["rewards"]["period"]["percentage_yield"] *= 1 + tmp
                else:
                    result["rewards"]["period"]["percentage_yield"] = 1 + tmp

                result["rewards"]["period"][
                    "usd_qtty"
                ] += yield_item.rewards.usd or Decimal("0")
                # add to total period seconds
                result["rewards"]["period"]["seconds"] += yield_item.period_seconds

        # calculate fees yield for the period
        if result["fees"]["period"]["percentage_yield"]:
            result["fees"]["period"]["percentage_yield"] -= 1
        if result["rewards"]["period"]["percentage_yield"]:
            result["rewards"]["period"]["percentage_yield"] -= 1
        # calculate impermanent yield for the period
        try:
            result["impermanent"]["period"]["percentage_yield"] = result["impermanent"][
                "period"
            ]["usd_qtty"] / (
                result["fees"]["period"]["usd_qtty"]
                / result["fees"]["period"]["percentage_yield"]
            )
        except Exception as e:
            # logging.getLogger(__name__).error("  cant calc impermanent yield ...%s", e)
            result["impermanent"]["period"]["percentage_yield"] = Decimal("0")

        # calculate net yield of the period
        result["aggregated"]["period"]["percentage_yield"] = (
            result["fees"]["period"]["percentage_yield"]
            + result["impermanent"]["period"]["percentage_yield"]
            + result["rewards"]["period"]["percentage_yield"]
        )

        result["aggregated"]["period"]["token0_qtty"] = (
            result["fees"]["period"]["token0_qtty"]
            + result["impermanent"]["period"]["token0_qtty"]
        )
        result["aggregated"]["period"]["token1_qtty"] = (
            result["fees"]["period"]["token1_qtty"]
            + result["impermanent"]["period"]["token1_qtty"]
        )
        # usd is the only combined measure
        result["aggregated"]["period"]["usd_qtty"] = (
            result["fees"]["period"]["usd_qtty"]
            + result["impermanent"]["period"]["usd_qtty"]
            + result["rewards"]["period"]["usd_qtty"]
        )

        # calculate yearly extrapolation yield
        day_in_seconds = 60 * 60 * 24
        year_in_seconds = day_in_seconds * 365

        result["fees"]["year"]["apr"] = (
            result["fees"]["period"]["percentage_yield"]
            * (
                (Decimal(str(year_in_seconds)))
                / Decimal(str(result["fees"]["period"]["seconds"]))
            )
            if result["fees"]["period"]["seconds"]
            else Decimal("0")
        )
        try:
            result["fees"]["year"]["apy"] = (
                (
                    (
                        1
                        + result["fees"]["period"]["percentage_yield"]
                        * (
                            (Decimal(str(day_in_seconds)))
                            / Decimal(str(result["fees"]["period"]["seconds"]))
                        )
                    )
                    ** Decimal("365")
                    - Decimal("1")
                )
                if result["fees"]["period"]["seconds"]
                else Decimal("0")
            )
        except OverflowError as e:
            logging.getLogger(__name__).debug(f"  cant calc apy Overflow err ...{e}")

    try:
        result["roi"] = (
            price_per_share["end_price_per_share"]
            - price_per_share["ini_price_per_share"]
        ) / price_per_share["ini_price_per_share"]
    except Exception as e:
        pass
    # result
    return result


def chart_period(yield_data: list[period_yield_data]) -> list[dict]:
    """Create a list of points in a chart representing the period yield evolution in time

    Args:
        yield_data (list[period_yield_data]):

    """

    #     "chain": "celo",
    #     "address": "0x002e2a8215e892e77681e2568f85c8f720ce63db",
    #     "symbol": "xPACT-CELO3",
    #     "block": 21593485,
    #     "timestamp": 1695635990,
    #     "period": 1,
    #     "year_feeApr": 0,
    #     "year_feeApy": 0,
    #     "year_allRewards2": 0,
    #     "period_feeApr": 0,
    #     "period_rewardsApr": 0,
    #     "period_lping": 8.36195166288179e-7,
    #     "period_hodl_deposited": 0.008339392081189901,
    #     "period_hodl_fifty": 0.00942349677500146,
    #     "period_hodl_token0": 0,
    #     "period_hodl_token1": 0.01884699355000292,
    #     "period_netApr": 8.36195166288179e-7,
    #     "period_impermanentResult": 8.36195166288179e-7,
    #     "gamma_vs_hodl": -0.008269592511716772
    #   },

    # calculate totals: sumarize data
    if yield_data:
        # time var control
        seconds_yield_data = 0

        price_per_share = {
            "ini_price_per_share": yield_data[0].price_per_share,
            "end_price_per_share": yield_data[-1].price_per_share,
        }

        ini_timestamp = yield_data[0].timeframe.ini.timestamp
        end_timestamp = yield_data[-1].timeframe.end.timestamp
        ini_prices = yield_data[0].status.ini.prices
        end_prices = yield_data[-1].status.end.prices

        # deposit control var
        deposit = {
            "qtty": yield_data[0].status.ini.underlying.qtty,
            "allValue_in_token0": ini_prices.token1
            / ini_prices.token0
            * yield_data[0].status.ini.underlying.qtty.token1
            + yield_data[0].status.ini.underlying.qtty.token0,
            "allValue_in_token1": ini_prices.token0
            / ini_prices.token1
            * yield_data[0].status.ini.underlying.qtty.token0
            + yield_data[0].status.ini.underlying.qtty.token1,
        }

    #     for yield_item in yield_data:
    #         # time var control
    #         seconds_yield_data += yield_item.period_seconds

    #         # period price per share
    #         if yield_item.timeframe.ini.timestamp < price_per_share["ini_timestamp"]:
    #             # modify control vars
    #             ini_timestamp = yield_item.timeframe.ini.timestamp
    #             price_per_share["ini_price_per_share"] = yield_item.price_per_share
    #             ini_prices = yield_item.status.ini.prices
    #             deposit = {
    #                 "qtty": yield_item.status.ini.underlying.qtty,
    #                 "allValue_in_token0": ini_prices.token1
    #                 / ini_prices.token0
    #                 * yield_item.status.ini.underlying.qtty.token1
    #                 + yield_item.status.ini.underlying.qtty.token0,
    #                 "allValue_in_token1": ini_prices.token0
    #                 / ini_prices.token1
    #                 * yield_item.status.ini.underlying.qtty.token0
    #                 + yield_item.status.ini.underlying.qtty.token1,
    #             }

    #         if yield_item.timeframe.end.timestamp > price_per_share["end_timestamp"]:
    #             # modify control vars
    #             end_timestamp = yield_item.timeframe.end.timestamp
    #             price_per_share["end_price_per_share"] = yield_item.price_per_share

    #         if tmp := yield_item.fees.period_yield:
    #             if result["fees"]["period"]["percentage_yield"]:
    #                 result["fees"]["period"]["percentage_yield"] *= 1 + tmp
    #             else:
    #                 result["fees"]["period"]["percentage_yield"] = 1 + tmp

    #             # add to total period seconds
    #             result["fees"]["period"]["seconds"] += yield_item.period_seconds

    #             result["fees"]["period"][
    #                 "token0_qtty"
    #             ] += yield_item.fees.qtty.token0 or Decimal("0")
    #             result["fees"]["period"][
    #                 "token1_qtty"
    #             ] += yield_item.fees.qtty.token1 or Decimal("0")
    #             result["fees"]["period"][
    #                 "usd_qtty"
    #             ] += yield_item.period_fees_usd or Decimal("0")

    #         if yield_item.fees_collected_within:
    #             if yield_item.fees_collected_within.qtty.token0:
    #                 result["fees"]["extra"][
    #                     "token0_collected_within"
    #                 ] += yield_item.fees_collected_within.qtty.token0
    #             if yield_item.fees_collected_within.qtty.token1:
    #                 result["fees"]["extra"][
    #                     "token1_collected_within"
    #                 ] += yield_item.fees_collected_within.qtty.token1

    #         # impermanent

    #         result["impermanent"]["period"][
    #             "token0_qtty"
    #         ] += yield_item.period_impermanent_token0
    #         result["impermanent"]["period"][
    #             "token1_qtty"
    #         ] += yield_item.period_impermanent_token1
    #         result["impermanent"]["period"][
    #             "usd_qtty"
    #         ] += yield_item.period_impermanent_usd
    #         #
    #         result["impermanent"]["period"][
    #             "token0_qtty_usd"
    #         ] += yield_item.period_impermanent_token0_usd
    #         result["impermanent"]["period"][
    #             "token1_qtty_usd"
    #         ] += yield_item.period_impermanent_token1_usd

    #         result["impermanent"]["period"]["seconds"] += yield_item.period_seconds

    #         # rewards
    #         if tmp := yield_item.rewards.period_yield:
    #             if result["rewards"]["period"]["percentage_yield"]:
    #                 result["rewards"]["period"]["percentage_yield"] *= 1 + tmp
    #             else:
    #                 result["rewards"]["period"]["percentage_yield"] = 1 + tmp

    #             result["rewards"]["period"][
    #                 "usd_qtty"
    #             ] += yield_item.rewards.usd or Decimal("0")
    #             # add to total period seconds
    #             result["rewards"]["period"]["seconds"] += yield_item.period_seconds

    #     # calculate fees yield for the period
    #     if result["fees"]["period"]["percentage_yield"]:
    #         result["fees"]["period"]["percentage_yield"] -= 1
    #     if result["rewards"]["period"]["percentage_yield"]:
    #         result["rewards"]["period"]["percentage_yield"] -= 1
    #     # calculate impermanent yield for the period
    #     try:
    #         result["impermanent"]["period"]["percentage_yield"] = result["impermanent"][
    #             "period"
    #         ]["usd_qtty"] / (
    #             result["fees"]["period"]["usd_qtty"]
    #             / result["fees"]["period"]["percentage_yield"]
    #         )
    #     except Exception as e:
    #         # logging.getLogger(__name__).error("  cant calc impermanent yield ...%s", e)
    #         result["impermanent"]["period"]["percentage_yield"] = Decimal("0")

    #     # calculate net yield of the period
    #     result["aggregated"]["period"]["percentage_yield"] = (
    #         result["fees"]["period"]["percentage_yield"]
    #         + result["impermanent"]["period"]["percentage_yield"]
    #         + result["rewards"]["period"]["percentage_yield"]
    #     )

    #     result["aggregated"]["period"]["token0_qtty"] = (
    #         result["fees"]["period"]["token0_qtty"]
    #         + result["impermanent"]["period"]["token0_qtty"]
    #     )
    #     result["aggregated"]["period"]["token1_qtty"] = (
    #         result["fees"]["period"]["token1_qtty"]
    #         + result["impermanent"]["period"]["token1_qtty"]
    #     )
    #     # usd is the only combined measure
    #     result["aggregated"]["period"]["usd_qtty"] = (
    #         result["fees"]["period"]["usd_qtty"]
    #         + result["impermanent"]["period"]["usd_qtty"]
    #         + result["rewards"]["period"]["usd_qtty"]
    #     )

    #     # calculate yearly extrapolation yield
    #     day_in_seconds = 60 * 60 * 24
    #     year_in_seconds = day_in_seconds * 365

    #     result["fees"]["year"]["apr"] = (
    #         result["fees"]["period"]["percentage_yield"]
    #         * (
    #             (Decimal(str(year_in_seconds)))
    #             / Decimal(str(result["fees"]["period"]["seconds"]))
    #         )
    #         if result["fees"]["period"]["seconds"]
    #         else Decimal("0")
    #     )
    #     try:
    #         result["fees"]["year"]["apy"] = (
    #             (
    #                 (
    #                     1
    #                     + result["fees"]["period"]["percentage_yield"]
    #                     * (
    #                         (Decimal(str(day_in_seconds)))
    #                         / Decimal(str(result["fees"]["period"]["seconds"]))
    #                     )
    #                 )
    #                 ** Decimal("365")
    #                 - Decimal("1")
    #             )
    #             if result["fees"]["period"]["seconds"]
    #             else Decimal("0")
    #         )
    #     except OverflowError as e:
    #         logging.getLogger(__name__).debug(f"  cant calc apy Overflow err ...{e}")

    # try:
    #     result["roi"] = (
    #         price_per_share["end_price_per_share"]
    #         - price_per_share["ini_price_per_share"]
    #     ) / price_per_share["ini_price_per_share"]
    # except Exception as e:
    #     pass
    # # result
    # return result


def log_summary(summary: dict, hype_static: dict, chain: Chain) -> None:
    """log summary data

    Args:
        summary (dict):
    """

    # log results

    logging.getLogger(__name__).info(
        f" {chain.fantasy_name}'s {hype_static['dex']} {hype_static['symbol']} returns:  [{hype_static['address']}]"
    )

    logging.getLogger(__name__).info(
        f"       period fees : {summary['fees']['period']['percentage_yield']:,.2%}  [ {summary['fees']['period']['token0_qtty']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['fees']['period']['token1_qtty']:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {summary['fees']['period']['usd_qtty']:,.2f} USD]  [total days: {summary['fees']['period']['seconds']/(60*60*24):,.1f} ]  [APR: {summary['fees']['year']['apr']:,.2%}  APY: {summary['fees']['year']['apy']:,.2%}]"
    )
    logging.getLogger(__name__).info(
        f"    period rewards : {summary['rewards']['period']['percentage_yield']:,.2%}  [ {summary['rewards']['period']['usd_qtty']:,.2f} USD]  [data total days: {summary['rewards']['period']['seconds']/(60*60*24):,.1f} ]"
    )
    logging.getLogger(__name__).info(
        f"period impermanent : {summary['impermanent']['period']['percentage_yield']:,.2%}  [ {summary['impermanent']['period']['token0_qtty']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['impermanent']['period']['token1_qtty']:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {summary['impermanent']['period']['usd_qtty']:,.2f} USD]  [data total days: {summary['impermanent']['period']['seconds']/(60*60*24):,.1f} ]"
    )

    # impermanent loss

    if (
        summary["impermanent"]["period"]["token0_qtty"] > 0
        and summary["impermanent"]["period"]["token1_qtty"] < 0
    ):
        logging.getLogger(__name__).info(
            f"       {hype_static['pool']['token0']['symbol']} buy price : {abs(summary['impermanent']['period']['token0_qtty_usd']/summary['impermanent']['period']['token0_qtty']):,.2f} USD"
        )
        logging.getLogger(__name__).info(
            f"       {hype_static['pool']['token1']['symbol']} sell price : {abs(summary['impermanent']['period']['token1_qtty_usd']/summary['impermanent']['period']['token1_qtty']):,.2f} USD"
        )
    elif (
        summary["impermanent"]["period"]["token1_qtty"] > 0
        and summary["impermanent"]["period"]["token0_qtty"] < 0
    ):
        logging.getLogger(__name__).info(
            f"       {hype_static['pool']['token0']['symbol']} sell price : {abs(summary['impermanent']['period']['token0_qtty_usd']/summary['impermanent']['period']['token0_qtty']):,.2f} USD"
        )
        logging.getLogger(__name__).info(
            f"       {hype_static['pool']['token1']['symbol']} buy price : {abs(summary['impermanent']['period']['token1_qtty_usd']/summary['impermanent']['period']['token1_qtty']):,.2f} USD"
        )
    else:
        if (
            summary["impermanent"]["period"]["token0_qtty"]
            and summary["impermanent"]["period"]["token1_qtty"]
        ):
            logging.getLogger(__name__).info(
                f"       panacea ... no impermanent loss? "
            )

    logging.getLogger(__name__).info(
        f" --> period net yield: {summary['aggregated']['period']['percentage_yield']:,.2%}  [ {summary['aggregated']['period']['token0_qtty']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['aggregated']['period']['token1_qtty']:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {summary['aggregated']['period']['usd_qtty']:,.2f} USD]"
    )

    # log roi
    logging.getLogger(__name__).info(f" --> period ROI: {summary['roi']:,.2%}")

    # logging.getLogger(__name__).info(
    #    f"cntrol fees collected: {summary['fees']['extra']['token0_collected_within']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['fees']['extra']['token1_collected_within']:,.2f} {hype_static['pool']['token1']['symbol']} "
    # )


def log_summary_web(summary: dict, hype_static: dict, chain: Chain) -> None:
    ini_status = summary[0]["status"]["ini"]
    end_status = summary[-1]["status"]["end"]

    # prices
    ini_prices = ini_status["prices"]
    end_prices = end_status["prices"]
    # value per share
    ini_vps = summary[0]["price_per_share"]
    end_vps = summary[-1]["price_per_share"]

    # LOG

    logging.getLogger(__name__).info(
        f" {chain.fantasy_name}'s {hype_static['dex']} {hype_static['symbol']} returns:  [{hype_static['address']}]"
    )
