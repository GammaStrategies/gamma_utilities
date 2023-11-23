from decimal import Decimal
import logging
import math

from bins.general.enums import Chain
from .objects import period_yield_data


def returns_sumary_oooo(
    yield_data: list[period_yield_data], feeType: str = "lps"
) -> dict:
    """_summary_

    Args:
        yield_data (list[period_yield_data]): _description_
        feeType (str, optional): Choose between all, gamma or lps . Defaults to "lps".
            The fee to be used in the summary

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
                "percentage_yield": Decimal("0"),  # in usd
                "token0_qtty": Decimal("0"),
                "token1_qtty": Decimal("0"),
                "usd_qtty": Decimal("0"),
            },
            "extra": {
                "comparison": {
                    "prices": {
                        "ini_token0": Decimal("0"),
                        "ini_token1": Decimal("0"),
                        "end_token0": Decimal("0"),
                        "end_token1": Decimal("0"),
                        "ini_share": Decimal("0"),
                        "end_share": Decimal("0"),
                    },
                    "deposit": {
                        "token0_qtty": Decimal("0"),
                        "token1_qtty": Decimal("0"),
                        "usd_qtty": Decimal("0"),
                        "allValue_in_token0": Decimal("0"),
                        "allValue_in_token1": Decimal("0"),
                        "fifty_token0_qtty": Decimal("0"),
                        "fifty_token1_qtty": Decimal("0"),
                    },
                    "supply": {
                        "ini": Decimal("0"),
                        "end": Decimal("0"),
                    },
                },
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
        ini_supply = yield_data[0].status.ini.supply
        end_supply = yield_data[-1].status.end.supply

        _current_total_value = (
            ini_prices.token0 * yield_data[0].status.ini.underlying.qtty.token0
            + ini_prices.token1 * yield_data[0].status.ini.underlying.qtty.token1
        )

        # deposit control var
        deposit = {
            # quantity of token0 and token1 deposited ( balanced as per the pool ratio at that time)
            "qtty": yield_data[0].status.ini.underlying.qtty,
            "usd_qtty": _current_total_value,
            # quantity of token 0 deposited ( includes token1 converted to token0)
            "allValue_in_token0": ini_prices.token1
            / ini_prices.token0
            * yield_data[0].status.ini.underlying.qtty.token1
            + yield_data[0].status.ini.underlying.qtty.token0,
            # quantity of token 1 deposited ( includes token0 converted to token1)
            "allValue_in_token1": ini_prices.token0
            / ini_prices.token1
            * yield_data[0].status.ini.underlying.qtty.token0
            + yield_data[0].status.ini.underlying.qtty.token1,
            # quantity of 50% token0 and 50% token1 deposited
            "fifty_token0_qtty": (_current_total_value * Decimal("0.5"))
            / ini_prices.token0,
            "fifty_token1_qtty": (_current_total_value * Decimal("0.5"))
            / ini_prices.token1,
        }

        for yield_item in yield_data:
            # time var control
            seconds_yield_data += yield_item.period_seconds

            # period price per share
            if yield_item.timeframe.ini.timestamp < ini_timestamp:
                # modify control vars
                ini_timestamp = yield_item.timeframe.ini.timestamp
                price_per_share["ini_price_per_share"] = yield_item.price_per_share
                ini_prices = yield_item.status.ini.prices
                _current_total_value = (
                    ini_prices.token0 * yield_item.status.ini.underlying.qtty.token0
                    + ini_prices.token1 * yield_item.status.ini.underlying.qtty.token1
                )
                ini_supply = yield_item.status.ini.supply
                deposit["qtty"] = yield_item.status.ini.underlying.qtty
                deposit["usd_qtty"] = _current_total_value
                deposit["allValue_in_token0"] = (
                    ini_prices.token1
                    / ini_prices.token0
                    * yield_item.status.ini.underlying.qtty.token1
                    + yield_item.status.ini.underlying.qtty.token0
                )
                deposit["allValue_in_token1"] = (
                    ini_prices.token0
                    / ini_prices.token1
                    * yield_item.status.ini.underlying.qtty.token0
                    + yield_item.status.ini.underlying.qtty.token1
                )
                deposit["fifty_token0_qtty"] = (
                    _current_total_value * Decimal("0.5")
                ) / ini_prices.token0
                deposit["fifty_token1_qtty"] = (
                    _current_total_value * Decimal("0.5")
                ) / ini_prices.token1

            if yield_item.timeframe.end.timestamp > end_timestamp:
                # modify control vars
                end_timestamp = yield_item.timeframe.end.timestamp
                price_per_share["end_price_per_share"] = yield_item.price_per_share
                end_prices = yield_item.status.end.prices
                end_supply = yield_item.status.end.supply

            if feeType == "all":
                tmp = yield_item.fees
            elif feeType == "gamma":
                tmp = yield_item.fees_gamma
            elif feeType == "lps":
                tmp = yield_item.fees

            if tmp:
                if result["fees"]["period"]["percentage_yield"]:
                    result["fees"]["period"]["percentage_yield"] *= 1 + tmp.period_yield
                else:
                    result["fees"]["period"]["percentage_yield"] = 1 + tmp.period_yield

                # add to total period seconds
                result["fees"]["period"]["seconds"] += yield_item.period_seconds

                # add to total period qtty
                result["fees"]["period"]["token0_qtty"] += tmp.qtty.token0 or Decimal(
                    "0"
                )
                result["fees"]["period"]["token1_qtty"] += tmp.qtty.token1 or Decimal(
                    "0"
                )

                result["fees"]["period"]["usd_qtty"] += (
                    tmp.qtty.token0 * yield_item.status.end.prices.token0
                    + tmp.qtty.token1 * yield_item.status.end.prices.token1
                ) or Decimal("0")

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

        # fill comparison data
        result["aggregated"]["extra"]["comparison"]["deposit"]["token0_qtty"] = deposit[
            "qtty"
        ].token0
        result["aggregated"]["extra"]["comparison"]["deposit"]["token1_qtty"] = deposit[
            "qtty"
        ].token1
        result["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"] = deposit[
            "usd_qtty"
        ]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "allValue_in_token0"
        ] = deposit["allValue_in_token0"]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "allValue_in_token1"
        ] = deposit["allValue_in_token1"]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "fifty_token0_qtty"
        ] = deposit["fifty_token0_qtty"]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "fifty_token1_qtty"
        ] = deposit["fifty_token1_qtty"]

        result["aggregated"]["extra"]["comparison"]["prices"][
            "ini_token0"
        ] = ini_prices.token0
        result["aggregated"]["extra"]["comparison"]["prices"][
            "ini_token1"
        ] = ini_prices.token1

        result["aggregated"]["extra"]["comparison"]["prices"][
            "end_token0"
        ] = end_prices.token0
        result["aggregated"]["extra"]["comparison"]["prices"][
            "end_token1"
        ] = end_prices.token1
        result["aggregated"]["extra"]["comparison"]["prices"][
            "ini_share"
        ] = price_per_share["ini_price_per_share"]
        result["aggregated"]["extra"]["comparison"]["prices"][
            "end_share"
        ] = price_per_share["end_price_per_share"]

        result["aggregated"]["extra"]["comparison"]["supply"]["ini"] = ini_supply
        result["aggregated"]["extra"]["comparison"]["supply"]["end"] = end_supply

    try:
        result["roi"] = (
            price_per_share["end_price_per_share"]
            - price_per_share["ini_price_per_share"]
        ) / price_per_share["ini_price_per_share"]

    except Exception as e:
        pass
    # result
    return result


def returns_sumary(yield_data: list[period_yield_data], feeType: str = "lps") -> dict:
    """

    Args:
        yield_data (list[period_yield_data]): _description_
        feeType (str, optional): Choose between all, gamma or lps . Defaults to "lps".
            The fee to be used in the summary

    Returns:
        dict: {

            }

    """

    result = {
        "roi": Decimal("0"),
        "aggregated": {
            "period": {
                "percentage_yield": Decimal("0"),  # in usd
                "token0_qtty": Decimal("0"),
                "token1_qtty": Decimal("0"),
                "usd_qtty": Decimal("0"),
                # x share
                "per_share_usd": Decimal("0"),
                "per_share_yield": Decimal("0"),
            },
            "extra": {
                "comparison": {
                    "prices": {
                        "ini_token0": Decimal("0"),
                        "ini_token1": Decimal("0"),
                        "end_token0": Decimal("0"),
                        "end_token1": Decimal("0"),
                        "ini_share": Decimal("0"),
                        "end_share": Decimal("0"),
                    },
                    "deposit": {
                        "token0_qtty": Decimal("0"),
                        "token1_qtty": Decimal("0"),
                        "usd_qtty": Decimal("0"),
                        "allValue_in_token0": Decimal("0"),
                        "allValue_in_token1": Decimal("0"),
                        "fifty_token0_qtty": Decimal("0"),
                        "fifty_token1_qtty": Decimal("0"),
                    },
                    "supply": {
                        "ini": Decimal("0"),
                        "end": Decimal("0"),
                    },
                },
            },
        },
        "fees": {
            "period": {
                "percentage_yield": Decimal("0"),
                "token0_qtty": Decimal("0"),
                "token1_qtty": Decimal("0"),
                "usd_qtty": Decimal("0"),
                "seconds": 0,
                # x share
                "per_share_usd": Decimal("0"),
                "per_share_yield": Decimal("0"),
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
                # x share
                "per_share_usd": Decimal("0"),
                "per_share_yield": Decimal("0"),
            },
        },
        "rewards": {
            "period": {
                "percentage_yield": Decimal("0"),
                "usd_qtty": Decimal("0"),
                "seconds": 0,
                # x share
                "per_share_usd": Decimal("0"),
                "per_share_yield": Decimal("0"),
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
        ini_supply = yield_data[0].status.ini.supply
        end_supply = yield_data[-1].status.end.supply

        _current_total_value = (
            ini_prices.token0 * yield_data[0].status.ini.underlying.qtty.token0
            + ini_prices.token1 * yield_data[0].status.ini.underlying.qtty.token1
        )

        # deposit control var
        deposit = {
            # quantity of token0 and token1 deposited ( balanced as per the pool ratio at that time)
            "qtty": yield_data[0].status.ini.underlying.qtty,
            "usd_qtty": _current_total_value,
            # quantity of token 0 deposited ( includes token1 converted to token0)
            "allValue_in_token0": ini_prices.token1
            / ini_prices.token0
            * yield_data[0].status.ini.underlying.qtty.token1
            + yield_data[0].status.ini.underlying.qtty.token0,
            # quantity of token 1 deposited ( includes token0 converted to token1)
            "allValue_in_token1": ini_prices.token0
            / ini_prices.token1
            * yield_data[0].status.ini.underlying.qtty.token0
            + yield_data[0].status.ini.underlying.qtty.token1,
            # quantity of 50% token0 and 50% token1 deposited
            "fifty_token0_qtty": (_current_total_value * Decimal("0.5"))
            / ini_prices.token0,
            "fifty_token1_qtty": (_current_total_value * Decimal("0.5"))
            / ini_prices.token1,
        }

        # logging.getLogger("benchmark").info(
        #     f" fees   \t    rewards   \t   impermanent   \t   ( price per share ini -> end USD ) [ difference to price share ini]"
        # )
        for yield_item in yield_data:
            # time var control
            seconds_yield_data += yield_item.period_seconds

            # period price per share
            if yield_item.timeframe.ini.timestamp < ini_timestamp:
                # modify control vars
                ini_timestamp = yield_item.timeframe.ini.timestamp
                price_per_share["ini_price_per_share"] = yield_item.price_per_share
                ini_prices = yield_item.status.ini.prices
                _current_total_value = (
                    ini_prices.token0 * yield_item.status.ini.underlying.qtty.token0
                    + ini_prices.token1 * yield_item.status.ini.underlying.qtty.token1
                )
                ini_supply = yield_item.status.ini.supply
                deposit["qtty"] = yield_item.status.ini.underlying.qtty
                deposit["usd_qtty"] = _current_total_value
                deposit["allValue_in_token0"] = (
                    ini_prices.token1
                    / ini_prices.token0
                    * yield_item.status.ini.underlying.qtty.token1
                    + yield_item.status.ini.underlying.qtty.token0
                )
                deposit["allValue_in_token1"] = (
                    ini_prices.token0
                    / ini_prices.token1
                    * yield_item.status.ini.underlying.qtty.token0
                    + yield_item.status.ini.underlying.qtty.token1
                )
                deposit["fifty_token0_qtty"] = (
                    _current_total_value * Decimal("0.5")
                ) / ini_prices.token0
                deposit["fifty_token1_qtty"] = (
                    _current_total_value * Decimal("0.5")
                ) / ini_prices.token1

            if yield_item.timeframe.end.timestamp > end_timestamp:
                # modify control vars
                end_timestamp = yield_item.timeframe.end.timestamp
                price_per_share["end_price_per_share"] = yield_item.price_per_share
                end_prices = yield_item.status.end.prices
                end_supply = yield_item.status.end.supply

            if feeType == "all":
                tmp = yield_item.fees
            elif feeType == "gamma":
                tmp = yield_item.fees_gamma
            elif feeType == "lps":
                tmp = yield_item.fees

            if tmp:
                if result["fees"]["period"]["percentage_yield"]:
                    result["fees"]["period"]["percentage_yield"] *= 1 + tmp.period_yield
                else:
                    result["fees"]["period"]["percentage_yield"] = 1 + tmp.period_yield

                # add to total period seconds
                result["fees"]["period"]["seconds"] += yield_item.period_seconds

                # add to total period qtty
                result["fees"]["period"]["token0_qtty"] += tmp.qtty.token0 or Decimal(
                    "0"
                )
                result["fees"]["period"]["token1_qtty"] += tmp.qtty.token1 or Decimal(
                    "0"
                )

                result["fees"]["period"]["usd_qtty"] += (
                    tmp.qtty.token0 * yield_item.status.end.prices.token0
                    + tmp.qtty.token1 * yield_item.status.end.prices.token1
                ) or Decimal("0")

                # per share
                result["fees"]["period"]["per_share_usd"] += yield_item.fees_per_share
                if result["fees"]["period"]["per_share_yield"]:
                    result["fees"]["period"]["per_share_yield"] *= (
                        1 + yield_item.fees_per_share_percentage_yield
                    )
                else:
                    result["fees"]["period"]["per_share_yield"] = (
                        1 + yield_item.fees_per_share_percentage_yield
                    )

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

            result["impermanent"]["period"]["token0_qtty"] += (
                yield_item.period_impermanent_token0
                + yield_item.rebalance_divergence.token0
            )
            result["impermanent"]["period"]["token1_qtty"] += (
                yield_item.period_impermanent_token1
                + yield_item.rebalance_divergence.token1
            )
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

            # per share
            result["impermanent"]["period"][
                "per_share_usd"
            ] += yield_item.impermanent_per_share
            if result["impermanent"]["period"]["per_share_yield"]:
                result["impermanent"]["period"]["per_share_yield"] *= (
                    1 + yield_item.impermanent_per_share_percentage_yield
                )
            else:
                result["impermanent"]["period"]["per_share_yield"] = (
                    1 + yield_item.impermanent_per_share_percentage_yield
                )

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

                # per share
                result["rewards"]["period"][
                    "per_share_usd"
                ] += yield_item.rewards_per_share
                if result["rewards"]["period"]["per_share_yield"]:
                    result["rewards"]["period"]["per_share_yield"] *= (
                        1 + yield_item.rewards_per_share_percentage_yield
                    )
                else:
                    result["rewards"]["period"]["per_share_yield"] = (
                        1 + yield_item.rewards_per_share_percentage_yield
                    )

            # # XPERIMENT
            # _share_price_difference = (
            #     yield_item.price_per_share - price_per_share["ini_price_per_share"]
            # )

            # logging.getLogger("benchmark").info(
            #     f" {result['fees']['period']['per_share_usd']:,.2f} [{result['fees']['period']['per_share_yield']:,.2%}] \t {result['rewards']['period']['per_share_usd']:,.2f} [{result['rewards']['period']['per_share_yield']:,.2%}] \t {result['impermanent']['period']['per_share_usd']:,.2f} [{result['impermanent']['period']['per_share_yield']:,.2%}]    ( {yield_item.price_per_share_at_ini:,.2f} -> {yield_item.price_per_share:,.2f} ) [ {_share_price_difference:,.2f}]  {yield_item.status.ini.underlying.qtty.token0}->{yield_item.status.end.underlying.qtty.token0} {yield_item.status.ini.underlying.qtty.token1}->{yield_item.status.end.underlying.qtty.token1}"
            # )

        # calculate fees yield for the period
        if result["fees"]["period"]["percentage_yield"]:
            result["fees"]["period"]["percentage_yield"] -= 1
        if result["rewards"]["period"]["percentage_yield"]:
            result["rewards"]["period"]["percentage_yield"] -= 1

        # per share
        if result["fees"]["period"]["per_share_yield"]:
            result["fees"]["period"]["per_share_yield"] -= 1
        if result["impermanent"]["period"]["per_share_yield"]:
            result["impermanent"]["period"]["per_share_yield"] -= 1
        if result["rewards"]["period"]["per_share_yield"]:
            result["rewards"]["period"]["per_share_yield"] -= 1

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

        # result["impermanent"]["period"]["per_share_yield"] = (
        #     result["impermanent"]["period"]["per_share_usd"]
        #     / price_per_share["ini_price_per_share"]
        #     if price_per_share["ini_price_per_share"]
        #     else Decimal("0")
        # )

        # # rewrds share yield
        # result["rewards"]["period"]["per_share_yield"] = (
        #     result["rewards"]["period"]["per_share_usd"]
        #     / price_per_share["ini_price_per_share"]
        #     if price_per_share["ini_price_per_share"]
        #     else Decimal("0")
        # )

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

        result["aggregated"]["period"]["per_share_usd"] = (
            result["fees"]["period"]["per_share_usd"]
            + result["impermanent"]["period"]["per_share_usd"]
            + result["rewards"]["period"]["per_share_usd"]
        )
        result["aggregated"]["period"]["per_share_yield"] = (
            result["fees"]["period"]["per_share_yield"]
            + result["impermanent"]["period"]["per_share_yield"]
            + result["rewards"]["period"]["per_share_yield"]
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

        # fill comparison data
        result["aggregated"]["extra"]["comparison"]["deposit"]["token0_qtty"] = deposit[
            "qtty"
        ].token0
        result["aggregated"]["extra"]["comparison"]["deposit"]["token1_qtty"] = deposit[
            "qtty"
        ].token1
        result["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"] = deposit[
            "usd_qtty"
        ]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "allValue_in_token0"
        ] = deposit["allValue_in_token0"]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "allValue_in_token1"
        ] = deposit["allValue_in_token1"]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "fifty_token0_qtty"
        ] = deposit["fifty_token0_qtty"]
        result["aggregated"]["extra"]["comparison"]["deposit"][
            "fifty_token1_qtty"
        ] = deposit["fifty_token1_qtty"]

        result["aggregated"]["extra"]["comparison"]["prices"][
            "ini_token0"
        ] = ini_prices.token0
        result["aggregated"]["extra"]["comparison"]["prices"][
            "ini_token1"
        ] = ini_prices.token1

        result["aggregated"]["extra"]["comparison"]["prices"][
            "end_token0"
        ] = end_prices.token0
        result["aggregated"]["extra"]["comparison"]["prices"][
            "end_token1"
        ] = end_prices.token1
        result["aggregated"]["extra"]["comparison"]["prices"][
            "ini_share"
        ] = price_per_share["ini_price_per_share"]
        result["aggregated"]["extra"]["comparison"]["prices"][
            "end_share"
        ] = price_per_share["end_price_per_share"]

        result["aggregated"]["extra"]["comparison"]["supply"]["ini"] = ini_supply
        result["aggregated"]["extra"]["comparison"]["supply"]["end"] = end_supply

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


def log_summary(summary: dict, hype_static: dict, chain: Chain, feeType: str) -> None:
    """log summary data

    Args:
        summary (dict):
    """

    # log results
    logging.getLogger("analysis").info(
        f" {chain.fantasy_name}'s {hype_static['dex']} {hype_static['symbol']} returns:  [{hype_static['address']}]  -> fee type: {feeType}"
    )

    # calculated denominator
    _deposit_weight_token0 = (
        (
            summary["aggregated"]["extra"]["comparison"]["deposit"]["token0_qtty"]
            * summary["aggregated"]["extra"]["comparison"]["prices"]["ini_token0"]
        )
        / summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        if summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        else Decimal("0")
    )
    _deposit_weight_token1 = (
        (
            summary["aggregated"]["extra"]["comparison"]["deposit"]["token1_qtty"]
            * summary["aggregated"]["extra"]["comparison"]["prices"]["ini_token1"]
        )
        / summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        if summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        else Decimal("0")
    )
    _denominator_qtty_usd = (
        summary["fees"]["period"]["usd_qtty"]
        / summary["fees"]["period"]["percentage_yield"]
        if summary["fees"]["period"]["percentage_yield"]
        else Decimal("0")
    )
    _denominator_qtty_token0 = (
        (_denominator_qtty_usd * _deposit_weight_token0)
        / summary["aggregated"]["extra"]["comparison"]["prices"]["ini_token0"]
        if summary["aggregated"]["extra"]["comparison"]["prices"]["ini_token0"]
        else Decimal("0")
    )
    _denominator_qtty_token1 = (
        (_denominator_qtty_usd * _deposit_weight_token1)
        / summary["aggregated"]["extra"]["comparison"]["prices"]["ini_token1"]
        if summary["aggregated"]["extra"]["comparison"]["prices"]["ini_token1"]
        else Decimal("0")
    )
    logging.getLogger("analysis").info(
        f"    denominator values:  {_denominator_qtty_token0:,.2f} {hype_static['pool']['token0']['symbol']}    {_denominator_qtty_token1:,.2f} {hype_static['pool']['token1']['symbol']}   [ {_denominator_qtty_usd:,.2f} USD]"
    )

    logging.getLogger("analysis").info(
        f"    {feeType} period fees : {summary['fees']['period']['percentage_yield']:,.2%}  [ {summary['fees']['period']['token0_qtty']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['fees']['period']['token1_qtty']:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {summary['fees']['period']['usd_qtty']:,.2f} USD]  [total days: {summary['fees']['period']['seconds']/(60*60*24):,.1f} ]  [APR: {summary['fees']['year']['apr']:,.2%}  APY: {summary['fees']['year']['apy']:,.2%}]"
    )
    logging.getLogger("analysis").info(
        f"     period rewards : {summary['rewards']['period']['percentage_yield']:,.2%}  [ {summary['rewards']['period']['usd_qtty']:,.2f} USD]  [data total days: {summary['rewards']['period']['seconds']/(60*60*24):,.1f} ]"
    )
    logging.getLogger("analysis").info(
        f" period impermanent : {summary['impermanent']['period']['percentage_yield']:,.2%}  [ {summary['impermanent']['period']['token0_qtty']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['impermanent']['period']['token1_qtty']:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {summary['impermanent']['period']['usd_qtty']:,.2f} USD]  [data total days: {summary['impermanent']['period']['seconds']/(60*60*24):,.1f} ]"
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
    elif (
        summary["impermanent"]["period"]["token1_qtty"] < 0
        and summary["impermanent"]["period"]["token0_qtty"] < 0
    ):
        # Worst case
        logging.getLogger(__name__).info(
            f"      worst case possible: both tokens are negative  "
        )
        logging.getLogger(__name__).info(
            f"       {hype_static['pool']['token0']['symbol']} sell price : {abs(summary['impermanent']['period']['token0_qtty_usd']/summary['impermanent']['period']['token0_qtty']):,.2f} USD"
        )
        logging.getLogger(__name__).info(
            f"       {hype_static['pool']['token1']['symbol']} sell price : {abs(summary['impermanent']['period']['token1_qtty_usd']/summary['impermanent']['period']['token1_qtty']):,.2f} USD"
        )
    else:
        if (
            summary["impermanent"]["period"]["token0_qtty"]
            and summary["impermanent"]["period"]["token1_qtty"]
        ):
            logging.getLogger(__name__).info(
                f"       panacea ... no impermanent loss? "
            )

    logging.getLogger("analysis").info(
        f" --> period net yield: {summary['aggregated']['period']['percentage_yield']:,.2%}  [ {summary['aggregated']['period']['token0_qtty']:,.2f} {hype_static['pool']['token0']['symbol']}   {summary['aggregated']['period']['token1_qtty']:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {summary['aggregated']['period']['usd_qtty']:,.2f} USD]"
    )

    # log roi
    logging.getLogger("analysis").info(f" --> period ROI: {summary['roi']:,.2%}")

    # comparison data
    logging.getLogger("analysis").info(f" --> other data for comparison purposes: ")

    period_lping = summary["aggregated"]["period"]["percentage_yield"]
    period_hodl_deposited = (
        summary["aggregated"]["extra"]["comparison"]["deposit"]["token0_qtty"]
        * summary["aggregated"]["extra"]["comparison"]["prices"]["end_token0"]
        + summary["aggregated"]["extra"]["comparison"]["deposit"]["token1_qtty"]
        * summary["aggregated"]["extra"]["comparison"]["prices"]["end_token1"]
    )
    period_hodl_deposited_yield = (
        (
            (
                period_hodl_deposited
                - summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
            )
            / summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        )
        if summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        else Decimal("0")
    )
    period_hodl_fifty = (
        summary["aggregated"]["extra"]["comparison"]["deposit"]["fifty_token0_qtty"]
        * summary["aggregated"]["extra"]["comparison"]["prices"]["end_token0"]
        + summary["aggregated"]["extra"]["comparison"]["deposit"]["fifty_token1_qtty"]
        * summary["aggregated"]["extra"]["comparison"]["prices"]["end_token1"]
    )
    period_hodl_fifty_yield = (
        (
            (
                period_hodl_fifty
                - summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
            )
            / summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        )
        if summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        else Decimal("0")
    )
    period_hodl_token0 = (
        summary["aggregated"]["extra"]["comparison"]["deposit"]["token0_qtty"]
        * summary["aggregated"]["extra"]["comparison"]["prices"]["end_token0"]
    )
    period_hodl_token0_yield = (
        (
            (
                period_hodl_token0
                - summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
            )
            / summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        )
        if summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        else Decimal("0")
    )
    period_hodl_token1 = (
        summary["aggregated"]["extra"]["comparison"]["deposit"]["token1_qtty"]
        * summary["aggregated"]["extra"]["comparison"]["prices"]["end_token1"]
    )
    period_hodl_token1_yield = (
        (
            (
                period_hodl_token1
                - summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
            )
            / summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        )
        if summary["aggregated"]["extra"]["comparison"]["deposit"]["usd_qtty"]
        else Decimal("0")
    )

    # log results: lping vs holding vs 50/50 vs token0 vs token1
    logging.getLogger("analysis").info(
        f"     lping  \t  holding  \t  50/50  \t  token0  \t  token1 : "
    )
    logging.getLogger("analysis").info(
        f"     {period_lping:,.2%}  \t  {period_hodl_deposited_yield:,.2%}  \t  {period_hodl_fifty_yield:,.2%}  \t  {period_hodl_token0_yield:,.2%}  \t  {period_hodl_token1_yield:,.2%} "
    )
    _one_ = (
        "  ^^^"
        if period_lping > period_hodl_deposited_yield
        and period_lping > period_hodl_fifty_yield
        and period_lping > period_hodl_token0_yield
        and period_lping > period_hodl_token1_yield
        else "     "
    )
    _two_ = (
        "  ^^^"
        if period_hodl_deposited_yield > period_lping
        and period_hodl_deposited_yield > period_hodl_fifty_yield
        and period_hodl_deposited_yield > period_hodl_token0_yield
        and period_hodl_deposited_yield > period_hodl_token1_yield
        else "     "
    )
    _three_ = (
        "  ^^^"
        if period_hodl_fifty_yield > period_lping
        and period_hodl_fifty_yield > period_hodl_deposited_yield
        and period_hodl_fifty_yield > period_hodl_token0_yield
        and period_hodl_fifty_yield > period_hodl_token1_yield
        else "     "
    )
    _four_ = (
        "  ^^^"
        if period_hodl_token0_yield > period_lping
        and period_hodl_token0_yield > period_hodl_deposited_yield
        and period_hodl_token0_yield > period_hodl_fifty_yield
        and period_hodl_token0_yield > period_hodl_token1_yield
        else "     "
    )
    _five_ = (
        " ^^^"
        if period_hodl_token1_yield > period_lping
        and period_hodl_token1_yield > period_hodl_deposited_yield
        and period_hodl_token1_yield > period_hodl_fifty_yield
        and period_hodl_token1_yield > period_hodl_token0_yield
        else "     "
    )
    logging.getLogger("analysis").info(
        f"     {_one_}  \t    {_two_}  \t   {_three_}  \t   {_four_}  \t   {_five_} "
    )
    p = ""


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


def log_debug_data(
    yield_data: list[period_yield_data], hype_static: dict, chain: Chain, feeType: str
):
    total_seconds = 0

    ini_price_per_share = yield_data[0].price_per_share
    end_price_per_share = yield_data[-1].price_per_share
    ini_timestamp = yield_data[0].timeframe.ini.timestamp
    end_timestamp = yield_data[-1].timeframe.end.timestamp
    ini_prices = yield_data[0].status.ini.prices
    end_prices = yield_data[-1].status.end.prices
    ini_supply = yield_data[0].status.ini.supply
    end_supply = yield_data[-1].status.end.supply
    # deposit control var
    deposit_qtty_token0 = yield_data[0].status.ini.underlying.qtty.token0
    deposit_qtty_token1 = yield_data[0].status.ini.underlying.qtty.token1

    fees_qtty_token0 = Decimal("0")
    fees_qtty_token1 = Decimal("0")
    fees_per_share = Decimal("0")
    fees_per_share_yield = Decimal("0")
    fees_qtty_usd = Decimal("0")

    rewards_per_share = Decimal("0")
    rewards_per_share_yield = Decimal("0")
    rewards_qtty_usd = Decimal("0")

    impermanent_qtty_token0 = Decimal("0")
    impermanent_qtty_token1 = Decimal("0")
    impermanent_per_share = Decimal("0")
    impermanent_per_share_yield = Decimal("0")
    impermanent_qtty_usd = Decimal("0")

    roi_qtty_usd = Decimal("0")
    roi_qtty_token0 = Decimal("0")
    roi_qtty_token1 = Decimal("0")
    roi_per_share = Decimal("0")
    roi_per_share_yield = Decimal("0")

    for yield_item in yield_data:
        total_seconds += yield_item.period_seconds

        if yield_item.timeframe.ini.timestamp < ini_timestamp:
            ini_timestamp = yield_item.timeframe.ini.timestamp
            ini_price_per_share = yield_item.price_per_share
            ini_prices = yield_item.status.ini.prices
            ini_supply = yield_item.status.ini.supply
            deposit_qtty_token0 = yield_item.status.ini.underlying.qtty.token0
            deposit_qtty_token1 = yield_item.status.ini.underlying.qtty.token1

        if yield_item.timeframe.end.timestamp > end_timestamp:
            end_timestamp = yield_item.timeframe.end.timestamp
            end_price_per_share = yield_item.price_per_share
            end_prices = yield_item.status.end.prices
            end_supply = yield_item.status.end.supply

        # FEES
        fees_qtty_token0 += yield_item.fees.qtty.token0
        fees_qtty_token1 += yield_item.fees.qtty.token1
        fees_qtty_usd += yield_item.period_fees_usd
        fees_per_share += yield_item.fees_per_share
        fees_per_share_yield = fees_per_share / ini_price_per_share

        # REWARDS
        rewards_qtty_usd += (
            yield_item.rewards.usd if yield_item.rewards.usd else Decimal("0")
        )
        rewards_per_share += yield_item.rewards_per_share
        rewards_per_share_yield = rewards_per_share / ini_price_per_share

        # IMPERMANENT
        impermanent_qtty_token0 += (
            yield_item.status.end.underlying.qtty.token0
            - yield_item.fees.qtty.token0
            - yield_item.status.ini.underlying.qtty.token0
        )
        impermanent_qtty_token1 += (
            yield_item.status.end.underlying.qtty.token1
            - yield_item.fees.qtty.token1
            - yield_item.status.ini.underlying.qtty.token1
        )
        impermanent_per_share += yield_item.impermanent_per_share
        impermanent_per_share_yield = impermanent_per_share / ini_price_per_share

        # SHARES LOCAL INFO
        roi_per_share = yield_item.price_per_share - ini_price_per_share
        roi_per_share_yield = roi_per_share / ini_price_per_share
        # roi USD = deposit usd *1+roi_per_share_yield

        impermanent_per_share_sinceIni = (
            roi_per_share - fees_per_share - rewards_per_share
        )
        impermanent_per_share_sinceIni_yield = (
            impermanent_per_share_sinceIni / ini_price_per_share
        )

        # analyze share price ALL
        logging.getLogger("benchmark").info(
            f"FE: {fees_per_share:,.2f} [ {fees_per_share_yield:,.2%} ] \t  RE: {rewards_per_share:,.2f} [ {rewards_per_share_yield:,.2%} ] \t IL: {impermanent_per_share:,.2f} [ {impermanent_per_share_yield:,.2%} ]  \t  RO: {roi_per_share:,.2f} [ {roi_per_share_yield:,.2%} ]  IL_tst {impermanent_per_share_sinceIni:,.2f} [ {impermanent_per_share_sinceIni_yield:,.2%} ]"
        )

        impermanent_qtty_usd = impermanent_per_share * yield_item.status.end.supply
        roi_qtty_usd = fees_qtty_usd + rewards_qtty_usd + impermanent_qtty_usd

        roi_qtty_token0 = fees_qtty_token0 + impermanent_qtty_token0
        roi_qtty_token1 = fees_qtty_token1 + impermanent_qtty_token1

        # price variation ( current from ini)
        _price_variation_token0 = (
            yield_item.status.end.prices.token0 - ini_prices.token0
        ) / ini_prices.token0
        _price_variation_token1 = (
            yield_item.status.end.prices.token1 - ini_prices.token1
        ) / ini_prices.token1

        logging.getLogger("benchmark").info(
            f"FE: {fees_qtty_usd:,.2f} [ {fees_per_share_yield:,.2%} ] \t  RE: {rewards_qtty_usd:,.2f} [ {rewards_per_share_yield:,.2%} ] \t IL: {impermanent_qtty_usd:,.2f} [ {impermanent_per_share_yield:,.2%} ]  \t  RO: {roi_qtty_usd:,.2f} [ {roi_per_share_yield:,.2%} ] "
        )

        # no rewards in ROI calculation
        logging.getLogger("benchmark").info(
            f"FE: {fees_qtty_token0:,.2f}   {fees_qtty_token1:,.2f}    \t  RE: {0:,.2f}      {0:,.2f}      \t IL: {impermanent_qtty_token0:,.2f}      {impermanent_qtty_token1:,.2f}  \t  RO: {roi_qtty_token0:,.2f}     {roi_qtty_token1:,.2f}   [ price var. {_price_variation_token0:,.2%} {_price_variation_token1:,.2%} ]"
        )
        logging.getLogger("benchmark").info("   ")

    # aggregated data ( net yield)
    period_net_yield = (
        fees_per_share_yield + rewards_per_share_yield + impermanent_per_share_yield
    )
    period_net_yield_qtty_token0 = fees_qtty_token0 + impermanent_qtty_token0
    period_net_yield_qtty_token1 = fees_qtty_token1 + impermanent_qtty_token1
    period_net_yield_qtty_usd = fees_qtty_usd + rewards_qtty_usd + impermanent_qtty_usd

    # YEARLY EXTRAPOLATION
    total_seconds = Decimal(str(total_seconds))
    day_in_seconds = 60 * 60 * 24
    year_in_seconds = Decimal(str(day_in_seconds * 365))
    # period Fees to yearly extrapolation
    year_fees_qtty_usd = (fees_qtty_usd / total_seconds) * year_in_seconds
    year_fees_per_share = (fees_per_share / total_seconds) * year_in_seconds
    year_fees_per_share_yield = year_fees_per_share / ini_price_per_share
    year_fees_qtty_token0 = (fees_qtty_token0 / total_seconds) * year_in_seconds
    year_fees_qtty_token1 = (fees_qtty_token1 / total_seconds) * year_in_seconds

    # period rewards to yearly extrapolation
    year_rewards_qtty_usd = (rewards_qtty_usd / total_seconds) * year_in_seconds
    year_rewards_per_share = (rewards_per_share / total_seconds) * year_in_seconds
    year_rewards_per_share_yield = year_rewards_per_share / ini_price_per_share

    # period impermanent to yearly extrapolation
    year_impermanent_qtty_usd = (impermanent_qtty_usd / total_seconds) * year_in_seconds
    year_impermanent_per_share = (
        impermanent_per_share / total_seconds
    ) * year_in_seconds
    year_impermanent_per_share_yield = year_impermanent_per_share / ini_price_per_share
    year_impermanent_qtty_token0 = (
        impermanent_qtty_token0 / total_seconds
    ) * year_in_seconds
    year_impermanent_qtty_token1 = (
        impermanent_qtty_token1 / total_seconds
    ) * year_in_seconds

    # period net yield to yearly extrapolation
    year_net_yield_qtty_usd = (
        period_net_yield_qtty_usd / total_seconds
    ) * year_in_seconds
    year_net_yield_per_share = (period_net_yield / total_seconds) * year_in_seconds
    year_net_yield_per_share_yield = year_net_yield_per_share / ini_price_per_share
    year_net_yield_qtty_token0 = (
        period_net_yield_qtty_token0 / total_seconds
    ) * year_in_seconds
    year_net_yield_qtty_token1 = (
        period_net_yield_qtty_token1 / total_seconds
    ) * year_in_seconds

    # Title
    logging.getLogger("analysis").info(
        f" {chain.fantasy_name}'s {hype_static['dex']} {hype_static['symbol']} returns:  [{hype_static['address']}]  -> fee type: {feeType}"
    )
    # Summary
    logging.getLogger("analysis").info(
        f"    {feeType} period fees : {fees_per_share_yield:,.2%}  [ {fees_qtty_token0:,.2f} {hype_static['pool']['token0']['symbol']}   {fees_qtty_token1:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {fees_qtty_usd:,.2f} USD]  [total days: {total_seconds/(60*60*24):,.1f} ]  [APR: {year_fees_per_share_yield:,.2%} ]"
    )
    logging.getLogger("analysis").info(
        f"     period rewards : {rewards_per_share_yield:,.2%}  [ {rewards_qtty_usd:,.2f} USD]  [data total days: {total_seconds/(60*60*24):,.1f} ]"
    )
    # logging.getLogger("analysis").info(
    #     f" period impermanent : {impermanent_per_share_yield:,.2%}  [ {impermanent_qtty_token0:,.2f} {hype_static['pool']['token0']['symbol']}   {impermanent_qtty_token1:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {impermanent_qtty_usd:,.2f} USD]  [data total days: {total_seconds/(60*60*24):,.1f} ]"
    # )
    logging.getLogger("analysis").info(
        f" period impermanent : {impermanent_per_share_yield:,.2%}  [ {impermanent_qtty_usd:,.2f} USD]  [data total days: {total_seconds/(60*60*24):,.1f} ]"
    )
    logging.getLogger("analysis").info(
        f" token usd price variation: {_price_variation_token0:,.2%} {hype_static['pool']['token0']['symbol']}         {_price_variation_token1:,.2%} {hype_static['pool']['token1']['symbol']}"
    )

    # NET YIELD SUMMARY
    logging.getLogger("analysis").info(
        f" --> period net yield: {period_net_yield:,.2%}  [ {period_net_yield_qtty_token0:,.2f} {hype_static['pool']['token0']['symbol']}   {period_net_yield_qtty_token1:,.2f} {hype_static['pool']['token1']['symbol']} ] [ {period_net_yield_qtty_usd:,.2f} USD]"
    )

    # log roi
    logging.getLogger("analysis").info(f" --> period ROI: {roi_per_share_yield:,.2%}")

    # comparison data
    logging.getLogger("analysis").info(f" --> other data for comparison purposes: ")

    # calculate deposited
    deposit_qtty_usd = (
        deposit_qtty_token0 * ini_prices.token0
        + deposit_qtty_token1 * ini_prices.token1
    )
    # calculate 50/50
    fifty_qtty_token0 = (deposit_qtty_usd / 2) / ini_prices.token0
    fifty_qtty_token1 = (deposit_qtty_usd / 2) / ini_prices.token1
    # calculate hold
    hold_token0_qtty = deposit_qtty_usd / ini_prices.token0
    hold_token1_qtty = deposit_qtty_usd / ini_prices.token1
    #
    period_lping = period_net_yield
    period_hodl_deposited = (
        deposit_qtty_token0 * end_prices.token0
        + deposit_qtty_token1 * end_prices.token1
    )
    period_hodl_deposited_yield = (
        ((period_hodl_deposited - deposit_qtty_usd) / deposit_qtty_usd)
        if deposit_qtty_usd
        else Decimal("0")
    )
    period_hodl_fifty = (
        fifty_qtty_token0 * end_prices.token0 + fifty_qtty_token1 * end_prices.token1
    )
    period_hodl_fifty_yield = (
        ((period_hodl_fifty - deposit_qtty_usd) / deposit_qtty_usd)
        if deposit_qtty_usd
        else Decimal("0")
    )
    period_hodl_token0 = hold_token0_qtty * end_prices.token0
    period_hodl_token0_yield = (
        ((period_hodl_token0 - deposit_qtty_usd) / deposit_qtty_usd)
        if deposit_qtty_usd
        else Decimal("0")
    )
    period_hodl_token1 = hold_token1_qtty * end_prices.token1
    period_hodl_token1_yield = (
        ((period_hodl_token1 - deposit_qtty_usd) / deposit_qtty_usd)
        if deposit_qtty_usd
        else Decimal("0")
    )

    # log results: lping vs holding vs 50/50 vs token0 vs token1
    logging.getLogger("analysis").info(
        f"     lping  \t  holding  \t  50/50  \t  token0  \t  token1 : "
    )
    logging.getLogger("analysis").info(
        f"     {period_lping:,.2%}  \t  {period_hodl_deposited_yield:,.2%}  \t  {period_hodl_fifty_yield:,.2%}  \t  {period_hodl_token0_yield:,.2%}  \t  {period_hodl_token1_yield:,.2%} "
    )
    _one_ = (
        "  ^^^"
        if period_lping > period_hodl_deposited_yield
        and period_lping > period_hodl_fifty_yield
        and period_lping > period_hodl_token0_yield
        and period_lping > period_hodl_token1_yield
        else "     "
    )
    _two_ = (
        "  ^^^"
        if period_hodl_deposited_yield > period_lping
        and period_hodl_deposited_yield > period_hodl_fifty_yield
        and period_hodl_deposited_yield > period_hodl_token0_yield
        and period_hodl_deposited_yield > period_hodl_token1_yield
        else "     "
    )
    _three_ = (
        "  ^^^"
        if period_hodl_fifty_yield > period_lping
        and period_hodl_fifty_yield > period_hodl_deposited_yield
        and period_hodl_fifty_yield > period_hodl_token0_yield
        and period_hodl_fifty_yield > period_hodl_token1_yield
        else "     "
    )
    _four_ = (
        "  ^^^"
        if period_hodl_token0_yield > period_lping
        and period_hodl_token0_yield > period_hodl_deposited_yield
        and period_hodl_token0_yield > period_hodl_fifty_yield
        and period_hodl_token0_yield > period_hodl_token1_yield
        else "     "
    )
    _five_ = (
        " ^^^"
        if period_hodl_token1_yield > period_lping
        and period_hodl_token1_yield > period_hodl_deposited_yield
        and period_hodl_token1_yield > period_hodl_fifty_yield
        and period_hodl_token1_yield > period_hodl_token0_yield
        else "     "
    )
    logging.getLogger("analysis").info(
        f"     {_one_}  \t    {_two_}  \t   {_three_}  \t   {_four_}  \t   {_five_} "
    )


class period_yield_analyzer:
    def __init__(
        self,
        chain: Chain,
        yield_data_list: list[period_yield_data],
        hypervisor_static: dict,
    ) -> None:
        # save base data
        self.chain = chain
        self.yield_data_list = yield_data_list
        self.hypervisor_static = hypervisor_static
        # init other vars
        self._initialize()
        # execute analysis
        self._execute_analysis()

    def _initialize(self):
        # total period seconds
        self._total_seconds = 0
        # initial and end
        self._ini_price_per_share = self.yield_data_list[0].price_per_share
        self._end_price_per_share = self.yield_data_list[-1].price_per_share
        self._ini_timestamp = self.yield_data_list[0].timeframe.ini.timestamp
        self._end_timestamp = self.yield_data_list[-1].timeframe.end.timestamp
        self._ini_prices = self.yield_data_list[0].status.ini.prices
        self._end_prices = self.yield_data_list[-1].status.end.prices
        self._ini_supply = self.yield_data_list[0].status.ini.supply
        self._end_supply = self.yield_data_list[-1].status.end.supply

        # price variation
        self._price_variation_token0 = Decimal("0")
        self._price_variation_token1 = Decimal("0")
        # deposit control var
        self._deposit_qtty_token0 = self.yield_data_list[
            0
        ].status.ini.underlying.qtty.token0
        self._deposit_qtty_token1 = self.yield_data_list[
            0
        ].status.ini.underlying.qtty.token1
        # fees
        self._fees_qtty_token0 = Decimal("0")
        self._fees_qtty_token1 = Decimal("0")
        self._fees_per_share = Decimal("0")
        self._fees_per_share_yield = Decimal("0")
        self._fees_qtty_usd = Decimal("0")
        # rewards
        self._rewards_per_share = Decimal("0")
        self._rewards_per_share_yield = Decimal("0")
        self._rewards_qtty_usd = Decimal("0")
        # impermanent
        self._impermanent_qtty_token0 = Decimal("0")
        self._impermanent_qtty_token1 = Decimal("0")
        self._impermanent_per_share = Decimal("0")
        self._impermanent_per_share_yield = Decimal("0")
        self._impermanent_qtty_usd = Decimal("0")
        # returns ( hypervisor and net returns ) -> hypervisor returns = fees + impermanent and net returns = hypervisor returns + rewards
        self._hype_roi_qtty_usd = Decimal("0")
        self._hype_roi_qtty_token0 = Decimal("0")
        self._hype_roi_qtty_token1 = Decimal("0")
        self._hype_roi_per_share = Decimal("0")
        self._hype_roi_per_share_yield = Decimal("0")
        self._net_roi_qtty_usd = Decimal("0")
        self._net_roi_qtty_token0 = Decimal("0")
        self._net_roi_qtty_token1 = Decimal("0")
        self._net_roi_per_share = Decimal("0")
        self._net_roi_per_share_yield = Decimal("0")

        # comparison
        self._period_hodl_deposited = Decimal("0")
        self._period_hodl_deposited_yield = Decimal("0")
        self._period_hodl_fifty = Decimal("0")
        self._period_hodl_fifty_yield = Decimal("0")
        self._period_hodl_token0 = Decimal("0")
        self._period_hodl_token0_yield = Decimal("0")
        self._period_hodl_token1 = Decimal("0")
        self._period_hodl_token1_yield = Decimal("0")

        # graphic
        self._graph_data = []

    def _create_year_vars(self):
        total_seconds = Decimal(str(self._total_seconds))
        day_in_seconds = 60 * 60 * 24
        year_in_seconds = Decimal(str(day_in_seconds * 365))
        # create vars
        self._year_fees_per_share_yield = (
            self._fees_per_share_yield / self._total_seconds
        ) * year_in_seconds

        self._year_fees_qtty_usd = (
            self._fees_qtty_usd / total_seconds
        ) * year_in_seconds
        self._year_fees_per_share = (
            self._fees_per_share / total_seconds
        ) * year_in_seconds
        self._year_fees_qtty_token0 = (
            self._fees_qtty_token0 / total_seconds
        ) * year_in_seconds
        self._year_fees_qtty_token1 = (
            self._fees_qtty_token1 / total_seconds
        ) * year_in_seconds

        # period rewards to yearly extrapolation
        self._year_rewards_qtty_usd = (
            self._rewards_qtty_usd / total_seconds
        ) * year_in_seconds
        self._year_rewards_per_share = (
            self._rewards_per_share / total_seconds
        ) * year_in_seconds
        self._year_rewards_per_share_yield = (
            self._year_rewards_per_share / self._ini_price_per_share
        )

        # period impermanent to yearly extrapolation
        self._year_impermanent_qtty_usd = (
            self._impermanent_qtty_usd / total_seconds
        ) * year_in_seconds
        self._year_impermanent_per_share = (
            self._impermanent_per_share / total_seconds
        ) * year_in_seconds
        self._year_impermanent_per_share_yield = (
            self._year_impermanent_per_share / self._ini_price_per_share
        )

        # period net yield to yearly extrapolation
        self._year_net_yield_qtty_usd = (
            self._net_roi_yield_qtty_usd / total_seconds
        ) * year_in_seconds
        self._year_net_yield_per_share = (
            self._net_roi_yield / total_seconds
        ) * year_in_seconds
        self._year_net_yield_per_share_yield = (
            self._year_net_yield_per_share / self._ini_price_per_share
        )

    # COMPARISON PROPERTIES
    @property
    def deposit_qtty_usd(self):
        return (
            self._deposit_qtty_token0 * self._ini_prices.token0
            + self._deposit_qtty_token1 * self._ini_prices.token1
        )

    @property
    def fifty_qtty_token0(self):
        return (self.deposit_qtty_usd / 2) / self._ini_prices.token0

    @property
    def fifty_qtty_token1(self):
        return (self.deposit_qtty_usd / 2) / self._ini_prices.token1

    @property
    def hold_token0_qtty(self):
        return self.deposit_qtty_usd / self._ini_prices.token0

    @property
    def hold_token1_qtty(self):
        return self.deposit_qtty_usd / self._ini_prices.token1

    @property
    def period_hodl_deposited(self):
        return (
            self._deposit_qtty_token0 * self._end_prices.token0
            + self._deposit_qtty_token1 * self._end_prices.token1
        )

    # MAIN METHODS
    def _execute_analysis(self):
        # fill variables
        self._find_initial_values()
        self._fill_variables()

        # check data consistency
        self._check_data_consistency()

    # LOOP
    def _fill_variables(self):
        for yield_item in self.yield_data_list:
            # FEES ( does not need any previous data )
            self._fill_variables_fees(yield_item)

            # REWARDS ( does not need any previous data )
            self._fill_variables_rewards(yield_item)

            # RETURN HYPERVISOR ( does not need any previous data )
            self._fill_variables_hypervisor_return(yield_item)

            # IMPERMANENT ( needs return to be processed first)
            self._fill_variables_impermanent(yield_item)

            # RETURN NET ( needs return+fees+rewards+impermanent to be processed first)
            self._fill_variables_net_return(yield_item)

            # PRICE VARIATION ( does not need any previous data )
            self._fill_variables_price(yield_item)

            # COMPARISON
            self._fill_variables_comparison()

            # YEAR variables
            self._create_year_vars()

            # GRAPH ( needs all previous data )
            self._fill_graph(yield_item)

            # TODO: remove this
            self.debug_line(yield_item)

    # FILL VARIABLES
    def _fill_variables_fees(self, yield_item: period_yield_data):
        # FEES
        self._fees_qtty_token0 += yield_item.fees.qtty.token0 or Decimal("0")
        self._fees_qtty_token1 += yield_item.fees.qtty.token1 or Decimal("0")
        self._fees_qtty_usd += yield_item.period_fees_usd or Decimal("0")
        self._fees_per_share += yield_item.fees_per_share or Decimal("0")
        self._fees_per_share_yield = self._fees_per_share / self._ini_price_per_share

    def _fill_variables_rewards(self, yield_item: period_yield_data):
        # rewards for the period
        # yield_item.rewards.period_yield is extrapolated to current period seconds
        self._rewards_per_share += yield_item.rewards_per_share or Decimal("0")
        self._rewards_qtty_usd += yield_item.rewards.usd or Decimal("0")

        self._rewards_per_share_yield = (
            self._rewards_per_share / self._ini_price_per_share
        )

    def _fill_variables_impermanent(self, yield_item: period_yield_data):
        # IMPERMANENT ( )

        self._impermanent_qtty_usd = self._hype_roi_qtty_usd - self._fees_qtty_usd

        self._impermanent_qtty_token0 = (
            yield_item.status.end.underlying.qtty.token0
            - yield_item.fees.qtty.token0
            # - self._deposit_qtty_token0
            - yield_item.status.ini.underlying.qtty.token0
        )
        self._impermanent_qtty_token1 = (
            yield_item.status.end.underlying.qtty.token1
            - yield_item.fees.qtty.token1
            - yield_item.status.ini.underlying.qtty.token1
        )
        self._impermanent_per_share = (
            yield_item.price_per_share
            - self._ini_price_per_share
            - self._fees_per_share
        )
        self._impermanent_per_share_yield = (
            self._impermanent_per_share / self._ini_price_per_share
        )

    def _fill_variables_hypervisor_return(self, yield_item: period_yield_data):
        """Fees + impermanent  ( no rewards included )

        Args:
            yield_item (period_yield_data): _description_

        Yields:
            _type_: _description_
        """
        self._hype_roi_per_share = (
            yield_item.price_per_share - self._ini_price_per_share
        )
        self._hype_roi_per_share_yield = (
            self._hype_roi_per_share / self._ini_price_per_share
        )

        self._hype_roi_qtty_usd = self.deposit_qtty_usd * self._hype_roi_per_share_yield
        _token0_weight, _token1_weight = self.get_token_usd_weight(yield_item)
        self._hype_roi_qtty_token0 = _token0_weight * self._hype_roi_qtty_usd
        self._hype_roi_qtty_token1 = _token1_weight * self._hype_roi_qtty_usd

    def _fill_variables_net_return(self, yield_item: period_yield_data):
        """Roi + rewards ( so fees + impermanent + rewards)

        Args:
            yield_item (period_yield_data):

        """
        # include rewards in net return
        self._net_roi_per_share = (
            self._fees_per_share + self._rewards_per_share + self._impermanent_per_share
        )
        self._net_roi_yield = self._net_roi_per_share / self._ini_price_per_share
        self._net_roi_yield_qtty_usd = (
            self._fees_qtty_usd + self._rewards_qtty_usd + self._impermanent_qtty_usd
        )

    def _fill_variables_price(self, yield_item: period_yield_data):
        self._price_variation_token0 = (
            yield_item.status.end.prices.token0 - self._ini_prices.token0
        ) / self._ini_prices.token0
        self._price_variation_token1 = (
            yield_item.status.end.prices.token1 - self._ini_prices.token1
        ) / self._ini_prices.token1

    def _fill_variables_comparison(self):
        self._period_hodl_deposited_yield = (
            (
                (self.period_hodl_deposited - self.deposit_qtty_usd)
                / self.deposit_qtty_usd
            )
            if self.deposit_qtty_usd
            else Decimal("0")
        )
        self._period_hodl_fifty = (
            self.fifty_qtty_token0 * self._end_prices.token0
            + self.fifty_qtty_token1 * self._end_prices.token1
        )
        self._period_hodl_fifty_yield = (
            ((self._period_hodl_fifty - self.deposit_qtty_usd) / self.deposit_qtty_usd)
            if self.deposit_qtty_usd
            else Decimal("0")
        )
        self._period_hodl_token0 = self.hold_token0_qtty * self._end_prices.token0
        self._period_hodl_token0_yield = (
            ((self._period_hodl_token0 - self.deposit_qtty_usd) / self.deposit_qtty_usd)
            if self.deposit_qtty_usd
            else Decimal("0")
        )
        self._period_hodl_token1 = self.hold_token1_qtty * self._end_prices.token1
        self._period_hodl_token1_yield = (
            ((self._period_hodl_token1 - self.deposit_qtty_usd) / self.deposit_qtty_usd)
            if self.deposit_qtty_usd
            else Decimal("0")
        )

    def _fill_graph(self, yield_item: period_yield_data):
        # add to graph data
        self._graph_data.append(
            {
                "chain": self.chain.database_name,
                "address": self.hypervisor_static["address"],
                "symbol": self.hypervisor_static["symbol"],
                "block": yield_item.timeframe.end.block,
                "timestamp": yield_item.timeframe.end.timestamp,
                "period": None,
                "fees": {
                    "period": {
                        "yield": self._fees_per_share_yield,
                        "qtty_usd": self._fees_qtty_usd,
                        "qtty_token0": self._fees_qtty_token0,
                        "qtty_token1": self._fees_qtty_token1,
                    },
                    "year": {
                        "yield": self._year_fees_per_share_yield,
                        "qtty_usd": self._year_fees_qtty_usd,
                        "qtty_token0": self._year_fees_qtty_token0,
                        "qtty_token1": self._year_fees_qtty_token1,
                    },
                },
                "rewards": {
                    "period": {
                        "yield": self._rewards_per_share_yield,
                        "qtty_usd": self._rewards_qtty_usd,
                    },
                    "year": {
                        "yield": self._year_rewards_per_share_yield,
                        "qtty_usd": self._year_rewards_qtty_usd,
                    },
                },
                "impermanent": {
                    "period": {
                        "yield": self._impermanent_per_share_yield,
                        "qtty_usd": self._impermanent_qtty_usd,
                        "qtty_token0": self._impermanent_qtty_token0,
                        "qtty_token1": self._impermanent_qtty_token1,
                    }
                },
                "roi": {
                    "period": {
                        "return": self._net_roi_yield,
                        "qtty_usd": self._net_roi_yield_qtty_usd,
                    },
                    "period_hypervisor": {
                        "return": self._hype_roi_per_share_yield,
                        "qtty_usd": self._hype_roi_qtty_usd,
                        "qtty_token0": self._hype_roi_qtty_token0,
                        "qtty_token1": self._hype_roi_qtty_token1,
                    },
                },
                "price": {
                    "period": {
                        "variation_token0": self._price_variation_token0,
                        "variation_token1": self._price_variation_token1,
                    },
                },
                "comparison": {
                    "return": {
                        "gamma": self._net_roi_yield,
                        "hodl_deposited": self._period_hodl_deposited_yield,
                        "hodl_fifty": self._period_hodl_fifty_yield,
                        "hodl_token0": self._period_hodl_token0_yield,
                        "hodl_token1": self._period_hodl_token1_yield,
                    },
                    "gamma_vs": {
                        "hodl_deposited": (
                            (self._net_roi_yield + 1)
                            / (self._period_hodl_deposited_yield + 1)
                        )
                        - 1,
                        "hodl_fifty": (
                            (self._net_roi_yield + 1)
                            / (self._period_hodl_fifty_yield + 1)
                        )
                        - 1,
                        "hodl_token0": (
                            (self._net_roi_yield + 1)
                            / (self._period_hodl_token0_yield + 1)
                        )
                        - 1,
                        "hodl_token1": (
                            (self._net_roi_yield + 1)
                            / (self._period_hodl_token1_yield + 1)
                        )
                        - 1,
                    },
                },
                # compatible with old version
                "year_feeApr": self._year_fees_per_share_yield,
                "year_feeApy": self._year_fees_per_share_yield,
                "year_allRewards2": self._year_rewards_per_share_yield,
                "period_feeApr": self._fees_per_share_yield,
                "period_rewardsApr": self._rewards_per_share_yield,
                "period_lping": self._net_roi_yield,
                "period_hodl_deposited": self._period_hodl_deposited_yield,
                "period_hodl_fifty": self._period_hodl_fifty_yield,
                "period_hodl_token0": self._period_hodl_token0_yield,
                "period_hodl_token1": self._period_hodl_token1_yield,
                "period_netApr": self._net_roi_yield,
                "period_impermanentResult": self._impermanent_per_share_yield,
                "gamma_vs_hodl": (
                    (self._net_roi_yield + 1) / (self._period_hodl_deposited_yield + 1)
                )
                - 1,
            }
        )
        # OLD VERSION
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

    def _check_data_consistency(self):
        """Check if data is consistent

        Raises:
            Exception: _description_
        """
        # check ROI equality
        if (
            f"{self._hype_roi_qtty_token0 + self._hype_roi_qtty_token1:,.3f}"
            != f"{self._hype_roi_qtty_usd:,.3f}"
        ):
            raise Exception("ROI calculation error")
        # check ROI overall
        if (
            f"{self._hype_roi_per_share:,.3f}"
            != f"{self._end_price_per_share - self._ini_price_per_share:,.3f}"
        ):
            raise Exception("ROI calculation error")

        # check USD qtty ( only fees + impermanent, because rewards are external to this system)
        if (
            f"{self._hype_roi_qtty_usd:,.3f}"
            != f"{self._fees_qtty_usd + self._impermanent_qtty_usd:,.3f}"
        ):
            raise Exception("USD qtty calculation error")

    # MMMM
    def get_graph(self) -> list[dict]:
        return self._graph_data

    def print_result(self):
        feeType = "LPs"
        # Title
        logging.getLogger("analysis").info(
            f" {self.chain.fantasy_name}'s {self.hypervisor_static['dex']} {self.hypervisor_static['symbol']} returns:  [{self.hypervisor_static['address']}]  -> fee type: {feeType}"
        )
        # Summary
        logging.getLogger("analysis").info(
            f"    {feeType} period fees : {self._fees_per_share_yield:,.2%}  [ {self._fees_qtty_token0:,.2f} {self.hypervisor_static['pool']['token0']['symbol']}   {self._fees_qtty_token1:,.2f} {self.hypervisor_static['pool']['token1']['symbol']} ] [ {self._fees_qtty_usd:,.2f} USD]  [total days: {self._total_seconds/(60*60*24):,.1f} ]  [APR: {self._year_fees_per_share_yield:,.2%} ]"
        )
        logging.getLogger("analysis").info(
            f"     period rewards : {self._rewards_per_share_yield:,.2%}  [ {self._rewards_qtty_usd:,.2f} USD]  [data total days: {self._total_seconds/(60*60*24):,.1f} ]"
        )
        # logging.getLogger("analysis").info(
        #     f" period impermanent : {impermanent_per_share_yield:,.2%}  [ {impermanent_qtty_token0:,.2f} {self.hypervisor_static['pool']['token0']['symbol']}   {impermanent_qtty_token1:,.2f} {self.hypervisor_static['pool']['token1']['symbol']} ] [ {impermanent_qtty_usd:,.2f} USD]  [data total days: {total_seconds/(60*60*24):,.1f} ]"
        # )
        logging.getLogger("analysis").info(
            f" period impermanent : {self._impermanent_per_share_yield:,.2%}  [ {self._impermanent_qtty_usd:,.2f} USD]  [data total days: {self._total_seconds/(60*60*24):,.1f} ]"
        )
        logging.getLogger("analysis").info(
            f" token usd price variation: {self._price_variation_token0:,.2%} {self.hypervisor_static['pool']['token0']['symbol']}         {self._price_variation_token1:,.2%} {self.hypervisor_static['pool']['token1']['symbol']}"
        )

        # Hypervisor Return
        logging.getLogger("analysis").info(
            f"     period HYPE ROI (fees+impermanent): {self._hype_roi_per_share_yield:,.2%}  [ {self._hype_roi_qtty_token0:,.2f} {self.hypervisor_static['pool']['token0']['symbol']}   {self._hype_roi_qtty_token1:,.2f} {self.hypervisor_static['pool']['token1']['symbol']} ] [ {self._hype_roi_qtty_usd:,.2f} USD]"
        )
        # NET Return
        logging.getLogger("analysis").info(
            f" --> period NET ROI: {self._net_roi_yield:,.2%}  [ {self._net_roi_yield_qtty_usd:,.2f} USD]"
        )

        # comparison data: lping vs holding vs 50/50 vs token0 vs token1
        logging.getLogger("analysis").info(f" --> other data for comparison purposes: ")

        logging.getLogger("analysis").info(
            f"     gamma  \t  holding  \t  50/50  \t  token0  \t  token1 : "
        )
        logging.getLogger("analysis").info(
            f"     {self._net_roi_yield:,.2%}  \t  {self._period_hodl_deposited_yield:,.2%}  \t  {self._period_hodl_fifty_yield:,.2%}  \t  {self._period_hodl_token0_yield:,.2%}  \t  {self._period_hodl_token1_yield:,.2%} "
        )
        _one_ = (
            "  ^^^"
            if self._net_roi_yield > self._period_hodl_deposited_yield
            and self._net_roi_yield > self._period_hodl_fifty_yield
            and self._net_roi_yield > self._period_hodl_token0_yield
            and self._net_roi_yield > self._period_hodl_token1_yield
            else "     "
        )
        _two_ = (
            "  ^^^"
            if self._period_hodl_deposited_yield > self._net_roi_yield
            and self._period_hodl_deposited_yield > self._period_hodl_fifty_yield
            and self._period_hodl_deposited_yield > self._period_hodl_token0_yield
            and self._period_hodl_deposited_yield > self._period_hodl_token1_yield
            else "     "
        )
        _three_ = (
            "  ^^^"
            if self._period_hodl_fifty_yield > self._net_roi_yield
            and self._period_hodl_fifty_yield > self._period_hodl_deposited_yield
            and self._period_hodl_fifty_yield > self._period_hodl_token0_yield
            and self._period_hodl_fifty_yield > self._period_hodl_token1_yield
            else "     "
        )
        _four_ = (
            "  ^^^"
            if self._period_hodl_token0_yield > self._net_roi_yield
            and self._period_hodl_token0_yield > self._period_hodl_deposited_yield
            and self._period_hodl_token0_yield > self._period_hodl_fifty_yield
            and self._period_hodl_token0_yield > self._period_hodl_token1_yield
            else "     "
        )
        _five_ = (
            " ^^^"
            if self._period_hodl_token1_yield > self._net_roi_yield
            and self._period_hodl_token1_yield > self._period_hodl_deposited_yield
            and self._period_hodl_token1_yield > self._period_hodl_fifty_yield
            and self._period_hodl_token1_yield > self._period_hodl_token0_yield
            else "     "
        )
        logging.getLogger("analysis").info(
            f"     {_one_}  \t    {_two_}  \t   {_three_}  \t   {_four_}  \t   {_five_} "
        )
        # gamma vs hold
        logging.getLogger("analysis").info(
            f"     gamma vs hold: {((self._net_roi_yield + 1) / (self._period_hodl_deposited_yield + 1)) - 1:,.2%}"
        )

    # HELPERS
    def _find_initial_values(self):
        for yield_item in self.yield_data_list:
            # add total seconds
            self._total_seconds += yield_item.period_seconds

            # modify initial and end values, if needed ( should not be needed bc its sorted by timestamp)
            if yield_item.timeframe.ini.timestamp < self._ini_timestamp:
                self._ini_timestamp = yield_item.timeframe.ini.timestamp
                self._ini_price_per_share = yield_item.price_per_share
                self._ini_prices = yield_item.status.ini.prices
                self._ini_supply = yield_item.status.ini.supply
                self._deposit_qtty_token0 = yield_item.status.ini.underlying.qtty.token0
                self._deposit_qtty_token1 = yield_item.status.ini.underlying.qtty.token1

            if yield_item.timeframe.end.timestamp > self._end_timestamp:
                self._end_timestamp = yield_item.timeframe.end.timestamp
                self._end_price_per_share = yield_item.price_per_share
                self._end_prices = yield_item.status.end.prices
                self._end_supply = yield_item.status.end.supply

    def get_token_usd_weight(
        self, yield_item: period_yield_data
    ) -> tuple[Decimal, Decimal]:
        """Extract current underlying token weights vs current usd price

        Args:
            yield_item (period_yield_data):
        """
        _token0_end_usd_value = (
            yield_item.status.end.prices.token0
            * yield_item.status.end.underlying.qtty.token0
        )
        _token1_end_usd_value = (
            yield_item.status.end.prices.token1
            * yield_item.status.end.underlying.qtty.token1
        )
        _token0_percentage = _token0_end_usd_value / (
            _token0_end_usd_value + _token1_end_usd_value
        )
        _token1_percentage = _token1_end_usd_value / (
            _token0_end_usd_value + _token1_end_usd_value
        )
        return _token0_percentage, _token1_percentage

    def debug_line(self,yield_item: period_yield_data):
        logging.getLogger("benchmark").info(
            f" {self._fees_per_share:,.2f}  {self._rewards_per_share:,.2f}  {self._impermanent_per_share:,.2f}  {yield_item.price_per_share:,.2f} [roi net:{self._net_roi_per_share:,.2f}] [roi hype:{self._hype_roi_per_share:,.2f}] [initial:{self._ini_price_per_share:,.2f} ]"
        )
