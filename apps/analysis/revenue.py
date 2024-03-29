import calendar
from datetime import datetime, timezone
import logging

import tqdm
from bins.general.enums import Chain
from bins.general.net_utilities import get_request


def build_global_revenue_report(chains: list[Chain] | None = None) -> list[dict]:
    # result var
    result = {}
    # xtrapolation vars
    current_year = datetime.now(timezone.utc).year
    current_month = datetime.now(timezone.utc).month
    days_passed_current_month = datetime.now(timezone.utc).day
    total_days_current_month = calendar.monthrange(
        datetime.now(timezone.utc).year, datetime.now(timezone.utc).month
    )[1]
    days_left_current_month = total_days_current_month - days_passed_current_month

    for chain in tqdm.tqdm(chains or Chain):
        # get revenue data from endpoint
        _revenue_data = get_revenue_data_from_endpoint(chain=chain)

        logging.getLogger(__name__).debug(
            f" {len(_revenue_data)} years of data forund for {chain.name}"
        )
        for _item in _revenue_data:
            if "detail" in _item:
                logging.getLogger(__name__).error(
                    f" ERROR: {chain.name} {_revenue_data['detail'][-1]['msg']}"
                )
                continue
            # year
            _year = _item["_id"]

            # ROOT: create year structure, if needed
            if not _year in result:
                result[_year] = {"total_usd": 0, "by_month": {}, "by_chain": {}}

            # ROOT: add total usd to year
            result[_year]["total_usd"] += _item["total_usd"]

            # ROOT>CHAIN: add chains total
            if not chain.database_name in result[_year]["by_chain"]:
                result[_year]["by_chain"][chain.database_name] = {"total_usd": 0}

            # ROOT>CHAIN: add total usd to chain
            result[_year]["by_chain"][chain.database_name]["total_usd"] += _item[
                "total_usd"
            ]

            # ROOT>MONTH:
            for _month_data in _item["items"]:
                _month = _month_data["month"]
                # ROOT>MONTH ROOT: create month structure months to year, ehn needed
                if not _month in result[_year]["by_month"]:
                    result[_year]["by_month"][_month] = {
                        "total_usd": 0,
                        "by_chain": {},
                    }

                # ROOT>MONTH ROOT: add total usd to month
                result[_year]["by_month"][_month]["total_usd"] += _month_data[
                    "total_usd"
                ]

                # ROOT>MONTH>CHAIN: add monthly chain totals
                if (
                    not chain.database_name
                    in result[_year]["by_month"][_month]["by_chain"]
                ):
                    result[_year]["by_month"][_month]["by_chain"][
                        chain.database_name
                    ] = {"total_usd": 0, "items": []}

                # ROOT>MONTH>CHAIN: add total usd to chain
                result[_year]["by_month"][_month]["by_chain"][chain.database_name][
                    "total_usd"
                ] += _month_data["total_usd"]

                # ROOT>MONTH>CHAIN>ITEMS: add items to the month
                result[_year]["by_month"][_month]["by_chain"][chain.database_name][
                    "items"
                ] += _month_data["items"]

    # SORT THE WHOLE THING
    year_keys = list(result.keys())
    year_keys.sort()
    result = {i: result[i] for i in year_keys}

    for year, year_data in result.items():
        # sort by_chain ( total_usd )
        bychain_values_dict = {
            v["total_usd"]: k for k, v in year_data["by_chain"].items()
        }
        bychain_values = list(bychain_values_dict.keys())
        bychain_values.sort()
        bychain_values.reverse()
        year_data["by_chain"] = {
            bychain_values_dict[i]: year_data["by_chain"][bychain_values_dict[i]]
            for i in bychain_values
        }

        # sort months
        month_keys = list(year_data["by_month"].keys())
        month_keys.sort()
        year_data["by_month"] = {i: year_data["by_month"][i] for i in month_keys}

        # sort by_month.by_chain ( total_usd )
        for month, month_data in year_data["by_month"].items():
            bychain_values_dict = {
                v["total_usd"]: k for k, v in month_data["by_chain"].items()
            }
            bychain_values = list(bychain_values_dict.keys())
            bychain_values.sort()
            bychain_values.reverse()
            month_data["by_chain"] = {
                bychain_values_dict[i]: month_data["by_chain"][bychain_values_dict[i]]
                for i in bychain_values
            }

    # ADD PERCENTAGES ( yearly, monthly, chain )
    result = add_analytic_data_to_global_revenue_report(
        report=result,
        current_year=current_year,
        current_month=current_month,
        days_left_current_month=days_left_current_month,
        days_passed_current_month=days_passed_current_month,
    )

    # return
    return result


def add_analytic_data_to_global_revenue_report(
    report: list[dict],
    current_year,
    current_month,
    days_left_current_month,
    days_passed_current_month,
) -> list[dict]:
    # ADD PERCENTAGES ( yearly, monthly, chain )
    # ADD POTENTIAL TOTAL USD ( monthly, chain )
    # ADD TOTAL USD PER HYPERVISOR ( TOTAL_USD/HYPERVISOR_COUNT )

    for year, year_data in report.items():
        # create a control var to get track of a unique list of hypes n pools
        _unique_hypervisor_list: set[str] = set()
        _unique_pool_list: set[str] = set()
        # because spNFT will not count as hypervisor, we setup a variable divisible by count
        _unique_hypervisor_total_usd: float = 0
        _unique_pool_total_usd: float = 0

        for chain, chain_data in year_data["by_chain"].items():
            chain_data["yearly_percentage"] = (
                (chain_data["total_usd"] / year_data["total_usd"])
                if year_data["total_usd"]
                else 0
            )

        for month, month_data in year_data["by_month"].items():
            # if month is current, lets xtrapolate linearly with a 50% decay on days left -> ( total_usd/days passed * 0.5 * days left + total_usd)
            if (
                int(month) == current_month
                and year == current_year
                and days_left_current_month > 0
            ):
                month_data["potential_total_usd"] = (
                    (month_data["total_usd"] / days_passed_current_month)
                    * 0.5
                    * days_left_current_month
                ) + month_data["total_usd"]

            month_data["yearly_percentage"] = (
                (month_data["total_usd"] / year_data["total_usd"])
                if year_data["total_usd"]
                else 0
            )
            # control var ( to be divided by total hypes count ( not being SPNFT like)
            _current_month_total_usd = 0

            for chain, chain_data in month_data["by_chain"].items():
                chain_data["yearly_percentage"] = (
                    (chain_data["total_usd"] / year_data["total_usd"])
                    if year_data["total_usd"]
                    else 0
                )
                chain_data["monthly_percentage"] = (
                    (chain_data["total_usd"] / month_data["total_usd"])
                    if month_data["total_usd"]
                    else 0
                )

                # if month is current, lets xtrapolate linearly with 50% decay
                if (
                    int(month) == current_month
                    and year == current_year
                    and days_left_current_month > 0
                ):
                    chain_data["potential_total_usd"] = (
                        (chain_data["total_usd"] / days_passed_current_month)
                        * 0.5
                        * days_left_current_month
                    ) + chain_data["total_usd"]

                # ADD TOTAL USD PER HYPERVISOR ( TOTAL_USD/HYPERVISOR_COUNT )
                for dex_item in chain_data["items"]:
                    # control var
                    _current_dex_hypervisor_count = 0
                    _current_dex_total_usd = 0

                    # define current dex hypervisor count (we are in the month)
                    for hype_data_item in dex_item["items"]:
                        for hype_item in hype_data_item["items"]:
                            # hypervisor=0 means spNFT related data and does not have "pool" key.
                            if not hype_item["hypervisor"] == 0:
                                _unique_hypervisor_list.add(hype_item["hypervisor"])
                                _unique_hypervisor_total_usd += hype_item["total_usd"]
                                _current_month_total_usd += hype_item["total_usd"]
                                _current_dex_hypervisor_count += 1
                                _current_dex_total_usd += hype_item["total_usd"]
                            else:
                                # TODO: get a static list of the "dex" hypes to add to hype list
                                pass

                            # add pool to unique pool list
                            if "pool" in hype_item:
                                _unique_pool_list.add(hype_item["pool"])
                                _unique_pool_total_usd += hype_item["total_usd"]
                    # create dex on month data structure
                    if not "by_dex" in month_data:
                        month_data["by_dex"] = {}

                    # create dex on by_dex data structure
                    if not dex_item["dex"] in month_data["by_dex"]:
                        month_data["by_dex"][dex_item["dex"]] = {
                            "total_usd": 0,
                            "total_hypervisor_count": 0,
                            "total_usd_per_hypervisor": 0,
                        }

                    # add to dex total usd
                    month_data["by_dex"][dex_item["dex"]]["total_usd"] += dex_item[
                        "total_usd"
                    ]

                    # add to dex total hypervisor count
                    month_data["by_dex"][dex_item["dex"]][
                        "total_hypervisor_count"
                    ] += _current_dex_hypervisor_count

                    # set dex total usd per hypervisor
                    month_data["by_dex"][dex_item["dex"]][
                        "total_usd_per_hypervisor"
                    ] = (
                        (
                            _current_dex_total_usd
                            / month_data["by_dex"][dex_item["dex"]][
                                "total_hypervisor_count"
                            ]
                        )
                        if month_data["by_dex"][dex_item["dex"]][
                            "total_hypervisor_count"
                        ]
                        else 0
                    )

        # ADD TOTAL USD PER HYPERVISOR ( TOTAL_USD/HYPERVISOR_COUNT )
        year_data["total_hypervisor_count"] = len(_unique_hypervisor_list)
        year_data["total_usd_per_hypervisor"] = (
            (year_data["total_usd"] / year_data["total_hypervisor_count"])
            if year_data["total_hypervisor_count"]
            else 0
        )
        year_data["total_pool_count"] = len(_unique_pool_list)

    # return
    return report


def get_revenue_data_from_endpoint(chain: Chain, yearly: bool = True) -> list[dict]:
    chain_endpoint_name = chain.value if chain != Chain.ETHEREUM else "mainnet"
    _url = f"https://wire3.gamma.xyz/internal/internal/{chain_endpoint_name}/revenue?yearly={yearly}"
    return get_request(url=_url)
