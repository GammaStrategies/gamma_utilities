import sys
import os
import logging
from web3 import Web3
from pathlib import Path
import tqdm
import concurrent.futures

from datetime import datetime, timedelta

# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION
from bins.general import general_utilities, file_utilities

from apps.database_feeder import feed_prices_force_sqrtPriceX96

from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_registry,
)

from bins.log import log_helper
from bins.database.db_raw_direct_info import direct_db_hypervisor_info
from bins.database.common.db_collections_common import (
    database_local,
    database_global,
    db_collections_common,
)


def test_w3_hypervisor_obj(
    protocol: str, network: str, dex: str, hypervisor_address: str, block: int
):
    hypervisor = None
    if dex == "uniswap_v3":
        hypervisor = gamma_hypervisor_cached(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "quickswap":
        hypervisor = gamma_hypervisor_quickswap_cached(
            address=hypervisor_address, network=network, block=block
        )
    else:
        raise NotImplementedError(f" {dex} exchange has not been implemented yet")

    # test fees
    po = hypervisor.as_dict()
    test = ""


def test_prices(protocol="gamma", network="ethereum"):
    # force feed prices from already known using conversion
    feed_prices_force_sqrtPriceX96(protocol=protocol, network=network, threaded=False)


def get_hypervisor_addresses(
    network: str, protocol: str, user_address: str = None
) -> list[str]:

    result = list()
    # get database configuration
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    # get blacklisted hypervisors
    blacklisted = (
        CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get(protocol, {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(network, [])
    )
    # check n clean
    if blacklisted is None:
        blacklisted = []

    # retrieve all addresses from database
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    if user_address:
        result = local_db_manager.get_distinct_items_from_database(
            collection_name="user_status",
            field="hypervisor_address",
            condition={"address": user_address},
        )
    else:
        result = local_db_manager.get_distinct_items_from_database(
            collection_name="static", field="address"
        )

    # apply black list
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process before applying filters: {len(result)}"
    )
    # filter blcacklisted
    result = [x for x in result if not x in blacklisted]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after applying filters: {len(result)}"
    )

    return result


def status_to_csv(status_list: list[dict], folder: str, network: str, symbol: str):
    """save data to csv file

    Args:
        status_list (list[dict]): list of status converted to dict
        folder (str): where to save
        network (str):
        symbol (str): hypervisor symbol
    """

    csv_columns = []
    csv_columns.extend(
        [x for x in list(status_list[-1].keys()) if x not in csv_columns]
    )

    # set filename
    csv_filename = "{}_{}_{}_from_{}_{}.csv".format(
        network,
        symbol,
        status_list[-1]["address"],
        status_list[0]["block"],
        status_list[-1]["block"],
    )
    csv_filename = os.path.join(folder, csv_filename)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass

    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=status_list)


def result_to_csv(
    result: list[dict], folder: str, filename: str, csv_columns: list = []
):
    """save data to csv file

    Args:
        result (list[dict]): list of dicts
        folder (str): where to save
        filename (str):
    """

    csv_columns.extend([x for x in list(result[-1].keys()) if x not in csv_columns])

    csv_filename = os.path.join(folder, filename)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass

    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=result)


def flatten_dict(my_dict: dict, existing_dict: dict = {}, add_key: str = ""):
    for k, v in my_dict.items():
        if not isinstance(v, dict):
            existing_dict[k if add_key == "" else "{}.{}".format(add_key, k)] = v
        else:
            flatten_dict(my_dict=v, existing_dict=existing_dict, add_key=k)
    return existing_dict


def test_db_direct_info(
    network: str,
    protocol: str = "gamma",
    ini_date: datetime = None,
    end_date: datetime = None,
):

    # 1 day from now
    if not end_date:
        end_date = datetime.utcnow()
    if not ini_date:
        ini_date = end_date - timedelta(days=1)

    # get all hype addresses
    addresses = get_hypervisor_addresses(network=network, protocol=protocol)
    # addresses = ["0x35abccd8e577607275647edab08c537fa32cc65e"]
    with tqdm.tqdm(total=len(addresses)) as progress_bar:
        for address in addresses:
            # create helper
            helper = direct_db_hypervisor_info(
                hypervisor_address=address, network=network, protocol=protocol
            )

            # ### DEBUG: create raw data csv ###############
            # # query database
            status = [
                helper.convert_hypervisor_status_fromDb(x)
                for x in helper.get_status_byDay(
                    ini_timestamp=ini_date.timestamp(),
                    end_timestamp=end_date.timestamp(),
                )
            ]
            # flatten dict so can be saved in columns
            status = [flatten_dict(x, {}) for x in status]
            # add usd prices
            for x in status:
                x["usd_price_token0"] = helper.get_price(
                    block=x["block"], address=x["token0.address"]
                )
                x["usd_price_token1"] = helper.get_price(
                    block=x["block"], address=x["token1.address"]
                )
            # save to csv
            # create a partial column list for d csv file
            csv_columns = []
            result_to_csv(
                result=status,
                folder=os.path.join(PARENT_FOLDER, "tests/impermanent"),
                filename="Rawdata_{}_{}_from_{:%Y-%m-%d}_{:%Y-%m-%d}__{}.csv".format(
                    network,
                    helper.symbol,
                    ini_date,
                    end_date,
                    helper.address,
                ),
                csv_columns=csv_columns,
            )
            # ### DEBUG ###############

            # progress
            progress_bar.set_description(
                f" Processing {network}'s {helper.dex} {helper.symbol}"
            )
            progress_bar.refresh()

            # create result
            result = helper.get_impermanent_data(ini_date=ini_date, end_date=end_date)

            # ### DEBUG: create Impermanent data csv ###############
            # create a partial column list for d csv file
            csv_columns = [
                # "block",
                # "timestamp",
                # "address",
                # "symbol",
                # "hodl_fifty_result_variation",
                # "hodl_token0_result_variation",
                # "hodl_token1_result_variation",
                # "hodl_proportion_result_variation",
                # "lping_result_variation",
            ]
            result_to_csv(
                result=result,
                folder=os.path.join(PARENT_FOLDER, "tests/impermanent"),
                filename="Impermanent_{}_{}_from_{:%Y-%m-%d}_{:%Y-%m-%d}__{}.csv".format(
                    network,
                    helper.symbol,
                    ini_date,
                    end_date,
                    helper.address,
                ),
                csv_columns=csv_columns,
            )

            # print result

            # update progress
            progress_bar.update(1)


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )
    # start time log
    _startime = datetime.utcnow()

    ########
    #  vars
    ########
    protocol = "gamma"
    network = "ethereum"
    dex = "uniswap_v3"

    days = 30
    end_date = datetime.utcnow()
    ini_date = end_date - timedelta(days=days)
    test_db_direct_info(
        network=network, protocol=protocol, ini_date=ini_date, end_date=end_date
    )

    ########
    ########
    # hypervisor_address = "0x02203f2351e7ac6ab5051205172d3f772db7d814"
    # block = 38525261

    # test_w3_hypervisor_obj(
    #     protocol=protocol,
    #     network=network,
    #     dex=dex,
    #     hypervisor_address=hypervisor_address,
    #     block=block,
    # )

    # end time log
    _timelapse = datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} minutes to complete".format(_timelapse.total_seconds() / 60)
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
