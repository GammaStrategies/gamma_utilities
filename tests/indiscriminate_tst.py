import contextlib
import sys
import os
import logging
from datetime import timezone
from web3 import Web3

from pathlib import Path
import tqdm
import concurrent.futures

from datetime import datetime, timedelta


# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION, STATIC_REGISTRY_ADDRESSES
from bins.general import general_utilities, file_utilities

from apps.database_feeder import (
    create_tokenBlocks_rewards,
    feed_prices,
    feed_prices_force_sqrtPriceX96,
)

from apps.feeds.status import feed_rewards_status

from apps.database_feeder_service import price_sequence_loop
from apps.database_checker import check_database, repair_blocks

from bins.mixed.price_utilities import price_scraper

from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_zyberswap_cached,
    gamma_hypervisor_registry,
)
from bins.w3.onchain_utilities.rewarders import (
    gamma_masterchef_registry,
    gamma_masterchef_v1,
    gamma_masterchef_rewarder,
)


from bins.log import log_helper
from bins.database.db_raw_direct_info import direct_db_hypervisor_info
from bins.database.common.db_collections_common import (
    database_local,
    database_global,
    db_collections_common,
)

from apps.database_checker import auto_get_prices, replace_quickswap_pool_dex_to_algebra

from bins.apis.coingecko_utilities import geckoterminal_price_helper

from bins.w3.onchain_utilities import rewarders
from bins.database.db_user_status import user_status_hypervisor_builderV2


def test_w3_hypervisor_obj(
    protocol: str, network: str, dex: str, hypervisor_address: str, block: int
):
    hypervisor = None
    if dex == "uniswapv3":
        hypervisor = gamma_hypervisor_cached(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "quickswap":
        hypervisor = gamma_hypervisor_quickswap_cached(
            address=hypervisor_address, network=network, block=block
        )
    elif dex == "zyberswap":
        hypervisor = gamma_hypervisor_zyberswap_cached(
            address=hypervisor_address, network=network, block=block
        )
    else:
        raise NotImplementedError(f" {dex} exchange has not been implemented yet")

    # test fees
    po = hypervisor.as_dict()
    test = ""


def test_prices(protocol="gamma"):
    network = "optimism"

    # force feed prices from already known using conversion
    # feed_prices_force_sqrtPriceX96(protocol=protocol, network=network, threaded=False)

    address = "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58"
    block = 80054403

    price_helper = price_scraper(cache=False)
    p = price_helper.get_price(network=network, token_id=address, block=block)
    po = ""


def test_price_sequence():
    price_sequence_loop(protocol="gamma", network="optimism")


def test_databaseChecker():
    check_database()


def get_hypervisor_addresses(
    network: str, protocol: str, user_address: str | None = None, dex: str | None = None
) -> list[str]:
    result = []
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
        _condition = {"address": user_address}
        if dex:
            _condition["dex"] = dex
        result = local_db_manager.get_distinct_items_from_database(
            collection_name="user_status",
            field="hypervisor_address",
            condition=_condition,
        )
    elif dex:
        _condition["dex"] = dex
        result = local_db_manager.get_distinct_items_from_database(
            collection_name="static",
            field="address",
            condition=_condition,
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
    result = [x for x in result if x not in blacklisted]
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
    csv_filename = f'{network}_{symbol}_{status_list[-1]["address"]}_from_{status_list[0]["block"]}_{status_list[-1]["block"]}.csv'

    csv_filename = os.path.join(folder, csv_filename)

    # remove file
    with contextlib.suppress(Exception):
        os.remove(csv_filename)
    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=status_list)


def result_to_csv(
    result: list[dict], folder: str, filename: str, csv_columns: list = None
):
    """save data to csv file

    Args:
        result (list[dict]): list of dicts
        folder (str): where to save
        filename (str):
    """

    if csv_columns is None:
        csv_columns = []
    csv_columns.extend([x for x in list(result[-1].keys()) if x not in csv_columns])

    csv_filename = os.path.join(folder, filename)

    # remove file
    with contextlib.suppress(Exception):
        os.remove(csv_filename)
    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=result)


def print_resultRow(
    resultRow: dict, ini_date: datetime, end_date: datetime, network: str
):
    logging.getLogger(__name__).info(" ")
    logging.getLogger(__name__).info(
        "{}'s {} ({}) {} day result from {:%Y-%m-%d %H:%M:%S } to {:%Y-%m-%d %H:%M:%S}".format(
            network,
            resultRow["symbol"],
            resultRow["dex"],
            (end_date - ini_date).days,
            ini_date,
            end_date,
        )
    )
    logging.getLogger(__name__).info(
        "  \t {}  \t last item dt: {:%Y-%m-%d %H:%M:%S } ".format(
            resultRow["address"],
            datetime.fromtimestamp(resultRow["timestamp"]),
        )
    )
    logging.getLogger(__name__).info(
        "  \t total USD locked: {:,.2f} ".format(
            resultRow["period_end_underlying_token0"]
            * resultRow["end_usd_price_token0"]
            + resultRow["period_end_underlying_token1"]
            * resultRow["end_usd_price_token1"]
        )
    )
    logging.getLogger(__name__).info(
        "  \t PnL {:,.2%}   [ {:,.2%} vs HODL]".format(
            resultRow["result_lping"], resultRow["result_LPvsHODL"]
        )
    )
    logging.getLogger(__name__).info(
        "  \t     feeAPR {:,.2%}   ".format(
            resultRow["result_period_Apr"],
        )
    )
    logging.getLogger(__name__).info(
        "  \t     Impermanent Price {:,.2%}  ".format(
            resultRow["result_period_ilg_price"],
        )
    )
    logging.getLogger(__name__).info(
        "  \t     Impermanent Other {:,.2%}  ".format(
            resultRow["result_period_ilg_others"],
        )
    )


def flatten_dict(my_dict: dict, existing_dict: dict = None, add_key: str = ""):
    if existing_dict is None:
        existing_dict = {}
    for k, v in my_dict.items():
        if not isinstance(v, dict):
            existing_dict[f"{add_key}.{k}" if add_key else k] = v
        else:
            flatten_dict(my_dict=v, existing_dict=existing_dict, add_key=k)
    return existing_dict


def test_db_direct_info(
    network: str,
    protocol: str = "gamma",
    dex: str | None = None,
    ini_date: datetime | None = None,
    end_date: datetime | None = None,
    folder: str = "tests",
    save_csv: bool = True,
    print_it: bool = True,
):
    # 1 day from now
    if not end_date:
        end_date = datetime.now(timezone.utc)
    if not ini_date:
        ini_date = end_date - timedelta(days=1)

    results_summary = []

    # get all hype addresses
    addresses = get_hypervisor_addresses(network=network, protocol=protocol)
    # addresses = ["0x8b6e73f17b613ce189be413f5dc435139f5fd45c"]
    with tqdm.tqdm(total=len(addresses)) as progress_bar:
        for address in addresses:
            # create helper
            helper = direct_db_hypervisor_info(
                hypervisor_address=address, network=network, protocol=protocol
            )

            # try avoiding the first hype week to minimize the chance to get a fix ratio transaction
            # ( at the ini life of hype, direct transfers without minting LP tokens are used to fix token ratios)
            # add one week to first operation time
            _first_time = datetime.fromtimestamp(
                helper.first_status["timestamp"], timezone.utc
            ) + timedelta(days=7)

            hype_ini_date = _first_time if ini_date < _first_time else ini_date
            hype_end_date = end_date

            # ### DEBUG: create raw data csv ###############
            # # query database
            # status = [
            #     helper.convert_hypervisor_status_fromDb(x)
            #     for x in helper.get_status_byDay(
            #         ini_timestamp=ini_date.timestamp(),
            #         end_timestamp=end_date.timestamp(),
            #     )
            # ]
            # # flatten dict so can be saved in columns
            # status = [flatten_dict(x, {}) for x in status]
            # # add usd prices
            # for x in status:
            #     x["usd_price_token0"] = helper.get_price(
            #         block=x["block"], address=x["token0.address"]
            #     )
            #     x["usd_price_token1"] = helper.get_price(
            #         block=x["block"], address=x["token1.address"]
            #     )
            # # save to csv
            # # create a partial column list for d csv file
            # csv_columns = []
            # if status:
            #     result_to_csv(
            #         result=status,
            #         folder=os.path.join(PARENT_FOLDER, folder),
            #         filename="Rawdata_{}_{}_from_{:%Y-%m-%d}_{:%Y-%m-%d}__{}.csv".format(
            #             network,
            #             helper.symbol,
            #             hype_ini_date,
            #             hype_end_date,
            #             helper.address,
            #         ),
            #         csv_columns=csv_columns,
            #     )
            # ### DEBUG ###############

            # progress
            progress_bar.set_description(
                f" Processing {network}'s {helper.dex} {helper.symbol}"
            )
            progress_bar.refresh()

            # create result
            try:
                result = helper.get_feeReturn_and_IL(
                    ini_date=hype_ini_date, end_date=hype_end_date
                )
                if result:
                    # ### DEBUG: create Impermanent data csv ###############
                    # create a partial column list for d csv file
                    csv_columns = []
                    if save_csv:
                        result_to_csv(
                            result=[flatten_dict(x, {}) for x in result],
                            folder=os.path.join(PARENT_FOLDER, folder),
                            filename="Impermanent_{}_{}_from_{:%Y-%m-%d}_{:%Y-%m-%d}__{}.csv".format(
                                network,
                                helper.symbol,
                                hype_ini_date,
                                hype_end_date,
                                helper.address,
                            ),
                            csv_columns=csv_columns,
                        )
                    if print_it:
                        # print result
                        print_resultRow(
                            resultRow=result[-1],
                            ini_date=hype_ini_date,
                            end_date=hype_end_date,
                            network=network,
                        )

                    # save last row to result summary
                    results_summary.append(result[-1])

            except Exception as err:
                logging.getLogger(__name__).error(
                    f" Unexpected error calc. fee return and IL  {err} "
                )

            # update progress
            progress_bar.update(1)

    if save_csv:
        # save summary
        result_to_csv(
            result=results_summary,
            folder=os.path.join(PARENT_FOLDER, folder),
            filename="summary_{}_from_{:%Y-%m-%d}_{:%Y-%m-%d}.csv".format(
                network,
                ini_date,
                end_date,
            ),
        )


def test_masterchef(network: str | None = None, dex: str | None = None):
    for network in [network] if network else STATIC_REGISTRY_ADDRESSES.keys():
        for dex in (
            [dex]
            if dex
            else STATIC_REGISTRY_ADDRESSES.get(network, {})
            .get("MasterChefV2Registry", {})
            .keys()
        ):
            # TODO: masterchef v1 registry

            # masterchef v2 registry
            address = STATIC_REGISTRY_ADDRESSES[network]["MasterChefV2Registry"][dex]

            # create masterchef registry
            registry = gamma_masterchef_registry(address, network)

            # get reward addresses from masterchef registry
            reward_registry_addresses = registry.get_masterchef_addresses()

            for registry_address in reward_registry_addresses:
                logging.getLogger(__name__).info(
                    f" ********  {network} testing Reward Registry {registry_address}  ******** "
                )

                reward_registry = gamma_masterchef_v1(
                    address=registry_address, network=network
                )

                for i in range(reward_registry.poolLength):
                    # TODO: how to scrape rid ?
                    for rid in range(100):
                        try:
                            # get hypervisor address
                            hypervisor_address = reward_registry.lpToken(pid=i)
                            # get reward address
                            rewarder_address = reward_registry.getRewarder(
                                pid=i, rid=rid
                            )

                            # get rewarder
                            rewarder = gamma_masterchef_rewarder(
                                address=rewarder_address, network=network
                            )

                            result = rewarder.as_dict(convert_bint=True)
                            # add hypervisor address to rewarder
                            result["hypervisor_address"] = hypervisor_address

                            # get rewarder info
                            # rewarder_address = rewarder_address
                            # rewarder_token = rewarder.rewardToken
                            # rewarder_registry = rewarder.masterchef_v2
                            # rewarder_RxSec = rewarder.rewardPerSecond
                            # rewarder_totalAllocPoint = rewarder.totalAllocPoint
                            # rewarder_poolLength = rewarder.poolLength

                            logging.getLogger(__name__).info(
                                f"           {rewarder_address}  ok "
                            )
                        except ValueError:
                            # no more rid's
                            # print(sys.exc_info()[0])
                            # logging.getLogger(__name__).info(
                            #     f"           {rewarder_address}  not ok (no more rid's) "
                            # )
                            break
                        except Exception:
                            if rewarder_address:
                                logging.getLogger(__name__).info(
                                    f"           {rewarder_address}  not ok (error)"
                                )
                            else:
                                logging.getLogger(__name__).info(
                                    f"            ------------ not ok (error)"
                                )
                            logging.getLogger(__name__).error(sys.exc_info()[0])
                            break


from bins.w3.onchain_utilities.rewarders import zyberswap_masterchef_v1


def test_zyberchef():
    network = "arbitrum"
    dex = "zyberswap"
    for address in STATIC_REGISTRY_ADDRESSES[network]["zyberswap_v1_masterchefs"]:
        # create database connection

        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{network}_{protocol}"
        local_db = database_local(mongo_url=mongo_url, mongo_db=db_name)
        global_db = database_global(mongo_url=mongo_url)
        price_helper = price_scraper(cache=False)

        # create zyberchef
        zyberchef = zyberswap_masterchef_v1(address=address, network=network)
        for pid in zyberchef.poolLength():
            #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
            pinfo = zyberchef.poolInfo(pid)
            # check lp token corresponds to gamma rewards: search address in database
            if hype := local_db.get_items_from_database(
                collection_name="status",
                find={"address": pinfo["lpToken"].lower()},
                sort=[("block", -1)],
                limit=2,
            ):
                hype = hype[
                    -1
                ]  # first hype is the most recent hype status found. Pick the one before that to make sure price is already scraped.
                #
                # option 1)   rewardRate * secondsPerYear * price of token) / (totalSupply * price per LP Token)
                #
                poolRewardsPerSec = zyberchef.poolRewardsPerSec(
                    pid
                )  # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
                secondsPerYear = 365 * 24 * 60 * 60

                # get LPtoken price
                price_token0 = global_db.get_price_usd(
                    network=network,
                    address=hype["pool"]["token0"]["address"],
                    block=hype["block"],
                )
                price_token1 = global_db.get_price_usd(
                    network=network,
                    address=hype["pool"]["token1"]["address"],
                    block=hype["block"],
                )
                price_lpToken = (
                    price_token0
                    * (
                        int(hype["totalAmounts"]["total0"])
                        / (10 ** int(hype["pool"]["token0"]["decimals"]))
                    )
                    + price_token1
                    * (
                        int(hype["totalAmounts"]["total1"])
                        / (10 ** int(hype["pool"]["token1"]["decimals"]))
                    )
                ) / (int(hype["totalSupply"]["totalSupply"]) / (10 ** hype["decimals"]))

                # get price of poolRewardsPerSec["address"]
                poolRewardsPerSec["usd_price"] = []
                for address, symbol, decimals, rewardsPerSec in zip(
                    poolRewardsPerSec["addresses"],
                    poolRewardsPerSec["symbols"],
                    poolRewardsPerSec["decimals"],
                    poolRewardsPerSec["rewardsPerSec"],
                ):
                    if rewardsPerSec:
                        # get price of token
                        price = price_helper.get_price(
                            network=network,
                            token_id=address.lower(),
                            block=hype["block"],
                        ) or global_db.get_price_usd(
                            network=network,
                            address=address.lower(),
                            block=hype["block"],
                        )

                        poolRewardsPerSec["usd_price"].append(price)


from apps.feeds.static import feed_rewards_static


def test_feed_rewards_static():
    network = "binance"
    dex = "thena"
    feed_rewards_static(network=network, dex=dex)
    network = "arbitrum"
    dex = "zyberswap"
    feed_rewards_static(network=network, dex=dex)


def test_feed_rewards_status():
    network = "arbitrum"
    zyberswap_masterchef = rewarders.zyberswap_masterchef_v1(
        address="0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD",
        network=network,
        block=70464630,
        timestamp=1674692152,
    )
    test = zyberswap_masterchef.poolInfo(pid=8)

    feed_rewards_status(network="ethereum")


from web3._utils.contracts import prepare_transaction


def custom_batch_request():
    hype = gamma_hypervisor(
        address="0xa3ecb6e941e773c6568052a509a04cf455a752ae", network="ethereum"
    )
    function01 = hype._contract.functions.name().call()

    po = ""


def test_autoRpccalls(
    network="ethereum", address="0x35abccd8e577607275647edab08c537fa32cc65e"
):
    print(" ")
    print(f"  Testing {address} on {network}")

    private_rpcUrls = [CONFIGURATION["sources"]["web3Providers"][network]]
    hype = gamma_hypervisor(address=address.lower(), network=network)
    rpckeys = ["public"]
    # place first call
    tmp_public = hype.call_function_autoRpc("getTotalAmounts", rpcKey_names=rpckeys)
    # if tmp_public is not None:
    #    result.append(tmp_public)
    # save first var
    first_block = hype.block
    first_timestamp = hype._timestamp

    for i in range(20):
        # modify hypervisor block
        hype.block = hype.block - 22500
        block_data = hype._w3.eth.get_block(hype.block)
        hype._timestamp = block_data.timestamp
        # place subsequent calls
        tmp_public = hype.call_function_autoRpc("getTotalAmounts", rpcKey_names=rpckeys)
        if tmp_public is not None:
            # compare with private call
            tmp_private = hype.call_function("getTotalAmounts", rpcUrls=private_rpcUrls)
            # result.append(tmp_public)
            print(
                f" {network} tested block:{hype.block}  timestamp:{hype._timestamp}  ({(first_timestamp-hype._timestamp)/(60*60)} hours back)"
            )
            if tmp_public != tmp_private:
                print(" !!!!!!!!!!!!!!! MISMATCH  !!!!!!!!!!!!!!! ")
                print(f"               public result: {tmp_public}")
                print(f"              private result: {tmp_private}")
                print(" ")
        else:
            print(f" no data found at block {hype.block}")
            break
    last_block = hype.block
    last_timestamp = hype._timestamp
    print(
        f" {first_block-last_block} blocks back -> {(first_timestamp-last_timestamp)/(60*60)} hours back"
    )
    # fdata = hype.call_function_autoRpc(
    #     "balanceOf",
    #     None,
    #     Web3.toChecksumAddress("0x450E5dd66c3c243bBf3b07379aF7E8B261579970"),
    # )

    po = ""


def test_price_geckterm():
    price_helper = geckoterminal_price_helper()
    price = price_helper.get_price_now(
        network="binance",
        token_address="0x3cd55356433c89e50dc51ab07ee0fa0a95623d53",
    )
    pp = ""


def test_user_status2():
    network = "polygon"
    protocol = "gamma"
    user_status_builder = user_status_hypervisor_builderV2(
        network=network, protocol=protocol
    )

    network_user_status = user_status_builder.calculate_status(
        user_address="0x93ab7580493da8e78a31c31250d3c5524b7ffd0e"
    )
    pp = ""


def calculate_missing():
    network = "polygon"
    protocol = "gamma"
    hypervisor_address = "0xf874d4957861e193aec9937223062679c14f9aca"
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    timestamps = database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_distinct_items_from_database(
        collection_name="status",
        field="timestamp",
        condition={"address": hypervisor_address},
    )

    missing_timestamps = general_utilities.get_missing_integers(timestamps, 1200)

    print(
        f"  missing_timestamps: {len(missing_timestamps)}  [ vs {len(timestamps)} found in db]"
    )

    po = ""


# START ####################################################################################################################
from datetime import timezone

if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    repair_blocks()

    kwargs_list = [
        {
            "network": "arbitrum",
            "address": "0xD06E6a71121BfD6c1079Bd0b4B231a92022953c9".lower(),
        },
        {
            "network": "optimism",
            "address": "0x2d6B26f430F261B77D14C495585116aA579b7217".lower(),
        },
        {
            "network": "polygon",
            "address": "0xb81686295822B639b647D3B421cD5e09Af700AdA".lower(),
        },
        {
            "network": "binance",
            "address": "0x1C2Ac2Ea1063395c34D2B5066ebC7F6745196e92".lower(),
        },
        {
            "network": "celo",
            "address": "0x897D907680D4890B91f3698f6878Ba56D2BF605c".lower(),
        },
        {
            "network": "ethereum",
            "address": "0x35abccd8e577607275647edab08c537fa32cc65e",
        },
    ]
    for kwargs in kwargs_list:
        test_autoRpccalls(**kwargs)

    test_feed_rewards_static()

    replace_quickswap_pool_dex_to_algebra(network="polygon")
    # test_prices()
    # test_price_sequence()

    test_w3_hypervisor_obj(
        protocol="gamma",
        network="polygon",
        dex="uniswapv3",
        hypervisor_address="0xFEa715aB7E1DE3640CD0662f6af0f9B25934E753".lower(),
        block=40949649,
    )

    ########
    #  vars
    ########
    protocol = "gamma"
    # networks = ["ethereum", "polygon", "optimism", "arbitrum"]
    networks = ["polygon"]
    dex = "quickswap"  # None
    months = 0
    days = 30 * (months or 1)
    end_date = datetime.now(timezone.utc)
    ini_date = end_date - timedelta(days=days)

    save_csv = False
    print_it = True

    for network in networks:
        folder = f"tests/results/{months}month_{network}"

        test_db_direct_info(
            network=network,
            protocol=protocol,
            dex=dex,
            ini_date=ini_date,
            end_date=end_date,
            folder=folder,
            save_csv=save_csv,
            print_it=print_it,
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

    # test_db_direct_info(
    #     network=network,
    #     protocol=protocol,
    #     ini_date=datetime(2022, 3, 30),
    #     end_date=datetime(2022, 12, 31),
    #     folder="tests/results/",
    # )

    # end time log
    _timelapse = datetime.now(timezone.utc) - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} minutes to complete".format(_timelapse.total_seconds() / 60)
    )
    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
