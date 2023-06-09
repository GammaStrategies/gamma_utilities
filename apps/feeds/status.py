import contextlib
from datetime import datetime, timezone
import logging
import concurrent.futures
import tqdm


from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import get_price_from_db
from bins.formulas.apr import calculate_rewards_apr
from bins.general.enums import Chain, Protocol, rewarderType
from bins.w3.protocols.beamswap.rewarder import beamswap_masterchef_v2

from bins.w3.protocols.thena.rewarder import thena_gauge_v2
from bins.w3.protocols.zyberswap.rewarder import zyberswap_masterchef_v1

from bins.w3.builders import (
    build_db_hypervisor,
)
from bins.w3.protocols.general import erc20_cached

### Status ######################


def feed_hypervisor_status(
    protocol: str, network: str, rewrite: bool = False, threaded: bool = True
):
    """Creates hypervisor status at all operations block and block-1
            + every 20 minutes after last found status block ( if those minutes have already passed )

    Args:
        protocol (str):
        network (str):
        rewrite (bool): rewrite all status
        threaded: (bool):
    """

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} hypervisors status information {'[rewriting all]' if rewrite else ''}"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # set local database name and create manager
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # apply filters
    hypes_not_included: list = [
        x.lower()
        for x in (
            CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
        )
        .get("hypervisors_not_included", {})
        .get(network, [])
    ]
    logging.getLogger(__name__).debug(f"   excluding hypervisors: {hypes_not_included}")
    # get all static hypervisor info and convert it to dict
    static_info = {
        x["address"]: x
        for x in local_db.get_items(collection_name="static")
        if x["address"] not in hypes_not_included
    }

    # create a unique list of blocks addresses from database to be processed including:
    #       operation blocks and their block-1 relatives
    #       block every X min
    toProcess_block_address = {}
    for x in local_db.get_unique_operations_addressBlock(
        topics=["deposit", "withdraw", "zeroBurn", "rebalance"]
    ):
        # add operation addressBlock to be processed
        toProcess_block_address[f"""{x["address"]}_{x["block"]}"""] = {
            "address": x["address"],
            "block": x["block"],
            "fees_metadata": "ini",
        }
        # add block -1
        toProcess_block_address[f"""{x["address"]}_{x["block"]-1}"""] = {
            "address": x["address"],
            "block": x["block"] - 1,
            "fees_metadata": "end",
        }

    # add latest block to all hypervisors every 20 min
    try:
        if (
            datetime.now(timezone.utc).timestamp()
            - local_db.get_max_field(collection="status", field="timestamp")[0]["max"]
        ) > 60 * 20:
            latest_block = (
                erc20_cached(
                    address="0x0000000000000000000000000000000000000000",
                    network=network,
                )
                ._w3.eth.get_block("latest")
                .number
            )

            logging.getLogger(__name__).debug(
                f" Adding the latest block [{latest_block}] to all addresses for status to be scraped "
            )

            for address in static_info:
                toProcess_block_address[f"""{address}_{latest_block}"""] = {
                    "address": address,
                    "block": latest_block,
                    "fees_metadata": "mid",
                }
    except IndexError:
        logging.getLogger(__name__).debug(
            f" Seems like there is no {network}'s {protocol} status data in db. Continue without adding latest block to all addresses for status to be scraped"
        )
    except Exception:
        logging.getLogger(__name__).exception(
            " unexpected error while adding new blocks to status scrape process "
        )

    if rewrite:
        # rewrite all address blocks
        processed_blocks = {}
    else:
        # get a list of blocks already processed
        processed_blocks = {
            f"""{x["address"]}_{x["block"]}""": x
            for x in local_db.get_unique_status_addressBlock()
        }

    logging.getLogger(__name__).debug(
        f"   Total address blocks {len(toProcess_block_address)} ->  Already processed {len(processed_blocks)} "
    )

    # remove already processed blocks
    for k in processed_blocks:
        try:
            toProcess_block_address.pop(k)
        except KeyError as err:
            # there are many more status blocks than operations ...
            # not to worry
            # logging.getLogger(__name__).warning(
            #     f" Could not find status block address key {k} in operations"
            # )
            pass
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error found while construction block addresses to feed db with hype status.  err-> {e}"
            )

    # set log list of hypervisors with errors
    _errors = 0

    with tqdm.tqdm(total=len(toProcess_block_address), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    item["address"],
                    network,
                    item["block"],
                    static_info[item["address"]]["dex"],
                    # False,
                )
                for item in toProcess_block_address.values()
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(
                    lambda p: create_and_save_hypervisor_status(*p), args
                ):
                    if not result:
                        # error found
                        _errors += 1

                    # else:
                    #     # progress
                    #     progress_bar.set_description(
                    #         f' {result.get("address", " ")} processed '
                    #     )
                    #     progress_bar.refresh()
                    #     # add hypervisor status to database
                    #     local_db.set_status(data=result)
                    # update progress
                    progress_bar.update(1)
        else:
            # get operations from database
            for item in toProcess_block_address.values():
                progress_bar.set_description(
                    f' 0x..{item.get("address", "    ")[-4:]} at block {item.get("block", "")} to be processed'
                )

                progress_bar.refresh()
                result = build_db_hypervisor(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                    dex=static_info[item["address"]]["dex"],
                    static_mode=False,
                )
                if not create_and_save_hypervisor_status(
                    address=item["address"],
                    network=network,
                    block=item["block"],
                    dex=static_info[item["address"]]["dex"],
                ):
                    # error found
                    _errors += 1
                # if result != None:
                #     # add hypervisor status to database
                #     local_db.set_status(data=result)
                # else:
                #     # error found
                #     _errors += 1
                # update progress
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisor status could not be scraped due to errors".format(
                    _errors,
                    len(toProcess_block_address),
                    (_errors / len(toProcess_block_address))
                    if toProcess_block_address
                    else 0,
                )
            )


def create_and_save_hypervisor_status(
    address: str, network: str, block: int, dex: str
) -> bool:
    """create hyperivor status at the specified block and save it into the database

    Args:
        address (str):
        network (Chain):
        block (int):
        dex (str):

    Returns:
        bool: saved or not
    """
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    db_name = f"{network}_gamma"

    try:
        # create hype and save

        if hype := build_db_hypervisor(
            address=address, network=network, block=block, dex=dex
        ):
            # save hype
            database_local(mongo_url=mongo_url, db_name=db_name).set_status(data=hype)
            # return success
            return True
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" unexpected error while creating and saving hype status {address} at block {block} -> {e}"
        )

    # return failure
    return False


## Rewards status


def feed_rewards_status(
    network: str | None = None, dex: str | None = None, protocol: str = "gamma"
):
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} rewards status information"
    )

    # set local database name and create manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)

    # get a list of static rewarders linked to hypes
    to_be_processed_reward_static = local_db.get_items_from_database(
        collection_name="rewards_static", find={}
    )

    with tqdm.tqdm(
        total=len(to_be_processed_reward_static), leave=False
    ) as progress_bar:
        for rewarder_static in to_be_processed_reward_static:
            for reward in feed_rewards_status_loop(rewarder_static):
                # only save rewards with positive rewards per second
                if reward:
                    if int(reward["rewards_perSecond"]) > 0:
                        # add to database
                        local_db.set_rewards_status(data=reward)
                    else:
                        logging.getLogger(__name__).debug(
                            f" {network}'s {reward['rewarder_address']} {reward['block']} not saved due to 0 rewards per second"
                        )
                # else:
                # no rewards found to be scraped

            # progress
            progress_bar.set_description(
                f' {rewarder_static.get("hypervisor_symbol", " ")} processed '
            )
            progress_bar.update(1)


def feed_rewards_status_loop(rewarder_static: dict):
    network = rewarder_static["network"]
    # set local database name and create manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"  # TODO: change hardcoded db name to be dynamic

    batch_size = 50000

    # already processed blocks for this hype rewarder combination
    processed_blocks = database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_distinct_items_from_database(
        collection_name="rewards_status",
        field="block",
        condition={
            "hypervisor_address": rewarder_static["hypervisor_address"],
            "rewarder_address": rewarder_static["rewarder_address"],
        },
    )

    # to be processed as per the hypervisor status
    to_process_hypervisor_status = database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_items_from_database(
        collection_name="status",
        find={
            "address": rewarder_static["hypervisor_address"],
            "$and": [
                {"block": {"$gte": rewarder_static["block"]}},
                {"block": {"$nin": processed_blocks}},
            ],
        },
        batch_size=batch_size,
    )

    # get the last 20 hype status to process
    if len(to_process_hypervisor_status) > 20:
        logging.getLogger(__name__).debug(
            f"  Found {len(to_process_hypervisor_status)} status blocks to be scraped  but only the last 20 will be processed << TODO: change this >>"
        )
        to_process_hypervisor_status = to_process_hypervisor_status[-20:]

    result = []
    # process
    logging.getLogger(__name__).debug(
        f"    -> {len(to_process_hypervisor_status)} status blocks to be scraped for {network}'s rewarder {rewarder_static['rewarder_address']} on hype {rewarder_static['hypervisor_address']}"
    )

    # prepare arguments
    args = (
        (hypervisor_status, rewarder_static, network)
        for hypervisor_status in to_process_hypervisor_status
    )
    with concurrent.futures.ThreadPoolExecutor() as ex:
        for result_item in ex.map(
            lambda p: create_reward_status_from_hype_status(*p), args
        ):
            # append only if not empty-> will be empty when processing static masterchef s
            if result:
                result.append(result_item)

    return result


def create_reward_status_from_hype_status(
    hypervisor_status: dict, rewarder_static: dict, network: str
) -> list:
    result = []
    rewards_data = []
    try:
        if rewarder_static["rewarder_type"] in [
            rewarderType.ZYBERSWAP_masterchef_v1,
            rewarderType.ZYBERSWAP_masterchef_v1_rewarder,
        ]:
            if rewarder_static["rewarder_type"] == rewarderType.ZYBERSWAP_masterchef_v1:
                # do nothing because this is the reward given by the masterchef and will be processed once we get to the rewarder ( thru the masterchef itself..)
                return None

            # create masterchef object:  USE address == rewarder_registry on masterchef for this type of rewarder
            zyberswap_masterchef = zyberswap_masterchef_v1(
                address=rewarder_static["rewarder_registry"],
                network=network,
                block=hypervisor_status["block"],
                timestamp=hypervisor_status["timestamp"],
            )
            # get rewards status
            rewards_data = zyberswap_masterchef.get_rewards(
                hypervisor_addresses=[rewarder_static["hypervisor_address"]],
                pids=rewarder_static["rewarder_refIds"],
                convert_bint=True,
            )

        elif rewarder_static["rewarder_type"] == rewarderType.THENA_gauge_v2:
            thena_gauge = thena_gauge_v2(
                address=rewarder_static["rewarder_address"],
                network=network,
                block=hypervisor_status["block"],
                timestamp=hypervisor_status["timestamp"],
            )

            # get rewards directly from gauge ( rewarder ).  Warning-> will not contain rewarder_registry field!!
            if rewards_from_gauge := thena_gauge.get_rewards(convert_bint=True):
                # add rewarder registry address
                for reward in rewards_from_gauge:
                    reward["rewarder_registry"] = rewarder_static["rewarder_registry"]

                # add to returnable data
                rewards_data += rewards_from_gauge
        elif rewarder_static["rewarder_type"] in [
            rewarderType.BEAMSWAP_masterchef_v2,
            rewarderType.BEAMSWAP_masterchef_v2_rewarder,
        ]:
            if rewarder_static["rewarder_type"] == rewarderType.BEAMSWAP_masterchef_v2:
                # do nothing because this is the reward given by the masterchef and will be processed once we get to the rewarder ( thru the masterchef itself..)
                return None

            # create masterchef object:   USE address == rewarder_registry on masterchef for this type of rewarder
            masterchef = beamswap_masterchef_v2(
                address=rewarder_static["rewarder_registry"],
                network=network,
                block=hypervisor_status["block"],
                timestamp=hypervisor_status["timestamp"],
            )
            # get rewards status
            rewards_data = masterchef.get_rewards(
                hypervisor_addresses=[rewarder_static["hypervisor_address"]],
                pids=rewarder_static["rewarder_refIds"],
                convert_bint=True,
            )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error constructing {network}'s {rewarder_static['rewarder_address']} rewarder data. error-> {e}"
        )

    logging.getLogger(__name__).debug(
        f"    -> Filling prices and APR for {network}'s rewarder {rewarder_static['rewarder_address']}"
    )

    for reward_data in rewards_data:
        # token_prices
        try:
            rewardToken_price = get_price_from_db(
                network=network,
                block=hypervisor_status["block"],
                token_address=reward_data["rewardToken"],
            )
            hype_token0_price = get_price_from_db(
                network=network,
                block=hypervisor_status["block"],
                token_address=hypervisor_status["pool"]["token0"]["address"],
            )
            hype_token1_price = get_price_from_db(
                network=network,
                block=hypervisor_status["block"],
                token_address=hypervisor_status["pool"]["token1"]["address"],
            )
            # hypervisor price per share
            hype_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
                10 ** hypervisor_status["pool"]["token0"]["decimals"]
            )
            hype_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
                10 ** hypervisor_status["pool"]["token1"]["decimals"]
            )
            hype_price_per_share = (
                hype_token0_price * hype_total0 + hype_token1_price * hype_total1
            ) / (
                int(hypervisor_status["totalSupply"])
                / (10 ** hypervisor_status["decimals"])
            )

            if int(reward_data["total_hypervisorToken_qtty"]) and hype_price_per_share:
                # if there is hype qtty staked and price per share
                apr = calculate_rewards_apr(
                    token_price=rewardToken_price,
                    token_reward_rate=int(reward_data["rewards_perSecond"])
                    / (10 ** reward_data["rewardToken_decimals"]),
                    total_lp_locked=int(reward_data["total_hypervisorToken_qtty"])
                    / (10 ** hypervisor_status["decimals"]),
                    lp_token_price=hype_price_per_share,
                )
            else:
                # no apr if no hype qtty staked or no price per share
                apr = 0

            # add status fields ( APR )
            reward_data["hypervisor_symbol"] = hypervisor_status["symbol"]
            reward_data["dex"] = hypervisor_status["dex"]
            reward_data["apr"] = apr
            reward_data["rewardToken_price_usd"] = rewardToken_price
            reward_data["token0_price_usd"] = hype_token0_price
            reward_data["token1_price_usd"] = hype_token1_price
            reward_data["hypervisor_share_price_usd"] = hype_price_per_share

            # add to result
            result.append(reward_data)

        except Exception as e:
            logging.getLogger(__name__).error(
                f" Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
            )
            logging.getLogger(__name__).debug(
                f" Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
            )

    logging.getLogger(__name__).debug(
        f"    -> Done processing {network}'s rewarder {rewarder_static['rewarder_address']}"
    )

    return result
