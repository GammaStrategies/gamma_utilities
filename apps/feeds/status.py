import contextlib
from datetime import datetime, timezone
import logging
import concurrent.futures
import tqdm
from bins.apis.angle_merkle import angle_merkle_wraper


from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_hypervisor_status
from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import get_from_localdb, get_price_from_db
from bins.errors.general import ProcessingError
from bins.formulas.apr import calculate_rewards_apr
from bins.formulas.dex_formulas import (
    pool_token_amounts_from_current_price,
    select_ramses_apr_calc_deviation,
)
from bins.general.enums import Chain, Protocol, rewarderType, text_to_chain
from bins.w3.protocols.angle.rewarder import angle_merkle_distributor_creator
from bins.w3.protocols.beamswap.rewarder import beamswap_masterchef_v2

from bins.w3.protocols.thena.rewarder import thena_gauge_v2
from bins.w3.protocols.zyberswap.rewarder import zyberswap_masterchef_v1

from bins.w3.protocols.ramses.hypervisor import gamma_hypervisor as ramses_hypervisor

from bins.w3.builders import (
    build_db_hypervisor,
    build_erc20_helper,
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

        toProcess_block_address[
            create_id_hypervisor_status(
                hypervisor_address=x["address"], block=x["block"]
            )
        ] = {
            "address": x["address"],
            "block": x["block"],
            "fees_metadata": "ini",
        }
        # add block -1
        toProcess_block_address[
            create_id_hypervisor_status(
                hypervisor_address=x["address"], block=x["block"] - 1
            )
        ] = {
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
                toProcess_block_address[
                    create_id_hypervisor_status(
                        hypervisor_address=address, block=latest_block
                    )
                ] = {
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
            create_id_hypervisor_status(
                hypervisor_address=x["address"], block=x["block"]
            ): x
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


## Rewards status:
# TODO: embed into classes


def feed_rewards_status(network: str | None = None, protocol: str = "gamma"):
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
            for reward in feed_rewards_status_loop(
                network=network, rewarder_static=rewarder_static
            ):
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


def feed_rewards_status_loop(
    network: str, rewarder_static: dict, rewrite: bool = False
) -> list[dict]:
    """
        feed rewards status for a specific rewarder

    Args:
        network (str):
        rewarder_static (dict):
        rewrite (bool, optional): rewrite all status. Defaults to False.

    Returns:
        list[dict]: list of rewards status
    """
    # set local database name and create manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"  # TODO: change hardcoded db name to be dynamic

    batch_size = 50000

    # already processed blocks for this hype rewarder combination
    processed_blocks = (
        database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_distinct_items_from_database(
            collection_name="rewards_status",
            field="block",
            condition={
                "hypervisor_address": rewarder_static["hypervisor_address"],
                "rewarder_address": rewarder_static["rewarder_address"],
            },
        )
        if not rewrite
        else []
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

    # limit the umber of status to process
    max_items = 20
    if len(to_process_hypervisor_status) > max_items:
        logging.getLogger(__name__).debug(
            f"  Found {len(to_process_hypervisor_status)} status blocks to be scraped but only the last {max_items} will be processed"
        )
        to_process_hypervisor_status = to_process_hypervisor_status[-max_items:]

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
            # add only if not empty-> will be empty when processing static masterchef s
            if result_item:
                result += result_item

    return result


def create_reward_status_from_hype_status(
    hypervisor_status: dict, rewarder_static: dict, network: str
) -> list:
    rewards_data = []

    # make sure block is after rewarder creation
    if hypervisor_status["block"] < rewarder_static["block"]:
        logging.getLogger(__name__).debug(
            f" {hypervisor_status['address']} hype block {hypervisor_status['block']} is before rewarder creation block {rewarder_static['block']}. Not processing reward status."
        )
        # create a dummy reward status zero so it gets discarded and not processed again
        rewards_data.append(
            {
                "rewards_perSecond": 0,
                "rewarder_address": rewarder_static["rewarder_address"],
                "block": hypervisor_status["block"],
            }
        )
        return rewards_data
    # make sure hypervisor has supply
    elif int(hypervisor_status["totalSupply"]) == 0:
        logging.getLogger(__name__).debug(
            f" {hypervisor_status['address']} hype at block {hypervisor_status['block']} has zero supply. Not processing reward status."
        )
        # create a dummy reward status zero so it gets discarded and not processed again
        rewards_data.append(
            {
                "rewards_perSecond": 0,
                "rewarder_address": rewarder_static["rewarder_address"],
                "block": hypervisor_status["block"],
            }
        )
        return rewards_data

    # start process
    try:
        if rewarder_static["rewarder_type"] in [
            rewarderType.ZYBERSWAP_masterchef_v1,
            rewarderType.ZYBERSWAP_masterchef_v1_rewarder,
        ]:
            # get rewards status
            rewards_data = create_rewards_status_zyberswap(
                network=network,
                rewarder_static=rewarder_static,
                hypervisor_status=hypervisor_status,
            )

        elif rewarder_static["rewarder_type"] == rewarderType.THENA_gauge_v2:
            rewards_data = create_rewards_status_thena(
                network=network,
                rewarder_static=rewarder_static,
                hypervisor_status=hypervisor_status,
            )

        elif rewarder_static["rewarder_type"] in [
            rewarderType.BEAMSWAP_masterchef_v2,
            rewarderType.BEAMSWAP_masterchef_v2_rewarder,
        ]:
            rewards_data = create_rewards_status_beamswap(
                network=network,
                rewarder_static=rewarder_static,
                hypervisor_status=hypervisor_status,
            )

        elif rewarder_static["rewarder_type"] in [
            rewarderType.ANGLE_MERKLE,
        ]:
            rewards_data = create_rewards_status_angle_merkle(
                network=network,
                rewarder_static=rewarder_static,
                hypervisor_status=hypervisor_status,
            )

        elif rewarder_static["rewarder_type"] in [
            rewarderType.RAMSES_v2,
        ]:
            rewards_data = create_rewards_status_ramses(
                chain=text_to_chain(network),
                rewarder_static=rewarder_static,
                hypervisor_status=hypervisor_status,
            )

    except ProcessingError as e:
        logging.getLogger(__name__).error(
            f" Unexpected error constructing {network}'s {rewarder_static['rewarder_address']} rewarder data. error-> {e.message}"
        )
        # TODO: code
        # may be better to keep processed items in queue indefinitely
        # if e.action=="remove":
        #     # do not remove but set count to 100 to filter those out
        # #     # create a dummy reward status zero so it gets discarded and not processed again
        # #     rewards_data = [{"rewards_perSecond":0}]
        #     pass

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error constructing {network}'s {rewarder_static['rewarder_address']} rewarder data. error-> {e}"
        )

    logging.getLogger(__name__).debug(
        f"    -> Done processing {network}'s rewarder {rewarder_static['rewarder_address']}  registry: {rewarder_static['rewarder_registry']}"
    )

    return rewards_data


def get_reward_pool_prices(
    network: str, block: int, reward_token: str, token0: str, token1: str
) -> tuple[float, float, float]:
    """Get reward token, token0 and token1 prices

    Args:
        network (str):
        block (int):
        reward_token (str): reward token address
        token0 (str): pool token0 address
        token1 (str): pool token1 address

    Returns:
        tuple[float, float, float]:  rewardToken_price, hype_token0_price, hype_token1_price
    """
    rewardToken_price = get_price_from_db(
        network=network,
        block=block,
        token_address=reward_token,
    )
    hype_token0_price = get_price_from_db(
        network=network,
        block=block,
        token_address=token0,
    )
    hype_token1_price = get_price_from_db(
        network=network,
        block=block,
        token_address=token1,
    )

    return rewardToken_price, hype_token0_price, hype_token1_price


def get_hypervisor_price_per_share(
    hypervisor_status: dict, token0_price: float, token1_price: float
) -> float:
    # do not calculate when no supply
    if int(hypervisor_status["totalSupply"]) == 0:
        return 0.0

    # get total amounts in pool ( owed and liquidity providing)
    hype_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
        10 ** hypervisor_status["pool"]["token0"]["decimals"]
    )
    hype_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
        10 ** hypervisor_status["pool"]["token1"]["decimals"]
    )

    # get uncollected fees for the pool at this block
    uncollected_fees0 = float(hypervisor_status["fees_uncollected"]["qtty_token0"])
    uncollected_fees1 = float(hypervisor_status["fees_uncollected"]["qtty_token1"])

    # create an easy to debug totals
    total_token0 = hype_total0 + uncollected_fees0
    total_token1 = hype_total1 + uncollected_fees1

    # calculate price per share
    return (token0_price * total_token0 + token1_price * total_token1) / (
        int(hypervisor_status["totalSupply"]) / (10 ** hypervisor_status["decimals"])
    )


def reward_add_status_fields(
    reward_data: dict,
    hypervisor_status: dict,
    apr: float,
    rewardToken_price: float,
    token0_price: float,
    token1_price: float,
    hype_price_per_share: float | None = None,
) -> dict:
    # add status fields ( APR )
    reward_data["hypervisor_symbol"] = hypervisor_status["symbol"]
    reward_data["dex"] = hypervisor_status["dex"]
    reward_data["apr"] = apr
    reward_data["rewardToken_price_usd"] = rewardToken_price
    reward_data["token0_price_usd"] = token0_price
    reward_data["token1_price_usd"] = token1_price
    reward_data[
        "hypervisor_share_price_usd"
    ] = hype_price_per_share or get_hypervisor_price_per_share(
        hypervisor_status=hypervisor_status,
        token0_price=token0_price,
        token1_price=token1_price,
    )

    return reward_data


def add_apr_process01(
    network: str,
    hypervisor_status: dict,
    reward_data: dict,
    rewardToken_price: float | None = None,
    hype_token0_price: float | None = None,
    hype_token1_price: float | None = None,
    hype_price_per_share: float | None = None,
) -> dict:
    # token_prices
    if not rewardToken_price or not hype_token0_price or not hype_token1_price:
        (
            rewardToken_price,
            hype_token0_price,
            hype_token1_price,
        ) = get_reward_pool_prices(
            network=network,
            block=hypervisor_status["block"],
            reward_token=reward_data["rewardToken"],
            token0=hypervisor_status["pool"]["token0"]["address"],
            token1=hypervisor_status["pool"]["token1"]["address"],
        )

    # hypervisor price per share
    if not hype_price_per_share:
        hype_price_per_share = get_hypervisor_price_per_share(
            hypervisor_status=hypervisor_status,
            token0_price=hype_token0_price,
            token1_price=hype_token1_price,
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

    reward_data = reward_add_status_fields(
        reward_data=reward_data,
        hypervisor_status=hypervisor_status,
        apr=apr,
        rewardToken_price=rewardToken_price,
        token0_price=hype_token0_price,
        token1_price=hype_token1_price,
        hype_price_per_share=hype_price_per_share,
    )

    return reward_data


# specific rewarder functions
def create_rewards_status_zyberswap(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    if rewarder_static["rewarder_type"] == rewarderType.ZYBERSWAP_masterchef_v1:
        # do nothing because this is the reward given by the masterchef and will be processed once we get to the rewarder ( thru the masterchef itself..)
        pass
        # return None

    # create masterchef object:  USE address == rewarder_registry on masterchef for this type of rewarder
    zyberswap_masterchef = zyberswap_masterchef_v1(
        address=rewarder_static["rewarder_registry"],
        network=network,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )

    result = []
    # get rewards onchain status
    for reward_data in zyberswap_masterchef.get_rewards(
        hypervisor_addresses=[rewarder_static["hypervisor_address"]],
        pids=rewarder_static["rewarder_refIds"],
        convert_bint=True,
    ):
        try:
            # add prices and APR to onchain status
            reward_data = add_apr_process01(
                network=network,
                hypervisor_status=hypervisor_status,
                reward_data=reward_data,
            )
            result.append(reward_data)
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Zyberswap Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
            )
            logging.getLogger(__name__).debug(
                f" Zyberswap Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
            )

    return result


def create_rewards_status_thena(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    result = []

    thena_gauge = thena_gauge_v2(
        address=rewarder_static["rewarder_address"],
        network=network,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )

    # get rewards directly from gauge ( rewarder ).  Warning-> will not contain rewarder_registry field!!
    if rewards_from_gauge := thena_gauge.get_rewards(convert_bint=True):
        # rewards_from_gauge is a list
        for reward in rewards_from_gauge:
            try:
                # add rewarder registry address
                reward["rewarder_registry"] = rewarder_static["rewarder_registry"]
                # add prices and APR to onchain status
                reward = add_apr_process01(
                    network=network,
                    hypervisor_status=hypervisor_status,
                    reward_data=reward,
                )
                # add to result
                result.append(reward)
            except Exception as e:
                logging.getLogger(__name__).error(
                    f" Thena Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Thena Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
                )

    return result


def create_rewards_status_beamswap(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    if rewarder_static["rewarder_type"] == rewarderType.BEAMSWAP_masterchef_v2:
        # do nothing because this is the reward given by the masterchef and will be processed once we get to the rewarder ( thru the masterchef itself..)
        pass
        # return None

    # create masterchef object:   USE address == rewarder_registry on masterchef for this type of rewarder
    masterchef = beamswap_masterchef_v2(
        address=rewarder_static["rewarder_registry"],
        network=network,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )

    result = []
    # get rewards onchain status
    for reward_data in masterchef.get_rewards(
        hypervisor_addresses=[rewarder_static["hypervisor_address"]],
        pids=rewarder_static["rewarder_refIds"],
        convert_bint=True,
    ):
        try:
            # add prices and APR to onchain status
            reward_data = add_apr_process01(
                network=network,
                hypervisor_status=hypervisor_status,
                reward_data=reward_data,
            )
            result.append(reward_data)
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Beamswap Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
            )
            logging.getLogger(__name__).debug(
                f" Beamswap Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
            )

    return result


def create_rewards_status_angle_merkle(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    result = []

    # create merkl helper at status block
    distributor_creator = angle_merkle_distributor_creator(
        address=rewarder_static["rewarder_registry"],
        network=network,
        block=hypervisor_status["block"],
    )
    # save for later use
    _epoch_duration = distributor_creator.EPOCH_DURATION

    # get active distribution data from merkle
    distributions = distributor_creator.getActivePoolDistributions(
        address=hypervisor_status["pool"]["address"]
    )
    for distribution_data in distributions:
        # check if reward token is valid
        if distributor_creator.isValid_reward_token(
            reward_address=distribution_data["token"].lower()
        ):
            try:
                # check tokenX == hype tokenX
                if (
                    distribution_data["token0_contract"]
                    != hypervisor_status["pool"]["token0"]["address"]
                    or distribution_data["token1_contract"]
                    != hypervisor_status["pool"]["token1"]["address"]
                ):
                    # big problem
                    logging.getLogger(__name__).error(
                        f" Merkle Rewards - rewarder id {rewarder_static['rewarder_address']} has different pool tokens than the hypervisor it is attached to!!"
                    )

                # get token prices
                (
                    rewardToken_price,
                    token0_price,
                    token1_price,
                ) = get_reward_pool_prices(
                    network=network,
                    block=hypervisor_status["block"],
                    reward_token=distribution_data["token"],
                    token0=distribution_data["token0_contract"],
                    token1=distribution_data["token1_contract"],
                )

                # hypervisor data
                hype_price_per_share = get_hypervisor_price_per_share(
                    hypervisor_status=hypervisor_status,
                    token0_price=token0_price,
                    token1_price=token1_price,
                )
                hype_tvl_usd = (
                    int(hypervisor_status["totalSupply"])
                    / (10 ** hypervisor_status["decimals"])
                ) * hype_price_per_share
                hypervisor_liquidity = int(
                    hypervisor_status["basePosition"]["liquidity"]
                ) + int(hypervisor_status["limitPosition"]["liquidity"])
                hypervisor_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
                    10 ** hypervisor_status["pool"]["token0"]["decimals"]
                )
                hypervisor_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
                    10 ** hypervisor_status["pool"]["token1"]["decimals"]
                )

                # pool data
                pool_liquidity = int(hypervisor_status["pool"]["liquidity"])
                pool_total0 = int(distribution_data["token0_balance_in_pool"]) / (
                    10 ** int(distribution_data["token0_decimals"])
                )
                pool_total1 = int(distribution_data["token1_balance_in_pool"]) / (
                    10 ** int(distribution_data["token1_decimals"])
                )
                pool_tvl_usd = pool_total0 * token0_price + pool_total1 * token1_price

                # multiple reward information
                calculations = distributor_creator.get_reward_calculations(
                    distribution=distribution_data, _epoch_duration=_epoch_duration
                )

                # reward x year decimal
                reward_x_year_decimal_propFees = (
                    calculations["reward_yearly_fees_decimal"]
                ) * (hypervisor_liquidity / pool_liquidity)
                reward_x_year_decimal_propToken0 = (
                    calculations["reward_yearly_token0_decimal"]
                ) * (hypervisor_total0 / pool_total0)
                reward_x_year_decimal_propToken1 = (
                    calculations["reward_yearly_token1_decimal"]
                ) * (hypervisor_total1 / pool_total1)

                # reward x second
                reward_x_second_propFees = (
                    calculations["reward_x_second"]
                    * (distribution_data["propFees"] / 10000)
                ) * (hypervisor_liquidity / pool_liquidity)
                reward_x_second_propToken0 = (
                    calculations["reward_x_second"]
                    * (distribution_data["propToken0"] / 10000)
                ) * (hypervisor_total0 / pool_total0)
                reward_x_second_propToken1 = (
                    calculations["reward_x_second"]
                    * (distribution_data["propToken1"] / 10000)
                ) * (hypervisor_total1 / pool_total1)

                total_yearly_rewards = (
                    reward_x_year_decimal_propFees
                    + reward_x_year_decimal_propToken0
                    + reward_x_year_decimal_propToken1
                )

                fee_APR = (
                    reward_x_year_decimal_propFees * rewardToken_price
                ) / hype_tvl_usd
                token0_APR = (
                    reward_x_year_decimal_propToken0 * rewardToken_price
                ) / hype_tvl_usd
                token1_APR = (
                    reward_x_year_decimal_propToken1 * rewardToken_price
                ) / hype_tvl_usd

                hype_APR = fee_APR + token0_APR + token1_APR
                reward_x_second = int(
                    reward_x_second_propToken1
                    + reward_x_second_propToken0
                    + reward_x_second_propFees
                )

                # build reward base data
                reward_data = distributor_creator.construct_reward_data(
                    distribution_data=distribution_data,
                    hypervisor_address=hypervisor_status["address"],
                    total_hypervisorToken_qtty=hypervisor_status["totalSupply"],
                    epoch_duration=_epoch_duration,
                    convert_bint=True,
                )

                reward_data["rewards_perSecond"] = str(reward_x_second)

                # add status fields ( APR )
                reward_data["hypervisor_symbol"] = hypervisor_status["symbol"]
                reward_data["dex"] = hypervisor_status["dex"]
                reward_data["apr"] = hype_APR
                reward_data["rewardToken_price_usd"] = rewardToken_price
                reward_data["token0_price_usd"] = token0_price
                reward_data["token1_price_usd"] = token1_price
                reward_data["hypervisor_share_price_usd"] = hype_price_per_share

                # add reward to result
                result.append(reward_data)

            except Exception as e:
                logging.getLogger(__name__).error(
                    f" Merkle Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Merkle Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {distributions}"
                )
                # make sure we return an empty list
                return []

        else:
            # this rewarder is not for this hypervisor
            continue

    # empty result means no rewards at this block
    if not result:
        logging.getLogger(__name__).debug(
            f" Merkle Rewards-> {network}'s {rewarder_static['rewardToken']} has no rewards at block {hypervisor_status['block']}"
        )

    return result


def create_rewards_status_ramses(
    chain: Chain, rewarder_static: dict, hypervisor_status: dict
) -> list:
    result = []
    # create ramses hypervisor
    hype_status = ramses_hypervisor(
        address=hypervisor_status["address"],
        network=chain.database_name,
        block=hypervisor_status["block"],
    )

    hypervisor_totalSupply = hype_status.totalSupply
    # pool_liquidity = hype_status.pool.liquidity
    # gamma_liquidity = (
    #     hype_status.getBasePosition["liquidity"]
    #     + hype_status.getLimitPosition["liquidity"]
    # )
    totalStaked = hype_status.receiver.totalStakes

    if not hypervisor_totalSupply:
        logging.getLogger(__name__).debug(
            f"Can't calculate rewards status for ramses hype {hype_status.symbol} {hype_status.address} because it has no supply at block {hype_status.block}"
        )
        return []

    # period timeframe
    current_timestamp = hype_status._timestamp
    current_period = hype_status.current_period
    # initial timestamp of the period
    period_ini_timestamp = current_period * 60 * 60 * 24 * 7
    # end timestamp of the period
    # period_end_timestamp = ((current_period + 1) * 60 * 60 * 24 * 7) - 1
    # create a list of hypervisor status between period_ini_timestamp and current_timestamp, excluding hypervisor_status id
    raw_timeframe_hype_status_list = get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={
            "address": hypervisor_status["address"],
            "timestamp": {"$gte": period_ini_timestamp, "$lte": current_timestamp},
        },
        sort=[("timestamp", 1)],
        limit=20,
    )

    # filter list with only those where any of the positions changed: list is ordered by ascending timestamp
    timeframe_hype_status_list = []
    for idx in range(len(raw_timeframe_hype_status_list)):
        # add the first item in the list
        if idx == 0:
            timeframe_hype_status_list.append(raw_timeframe_hype_status_list[idx])
            continue

        # # check if current item positions are different from previous item positions
        # if (
        #     raw_timeframe_hype_status_list[idx]["baseUpper"]
        #     != raw_timeframe_hype_status_list[idx - 1]["baseUpper"]
        #     or raw_timeframe_hype_status_list[idx]["limitUpper"]
        #     != raw_timeframe_hype_status_list[idx - 1]["limitUpper"]
        #     or raw_timeframe_hype_status_list[idx]["baseLower"]
        #     != raw_timeframe_hype_status_list[idx - 1]["baseLower"]
        #     or raw_timeframe_hype_status_list[idx]["limitLower"]
        #     != raw_timeframe_hype_status_list[idx - 1]["limitLower"]
        # ):
        # add to last item the total time in seconds passed between this item and the previous one
        timeframe_hype_status_list[-1]["time_passed"] = (
            raw_timeframe_hype_status_list[idx]["timestamp"]
            - timeframe_hype_status_list[-1]["timestamp"]
        ) - 1  # one second less to avoid overlapping periods

        # add the item to the list
        timeframe_hype_status_list.append(raw_timeframe_hype_status_list[idx])

    # when only one item in the list, add total time in seconds passed between this item and the previous one
    # if len(timeframe_hype_status_list) ==1:
    #     timeframe_hype_status_list[-1]["time_passed"] = (
    #         current_timestamp
    #         - timeframe_hype_status_list[-1]["timestamp"]
    #     ) - 1

    result = []
    for reward_token in hype_status.gauge.getRewardTokens:
        # lower case address
        reward_token = reward_token.lower()

        # build erc20 helper
        erc20_helper = build_erc20_helper(
            chain=chain, address=reward_token, cached=True
        )

        # get rewards for all this period for the multiple positions this hypervisor has had
        items_to_calc_apr = []
        total_baseRewards = 0
        total_boostedRewards = 0
        total_time_passed = 0
        current_period
        for hype in timeframe_hype_status_list:
            # do not process "time_passed" -1
            if hype.get("time_passed", 0) == -1:
                continue

            # get rewards for this period using the hype's position
            # create ramses hypervisor
            _temp_hype_status = ramses_hypervisor(
                address=hype["address"],
                network=chain.database_name,
                block=hype["block"],
            )
            # calculate rewards for this position at period
            _temp_real_rewards = _temp_hype_status.calculate_rewards(
                period=_temp_hype_status.current_period, reward_token=reward_token
            )
            # calculate rewards per second for this position and perood
            baseRewards_per_second = (
                _temp_real_rewards["current_baseRewards"]
                / _temp_real_rewards["current_period_seconds"]
            )
            boostRewards_per_second = (
                _temp_real_rewards["current_boostedRewards"]
                / _temp_real_rewards["current_period_seconds"]
            )

            # time passed since the position was active. On the last item, use the current timestamp
            _temp_time_passed = hype.get(
                "time_passed", current_timestamp - hype["timestamp"]
            )

            # calculate real rewards for the position during the time it was active
            items_to_calc_apr.append(
                {
                    "base_rewards": (baseRewards_per_second * _temp_time_passed)
                    / (10 ** rewarder_static["rewardToken_decimals"]),
                    "boosted_rewards": (boostRewards_per_second * _temp_time_passed)
                    / (10 ** rewarder_static["rewardToken_decimals"]),
                    "time_passed": _temp_time_passed,
                    "timestamp_ini": hype["timestamp"],
                    "hype_totalSupply": int(hype["totalSupply"])
                    / (10 ** hype["decimals"]),
                    "hype_underlying_token0": (
                        float(hype["totalAmounts"]["total0"])
                        + float(hype["fees_uncollected"]["qtty_token0"])
                    )
                    / (10 ** hype["pool"]["token0"]["decimals"]),
                    "hype_underlying_token1": (
                        float(hype["totalAmounts"]["total1"])
                        + float(hype["fees_uncollected"]["qtty_token1"])
                    )
                    / (10 ** hype["pool"]["token1"]["decimals"]),
                }
            )
            total_baseRewards += baseRewards_per_second * _temp_time_passed
            total_boostedRewards += boostRewards_per_second * _temp_time_passed
            total_time_passed += _temp_time_passed

        gamma_baseRewards_per_second = total_baseRewards / total_time_passed
        gamma_boostedRewards_per_second = total_boostedRewards / total_time_passed
        gamma_totalRewards_per_second = (
            gamma_baseRewards_per_second + gamma_boostedRewards_per_second
        )
        # set  LP staked
        total_hypervisorToken_qtty = totalStaked or hype_status.totalSupply

        # if real_rewards["current_rewards_per_second"]:
        #     gamma_rewards_per_second = real_rewards["current_rewards_per_second"]
        #     # get LP staked
        #     total_hypervisorToken_qtty = totalStaked or hype_status.totalSupply
        # else:
        #     logging.getLogger(__name__).debug(
        #         f" Using Ramses max rewards per second for hype {hype_status.symbol} {hype_status.address.lower()} because it has no real rewards at block {hype_status.block}. rewarder address: {hype_status.gauge.address.lower()}"
        #     )
        #     gamma_rewards_per_second = real_rewards["max_rewards_per_second"]
        #     # xtrapolate gamma hype supply to approach pool value
        #     if gamma_liquidity:
        #         total_hypervisorToken_qtty = int(
        #             hypervisor_totalSupply * (pool_liquidity / gamma_liquidity)
        #         )
        #     else:
        #         logging.getLogger(__name__).warning(
        #             f" No liquidity for hype {hypervisor_status['address']} found at block {hypervisor_status['block']}."
        #         )
        #         total_hypervisorToken_qtty = 0

        # when available, get last known reward status from database to be able to calculate staked
        # if last_reward_status := get_from_localdb(
        #     network=chain.database_name,
        #     collection="rewards_status",
        #     find={
        #         "hypervisor_address": hypervisor_status["address"],
        #         "rewarder_address": hype_status.gauge.address.lower(),
        #         "rewardToken": reward_token.lower(),
        #         "block": {"$lt": hypervisor_status["block"]},
        #     },
        #     sort=[("block", -1)],
        #     limit=1,
        # ):
        #     last_reward_status = last_reward_status[0]

        #     if int(last_reward_status["total_hypervisorToken_qtty"]) > 0:
        #         logging.getLogger(__name__).debug(
        #             f"    ...chainging total hype {hypervisor_status['address']} qtty from {total_hypervisorToken_qtty} to last known {last_reward_status['total_hypervisorToken_qtty']} {hypervisor_status['block']-last_reward_status['block']} blocks in the past [curr:{hypervisor_status['block']} last: {last_reward_status['block']}]"
        #         )
        #         total_hypervisorToken_qtty = int(
        #             last_reward_status["total_hypervisorToken_qtty"]
        #         )
        #     else:
        #         logging.getLogger(__name__).debug(
        #             f"    ...no past rewards_status found for hypervisor {hypervisor_status['address']} {hypervisor_status['symbol']} at block {hypervisor_status['block']}. Using current {total_hypervisorToken_qtty}"
        #         )

        # else:
        #     logging.getLogger(__name__).debug(
        #         f"  ...no past rewards found for hypervisor {hypervisor_status['address']} {hypervisor_status['symbol']}"
        #     )

        # add apr
        try:
            # get prices
            (
                rewardToken_price,
                hype_token0_price,
                hype_token1_price,
            ) = get_reward_pool_prices(
                network=chain.database_name,
                block=hypervisor_status["block"],
                reward_token=reward_token.lower(),
                token0=hypervisor_status["pool"]["token0"]["address"],
                token1=hypervisor_status["pool"]["token1"]["address"],
            )

            # calculate apr using items_to_calc_apr data and prices
            cum_reward_return = 0
            cum_baseReward_return = 0
            cum_boostedReward_return = 0
            total_period_seconds = 0
            for item in items_to_calc_apr:
                # discard items with timepassed = 0
                if item["time_passed"] == 0:
                    continue

                # calculate price per share for each item using current prices
                item["tvl"] = (
                    item["hype_underlying_token0"] * hype_token0_price
                    + item["hype_underlying_token1"] * hype_token1_price
                )
                item["hypervisor_price_per_share"] = (
                    item["tvl"] / item["hype_totalSupply"]
                )

                item["base_rewards_usd"] = item["base_rewards"] * rewardToken_price
                item["boosted_rewards_usd"] = (
                    item["boosted_rewards"] * rewardToken_price
                )
                item["total_rewards_usd"] = (
                    item["base_rewards_usd"] + item["boosted_rewards_usd"]
                )

                # calculate period yield
                item["period_yield"] = item["total_rewards_usd"] / item["tvl"]
                # add to cumulative yield
                if cum_reward_return:
                    cum_reward_return *= 1 + item["period_yield"]
                else:
                    cum_reward_return = 1 + item["period_yield"]
                if cum_baseReward_return:
                    cum_baseReward_return *= 1 + (
                        item["base_rewards_usd"] / item["tvl"]
                    )
                else:
                    cum_baseReward_return = 1 + (item["base_rewards_usd"] / item["tvl"])
                if cum_boostedReward_return:
                    cum_boostedReward_return *= 1 + (
                        item["boosted_rewards_usd"] / item["tvl"]
                    )
                else:
                    cum_boostedReward_return = 1 + (
                        item["boosted_rewards_usd"] / item["tvl"]
                    )

                # extrapolate rewards to a year
                item["base_rewards_usd_year"] = (
                    (item["base_rewards_usd"] / item["time_passed"])
                    * 60
                    * 60
                    * 24
                    * 365
                )
                item["boosted_rewards_usd_year"] = (
                    (item["boosted_rewards_usd"] / item["time_passed"])
                    * 60
                    * 60
                    * 24
                    * 365
                )
                item["total_rewards_usd_year"] = (
                    item["base_rewards_usd_year"] + item["boosted_rewards_usd_year"]
                )

                item["total_reward_apr"] = (cum_reward_return - 1) * (
                    (60 * 60 * 24 * 365) / item["time_passed"]
                )
                item["total_reward_apy"] = (
                    1 + (cum_reward_return - 1) * ((60 * 60 * 24) / item["time_passed"])
                ) ** 365 - 1
                item["base_reward_apr"] = (cum_baseReward_return - 1) * (
                    (60 * 60 * 24 * 365) / item["time_passed"]
                )
                item["base_reward_apy"] = (
                    1
                    + (cum_baseReward_return - 1)
                    * ((60 * 60 * 24) / item["time_passed"])
                ) ** 365 - 1
                item["boosted_reward_apr"] = (cum_boostedReward_return - 1) * (
                    (60 * 60 * 24 * 365) / item["time_passed"]
                )
                item["boosted_reward_apy"] = (
                    1
                    + (cum_boostedReward_return - 1)
                    * ((60 * 60 * 24) / item["time_passed"])
                ) ** 365 - 1

                total_period_seconds += item["time_passed"]

            # calculate total apr
            cum_reward_return -= 1
            cum_baseReward_return -= 1
            cum_boostedReward_return -= 1
            reward_apr = cum_reward_return * (
                (60 * 60 * 24 * 365) / total_period_seconds
            )
            reward_apy = (
                1 + cum_reward_return * ((60 * 60 * 24) / total_period_seconds)
            ) ** 365 - 1

            baseRewards_apr = cum_baseReward_return * (
                (60 * 60 * 24 * 365) / total_period_seconds
            )
            baseRewards_apy = (
                1 + cum_baseReward_return * ((60 * 60 * 24) / total_period_seconds)
            ) ** 365 - 1
            boostRewards_apr = cum_boostedReward_return * (
                (60 * 60 * 24 * 365) / total_period_seconds
            )
            boostRewards_apy = (
                1 + cum_boostedReward_return * ((60 * 60 * 24) / total_period_seconds)
            ) ** 365 - 1

            # build reward data
            reward_data = {
                "apr": reward_apr,
                "apy": reward_apy,
                "block": hype_status.block,
                "timestamp": hype_status._timestamp,
                "hypervisor_address": hype_status.address.lower(),
                "rewarder_address": hype_status.gauge.address.lower(),
                "rewarder_type": rewarderType.RAMSES_v2,
                "rewarder_refIds": [],
                "rewarder_registry": hype_status.receiver.address.lower(),
                "rewardToken": reward_token.lower(),
                "rewardToken_symbol": erc20_helper.symbol,
                "rewardToken_decimals": erc20_helper.decimals,
                "rewards_perSecond": str(gamma_totalRewards_per_second),
                "total_hypervisorToken_qtty": str(total_hypervisorToken_qtty),
                # extra fields
                "extra": {
                    "baseRewards": total_baseRewards,
                    "boostedRewards": total_boostedRewards,
                    "baseRewards_apr": baseRewards_apr,
                    "baseRewards_apy": baseRewards_apy,
                    "boostedRewards_apr": boostRewards_apr,
                    "boostedRewards_apy": boostRewards_apy,
                    "baseRewards_per_second": str(gamma_baseRewards_per_second),
                    "boostedRewards_per_second": str(gamma_boostedRewards_per_second),
                    # "raw_data": items_to_calc_apr,
                },
            }

            result.append(reward_data)
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Ramses Rewards-> {chain.database_name}'s {rewarder_static.get('rewardToken','None')} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
            )
            logging.getLogger(__name__).debug(
                f" Ramses Rewards last err debug data -> rewarder_static {rewarder_static}    hype status {hypervisor_status}"
            )

    # empty result means no rewards at this block
    if not result:
        logging.getLogger(__name__).debug(
            f" Ramses Rewards-> {chain.database_name}'s {rewarder_static.get('rewardToken','None')} has no rewards at block {hypervisor_status['block']}"
        )

    return result


# User rewards status


def create_user_rewards_status_merkl(
    chain: Chain,
    already_processed: list,
    rewrite: bool = False,
):
    # TODO: work in progress

    # create merkl helper
    canciller = angle_merkle_wraper()
    # get epochs
    for epoch_data in canciller.get_epochs(chain=chain):
        timestamp = epoch_data["timestamp"]
        epoch = epoch_data["epoch"]
        # get rewards for epoch
        for merkl_proof, merkl_data in canciller.get_rewards(chain=chain, epoch=epoch):
            # boostedAddress = merkl_data["boostedAddress"]
            # boostedReward = merkl_data["boostedReward"]
            # lastUpdateEpoch = merkl_data["lastUpdateEpoch"]
            # pool = merkl_data["pool"]
            # token = merkl_data["token"]
            # tokenDecimals = merkl_data["tokenDecimals"]
            # tokenSymbol = merkl_data["tokenSymbol"]
            # totalAmount = merkl_data["totalAmount"]

            for holder_address, amount_data in merkl_data["holders"].items():
                if gamma_amount := amount_data["breakdown"].get("gamma", 0):
                    # this gamma user has merkl rewards
                    pass
