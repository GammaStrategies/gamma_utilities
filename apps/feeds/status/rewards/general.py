import logging
import concurrent.futures
import tqdm

from apps.feeds.status.rewards.beamswap import create_rewards_status_beamswap
from apps.feeds.status.rewards.synthswap import create_rewards_status_synthswap
from apps.feeds.status.rewards.thena import create_rewards_status_thena
from apps.feeds.status.rewards.zyberswap import create_rewards_status_zyberswap
from apps.hypervisor_periods.rewards.angle import hypervisor_periods_angleMerkl
from apps.hypervisor_periods.rewards.ramses import hypervisor_periods_ramses

from bins.configuration import CONFIGURATION

from bins.database.common.db_collections_common import database_global, database_local
from bins.errors.actions import process_error

from bins.errors.general import ProcessingError
from bins.general.enums import Chain, Protocol, rewarderType, text_to_chain


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
                    tmp = 0
                    try:
                        tmp = int(reward["rewards_perSecond"])
                    except Exception as e:
                        logging.getLogger(__name__).warning(
                            f" rewards per second are float not int in {reward['id']}"
                        )
                        tmp = float(reward["rewards_perSecond"])

                    if tmp > 0:
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
            aMerkl_helper = hypervisor_periods_angleMerkl(
                chain=text_to_chain(network),
                hypervisor_status=hypervisor_status,
                rewarder_static=rewarder_static,
            )
            # limit to 2 week data back ( from status )

            rewards_data = aMerkl_helper.execute_processes_within_hypervisor_periods(
                timestamp_ini=hypervisor_status["timestamp"] - 60 * 60 * 24 * 14,
                timestamp_end=hypervisor_status["timestamp"],
            )

        elif rewarder_static["rewarder_type"] in [
            rewarderType.RAMSES_v2,
        ]:
            ramses_helper = hypervisor_periods_ramses(
                chain=text_to_chain(network),
                hypervisor_status=hypervisor_status,
                rewarder_static=rewarder_static,
            )
            # limit to >2 week data back
            rewards_data = ramses_helper.execute_processes_within_hypervisor_periods(
                timestamp_ini=hypervisor_status["timestamp"] - 60 * 60 * 24 * 16,
                timestamp_end=hypervisor_status["timestamp"],
            )

        elif rewarder_static["rewarder_type"] in [
            rewarderType.SYNTHSWAP_masterchef_v1,
            rewarderType.SYNTHSWAP_masterchef_v1_rewarder,
        ]:
            # get rewards status
            rewards_data = create_rewards_status_synthswap(
                network=network,
                rewarder_static=rewarder_static,
                hypervisor_status=hypervisor_status,
            )
        else:
            raise ValueError(
                f" Unknown rewarder type {rewarder_static['rewarder_type']} for {network}'s rewarder: {rewarder_static['rewarder_address']}  hype: {hypervisor_status['address']} at block {hypervisor_status['block']}"
            )

    except ProcessingError as e:
        logging.getLogger(__name__).error(
            f" Unexpected error constructing {network}'s {rewarder_static['rewarder_address']} rewarder data. error-> {e.message}"
        )
        # process error
        process_error(e)

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error constructing {network}'s {rewarder_static['rewarder_address']} rewarder data. error-> {e}"
        )

    logging.getLogger(__name__).debug(
        f"    -> Done processing {network}'s hype {hypervisor_status['address']}  rewarder {rewarder_static['rewarder_address']}  registry: {rewarder_static['rewarder_registry']} at block {hypervisor_status['block']}."
    )

    return rewards_data
