import logging
import concurrent.futures
import tqdm

from apps.feeds.queue.push import build_and_save_queue_from_hypervisor_status
from bins.configuration import CONFIGURATION, TOKEN_ADDRESS_EXCLUDE
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, queueItemType


def repair_missing_rewards_status(
    chain: Chain, max_repair: int = None, hypervisor_addresses: list[str] | None = None
):
    """ """

    # TODO: change so that avoids filling rewards status queue with inexistent ones.
    # Rewards status start in an offchain decision and may start in any block. It may stop n start again in a different block.

    batch_size = 100000
    logging.getLogger(__name__).info(
        f"> Finding missing {chain.database_name}'s rewards status in database"
    )
    # get all operation blocks from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{chain.database_name}_gamma"

    # loop thru all static rewards in database
    for reward_static in tqdm.tqdm(
        database_local(mongo_url=mongo_url, db_name=db_name).get_items_from_database(
            collection_name="rewards_static",
            find=(
                {"hypervisor_address": {"$in": hypervisor_addresses}}
                if hypervisor_addresses
                else {}
            ),
            batch_size=batch_size,
            sort=[("_id", -1)],
        )
    ):
        # do not process excluded tokens
        if reward_static["rewardToken"] in TOKEN_ADDRESS_EXCLUDE.get(chain, {}):
            continue

        # get rewards_status ids and blocks from database
        rewards_status_ids = []
        rewards_status_blocks = []
        for item in database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="rewards_status",
            find={
                "hypervisor_address": reward_static["hypervisor_address"],
                "rewardToken": reward_static["rewardToken"],
                "rewarder_type": reward_static["rewarder_type"],
                "rewarder_address": reward_static["rewarder_address"],
            },
            projection={"id": 1, "_id": 0, "block": 1},
            batch_size=batch_size,
        ):
            rewards_status_ids.append(item["id"])
            rewards_status_blocks.append(int(item["block"]))

        # check for queue'd rewards_status with same hypervisor and rewardToken and block
        for item in get_from_localdb(
            network=chain.database_name,
            collection="queue",
            find={
                "type": queueItemType.REWARD_STATUS,
                "data.hypervisor_status.address": reward_static["hypervisor_address"],
                "data.reward_static.rewarder_address": reward_static[
                    "rewarder_address"
                ],
                "data.reward_static.rewarder_type": reward_static["rewarder_type"],
                "data.reward_static.rewardToken": reward_static["rewardToken"],
            },
        ):
            rewards_status_blocks.append(int(item["block"]))

        # sort by block desc so that newer items can be seen first
        hypervisors_status_lits = database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="status",
            find={
                "address": reward_static["hypervisor_address"],
                "$and": [
                    {"block": {"$gte": reward_static["block"]}},
                    {"block": {"$nin": rewards_status_blocks}},
                ],
                "totalSupply": {"$ne": "0"},
            },
            sort=[("block", -1)],
            batch_size=batch_size,
        )

        if hypervisors_status_lits:
            logging.getLogger(__name__).debug(
                f" Found {len(hypervisors_status_lits)} missing rewards status blocks for {chain.database_name}'s {reward_static['hypervisor_address']}"
            )
            if max_repair and len(hypervisors_status_lits) > max_repair:
                # select the most recent
                logging.getLogger(__name__).info(
                    f"  Selecting the most recent {max_repair} rewards status missing due to max_repair limit set."
                )
                hypervisors_status_lits = hypervisors_status_lits[:max_repair]
                # make a random selection
                # logging.getLogger(__name__).info(
                #     f"  Selecting a random sample of {max_repair} rewards status missing due to max_repair limit set."
                # )
                # hypervisors_status_lits = random.sample(
                #     hypervisors_status_lits, max_repair
                # )

            logging.getLogger(__name__).info(
                f" A total of {len(hypervisors_status_lits)} rewards status will be added to the queue (prices may also get queued when needed)"
            )

            # prepare arguments for treaded function
            with concurrent.futures.ThreadPoolExecutor() as ex:
                for result in ex.map(
                    lambda p: build_and_save_queue_from_hypervisor_status(*p), args
                ):
                    # progress_bar.update(0)
                    pass

        else:
            logging.getLogger(__name__).debug(
                f" No missing rewards status found for {chain.database_name}'s {reward_static['hypervisor_address']}"
            )
