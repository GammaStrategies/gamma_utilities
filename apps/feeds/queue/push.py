import logging

from apps.feeds.queue.queue_item import QueueItem

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import (
    create_id_hypervisor_status,
    create_id_price,
    create_id_rewards_status,
)
from bins.database.common.db_collections_common import database_global, database_local
from bins.general.enums import (
    queueItemType,
)


# PUSH DATA


def build_and_save_queue_from_operation(operation: dict, network: str):
    """-> approval operations are discarded

    Args:
        operation (dict): _description_
        network (str): _description_
    """

    # discard approval operations ( they are >10% of all operations)
    if operation["topic"] not in [
        "deposit",
        "withdraw",
        "rebalance",
        "zeroBurn",
        "transfer",
    ]:
        # approval operations do not need a hype status nor prices, etc..
        return

    logging.getLogger(__name__).debug(
        f"  Building queue items related to a {operation['topic']} operation on {network}'s {operation['address']} hype at block {operation['blockNumber']}"
    )

    # create local database manager
    db_name = f"{network}_gamma"
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    local_db = database_local(
        mongo_url=mongo_url,
        db_name=db_name,
    )

    # build a block list: block and block-1 to calc APR loc's way
    blocks = (
        [operation["blockNumber"], operation["blockNumber"] - 1]
        if operation["topic"] in ["deposit", "withdraw", "rebalance", "zeroBurn"]
        else [operation["blockNumber"]]
    )
    # 1) create new hypervisor status at block and block -1 if operation["topic"] in ["deposit", "withdraw", "rebalance", "zeroBurn"]
    for block in blocks:
        # build id
        hypervisor_id = create_id_hypervisor_status(
            hypervisor_address=operation["address"], block=block
        )
        if found_status := local_db.get_items_from_database(
            collection_name="status", find={"id": hypervisor_id}
        ):
            # already in database
            logging.getLogger(__name__).debug(
                f" {network}'s {operation['address']} hypervisor at block {operation['blockNumber']} is already in database"
            )
            # check if rewards should be updated
            build_and_save_queue_from_hypervisor_status(
                hypervisor_status=found_status[0], network=network
            )
        else:
            # not in database
            # add hype status to queue
            local_db.set_queue_item(
                data=QueueItem(
                    type=queueItemType.HYPERVISOR_STATUS,
                    block=block,
                    address=operation["address"],
                    data=operation,
                ).as_dict
            )

            # save block timestamp when block is operation["blockNumber"]
            if block == operation["blockNumber"]:
                # save to database
                database_global(mongo_url=mongo_url).set_block(
                    network=network, block=block, timestamp=operation["timestamp"]
                )
            else:
                # block-1
                local_db.set_queue_item(
                    data=QueueItem(
                        type=queueItemType.BLOCK,
                        block=block,
                        address=operation["address"],
                        data=operation,
                    ).as_dict
                )


def build_and_save_queue_from_hypervisor_status(hypervisor_status: dict, network: str):
    # create local database manager
    db_name = f"{network}_gamma"
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    local_db = database_local(
        mongo_url=mongo_url,
        db_name=db_name,
    )
    # build items to update
    items = []

    # token 0 price
    price0_id = create_id_price(
        network=network,
        block=hypervisor_status["block"],
        token_address=hypervisor_status["pool"]["token0"]["address"],
    )
    if not database_global(mongo_url=mongo_url).get_items_from_database(
        collection_name="usd_prices",
        find={"id": price0_id},
    ):
        # add to queue
        items.append(
            QueueItem(
                type=queueItemType.PRICE,
                block=hypervisor_status["block"],
                address=hypervisor_status["pool"]["token0"]["address"],
                data=hypervisor_status,
            ).as_dict
        )
    else:
        logging.getLogger(__name__).debug(
            f" {network}'s {hypervisor_status['pool']['token0']['address']} token at block {hypervisor_status['block']} is already in database"
        )

    # token 1 price
    price1_id = create_id_price(
        network=network,
        block=hypervisor_status["block"],
        token_address=hypervisor_status["pool"]["token1"]["address"],
    )
    if not database_global(mongo_url=mongo_url).get_items_from_database(
        collection_name="usd_prices",
        find={"id": price1_id},
    ):
        # add to queue
        items.append(
            QueueItem(
                type=queueItemType.PRICE,
                block=hypervisor_status["block"],
                address=hypervisor_status["pool"]["token1"]["address"],
                data=hypervisor_status,
            ).as_dict
        )
    else:
        logging.getLogger(__name__).debug(
            f" {network}'s {hypervisor_status['pool']['token1']['address']} token at block {hypervisor_status['block']} is already in database"
        )

    # Rewards
    # avoid rewards if there is no hypervisor supply
    if int(hypervisor_status["totalSupply"]) <= 0:
        logging.getLogger(__name__).warning(
            f" {network}'s {hypervisor_status['address']} hypervisor has no supply at block {hypervisor_status['block']}. Can't queue reward status scrape."
        )
        return

    # get a list of rewards_static rewardToken linked with hypervisor_address
    # make sure hype block is greater than static reward block
    for reward_static in local_db.get_items_from_database(
        collection_name="rewards_static",
        find={
            "hypervisor_address": hypervisor_status["address"],
            "block": {"$lte": hypervisor_status["block"]},
        },
    ):
        # Reward price
        reward_price_id = create_id_price(
            network=network,
            block=hypervisor_status["block"],
            token_address=reward_static["rewardToken"],
        )
        if not database_global(mongo_url=mongo_url).get_items_from_database(
            collection_name="usd_prices",
            find={"id": reward_price_id},
        ):
            # add price
            items.append(
                QueueItem(
                    type=queueItemType.PRICE,
                    block=hypervisor_status["block"],
                    address=reward_static["rewardToken"],
                    data=reward_static,
                ).as_dict
            )
        else:
            logging.getLogger(__name__).debug(
                f" {network}'s {reward_static['rewardToken']} token at block {hypervisor_status['block']} is already in database"
            )

        # add reward_status
        reward_status_id = create_id_rewards_status(
            hypervisor_address=reward_static["hypervisor_address"],
            rewarder_address=reward_static["rewarder_address"],
            rewardToken_address=reward_static["rewardToken"],
            block=hypervisor_status["block"],
        )
        if not local_db.get_items_from_database(
            collection_name="rewards_status",
            find={"id": reward_status_id},
        ):
            # add to queue
            items.append(
                QueueItem(
                    type=queueItemType.REWARD_STATUS,
                    block=hypervisor_status["block"],
                    address=reward_static["rewarder_address"],
                    data={
                        "reward_static": reward_static,
                        "hypervisor_status": hypervisor_status,
                    },
                ).as_dict
            )
        else:
            logging.getLogger(__name__).debug(
                f" {network}'s {hypervisor_status['address']} hype's {reward_static['rewarder_address']} reward status at block {hypervisor_status['block']} is already in database"
            )

    # add all items to database at once
    if items:
        local_db.replace_items_to_database(data=items, collection_name="queue")
