import logging

from apps.feeds.queue.queue_item import QueueItem

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import (
    create_id_hypervisor_status,
    create_id_price,
    create_id_rewards_status,
)
from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)
from bins.general.enums import (
    Chain,
    Protocol,
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
        if found_status := get_from_localdb(
            network=network, collection="status", find={"id": hypervisor_id}
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
            get_default_localdb(network=network).set_queue_item(
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
                get_default_globaldb().set_block(
                    network=network, block=block, timestamp=operation["timestamp"]
                )
            else:
                # block-1
                get_default_localdb(network=network).set_queue_item(
                    data=QueueItem(
                        type=queueItemType.BLOCK,
                        block=block,
                        address=operation["address"],
                        data=operation,
                    ).as_dict
                )

    # # 2) create a user operation queue item
    if operation["topic"] in ["deposit", "withdraw", "transfer"]:
        if db_return := get_default_localdb(network=network).set_queue_item(
            data=QueueItem(
                type=queueItemType.USER_OPERATION,
                block=operation["blockNumber"],
                address=operation["address"],
                data=operation,
            ).as_dict
        ):
            logging.getLogger(__name__).debug(
                f" Saved user operation Queue Item {operation['id']}"
            )
        else:
            logging.getLogger(__name__).error(
                f"  database did not return anything while saving {queueItemType.USER_OPERATION} to queue"
            )


def build_and_save_queue_from_hypervisor_status(hypervisor_status: dict, network: str):

    # add all items to database at once
    if items := build_queue_items_from_hypervisor_status(
        hypervisor_status=hypervisor_status, network=network
    ):
        get_default_localdb(network=network).replace_items_to_database(
            data=items, collection_name="queue"
        )


def build_queue_items_from_hypervisor_status(
    hypervisor_status: dict, network: str
) -> list[QueueItem]:
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
        # check if end rewards is > hypervisor timestamp
        if (
            "end_rewards_timestamp" in reward_static
            and reward_static["end_rewards_timestamp"] > 0
            and reward_static["end_rewards_timestamp"] < hypervisor_status["timestamp"]
        ) or (
            "start_rewards_timestamp" in reward_static
            and reward_static["start_rewards_timestamp"] > 0
            and reward_static["start_rewards_timestamp"]
            > hypervisor_status["timestamp"]
        ):
            logging.getLogger(__name__).debug(
                f" {network}'s {hypervisor_status['address']} hype's reward status at block {hypervisor_status['block']} will not be queued bc its not within its active timewindow {reward_static['start_rewards_timestamp']}-{reward_static['end_rewards_timestamp']}. Skipping."
            )
            continue

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

    return items


# def build_and_save_queue_from_revenue_operation(operation: dict, network: str):

#     items = []

#     # token price
#     price_id = create_id_price(
#         network=network,
#         block=operation["block"],
#         token_address=operation["address"].lower(),
#     )
#     if not get_default_globaldb().get_items_from_database(
#         collection_name="usd_prices",
#         find={"id": price_id},
#     ):
#         # add to queue
#         items.append(
#             QueueItem(
#                 type=queueItemType.PRICE,
#                 block=operation["block"],
#                 address=operation["address"].lower(),
#                 data=operation,
#             ).as_dict
#         )
#     else:
#         logging.getLogger(__name__).debug(
#             f" {network}'s {operation['address']} token at block {operation['block']} is already in database"
#         )


#     # add all items to database at once
#     if items:
#         get_default_localdb(network=network).replace_items_to_database(data=items, collection_name="queue")


def build_and_save_queue_from_hypervisor_static(hypervisor_static: dict, network: str):
    pass


# hypervisor static
def build_hypervisor_static_queueItems(
    hypervisor_addresses: list[str],
    chain: Chain,
    protocol: Protocol,
    create_reward_static: bool = True,
):
    """Create queue items of type hypervisor static using a list of hypervisor addresses

    Args:
        hypervisor_addresses (list[str]):
        create_reward_static (bool, optional): After the hype static queue item is processed, would u like to enqueue a reward static work ? Defaults to True.
    """
    qitems_list = []
    for hypervisor_address in hypervisor_addresses:
        # create queue item
        qitems_list.append(
            QueueItem(
                type=queueItemType.HYPERVISOR_STATIC,
                block=0,
                address=hypervisor_address.lower(),
                data={
                    "chain": chain.database_name,
                    "protocol": protocol.database_name,
                    "create_reward_static": create_reward_static,
                },
            ).as_dict
        )

    return qitems_list


def build_and_save_queue_items_from_hypervisor_addresses(
    hypervisor_addresses: list[str], chain: Chain, protocol: Protocol
):
    """Create queue items of type hypervisor static using a list of hypervisor addresses

    Args:
        hypervisor_addresses (list[str]):
        network (str):
    """
    # create queue items list
    qitems_list = build_hypervisor_static_queueItems(
        hypervisor_addresses=hypervisor_addresses, chain=chain, protocol=protocol
    )

    # save to database
    if db_return := get_default_localdb(
        network=chain.database_name
    ).replace_items_to_database(
        data=qitems_list,
        collection_name="queue",
    ):
        logging.getLogger(__name__).debug(
            f"     db return-> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
        )
    else:
        logging.getLogger(__name__).error(
            f"  database did not return anything while saving {queueItemType.HYPERVISOR_STATIC}s to queue"
        )


# rewards static


def build_reward_static_queueItem(
    chain: Chain, protocol: Protocol, hypervisor_address: str
) -> QueueItem:
    pass
