# hypervisors static data
from dataclasses import dataclass, asdict
import logging
import time
import concurrent.futures

from apps.feeds.status import (
    create_and_save_hypervisor_status,
    create_reward_status_from_hype_status,
)
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global, database_local
from bins.general.enums import Chain, queueItemType
from bins.general.general_utilities import seconds_to_time_passed
from bins.w3.builders import build_db_hypervisor
from bins.mixed.price_utilities import price_scraper
from bins.w3.protocols.general import erc20, bep20


@dataclass
class QueueItem:
    type: queueItemType
    block: int
    address: str
    data: dict
    processing: float = 0  # timestamp
    id: str | None = None
    creation: float = 0
    _id: str | None = None  # db only
    count: int = 0

    def __post_init__(self):
        if type == "reward_status":
            self.id = f"{self.type}_{self.block}_{self.address}_{self.data['hypervisor_status']['address']}"
        else:
            self.id = f"{self.type}_{self.block}_{self.address}"

        # add creation time when object is created for the first time (not when it is loaded from database)
        if self.creation == 0:
            self.creation = time.time()
            # self.count = 0 # not needed because it is set to 0 by default
        else:
            # add a counter to avoid infinite info gathering loops on errors
            self.count += 1

    @property
    def as_dict(self) -> dict:
        return {
            "type": self.type,
            "block": self.block,
            "address": self.address,
            "processing": self.processing,
            "data": self.data,
            "id": self.id,
            "creation": self.creation,
            "count": self.count,
        }


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
        if found_status := local_db.get_items_from_database(
            collection_name="status", find={"id": f"{operation['address']}_{block}"}
        ):
            # already in database
            logging.getLogger(__name__).debug(
                f" {network}'s {operation['address']} hype at block {operation['blockNumber']} is already in database"
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
    if not database_global(mongo_url=mongo_url).get_items_from_database(
        collection_name="usd_prices",
        find={
            "id": f"{network}_{hypervisor_status['block']}_{hypervisor_status['pool']['token0']['address']}"
        },
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
    if not database_global(mongo_url=mongo_url).get_items_from_database(
        collection_name="usd_prices",
        find={
            "id": f"{network}_{hypervisor_status['block']}_{hypervisor_status['pool']['token1']['address']}"
        },
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
    if int(hypervisor_status["totalSupply"]) > 0:
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
            if not database_global(mongo_url=mongo_url).get_items_from_database(
                collection_name="usd_prices",
                find={
                    "id": f"{network}_{hypervisor_status['block']}_{reward_static['rewardToken']}"
                },
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
            if not local_db.get_items_from_database(
                collection_name="rewards_status",
                find={
                    "id": f"{hypervisor_status['address']}_{reward_static['rewarder_address']}_{hypervisor_status['block']}"
                },
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
    else:
        logging.getLogger(__name__).warning(
            f" {network}'s {hypervisor_status['address']} hypervisor has no supply at block {hypervisor_status['block']}. Can't queue reward status scrape."
        )

    # add all items to database at once
    if items:
        local_db.replace_items_to_database(data=items, collection_name="queue")


# PULL DATA


def parallel_pull(network: str):
    # TEST funcion: use parallel_feed.py instead
    args = [
        (network, [queueItemType.HYPERVISOR_STATUS, queueItemType.PRICE]),
        (network, [queueItemType.BLOCK]),
        (network, [queueItemType.REWARD_STATUS]),
    ] * 5
    with concurrent.futures.ThreadPoolExecutor() as ex:
        for n in ex.map(lambda p: pull_from_queue(*p), args):
            pass


def pull_from_queue(network: str, types: list[queueItemType] | None = None):
    # variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    # get first item from queue
    if db_queue_item := local_db.get_queue_item(types=types):
        try:
            # convert database queue item to class
            queue_item = QueueItem(**db_queue_item)

            # process queue item
            return process_queue_item_type(network=network, queue_item=queue_item)

        except Exception as e:
            # reset queue item
            queue_item.processing = 0
            local_db.replace_item_to_database(
                data=queue_item.as_dict, collection_name="queue"
            )
            raise e
    # else:
    # no item found
    return True


# classifier
def process_queue_item_type(network: str, queue_item: QueueItem) -> bool:
    # check if queue item has been processed more than 10 times, and return if so
    if queue_item.count > 10:
        logging.getLogger(__name__).error(
            f" {network}'s queue item {queue_item.id} has been processed more than 10 times unsuccessfully. Skipping ( check it manually)"
        )
        return False

    logging.getLogger(__name__).debug(
        f"Processing {network}'s {queue_item.type} queue item {queue_item.address} at block {queue_item.block}"
    )

    if queue_item.type == queueItemType.HYPERVISOR_STATUS:
        return pull_from_queue_hypervisor_status(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.REWARD_STATUS:
        return pull_from_queue_reward_status(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.PRICE:
        return pull_from_queue_price(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.BLOCK:
        return pull_from_queue_block(network=network, queue_item=queue_item)
    else:
        # reset queue item

        # variables
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        # set local database name and create manager
        local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")
        # set queue item as not being processed
        queue_item.processing = 0
        local_db.replace_item_to_database(
            data=queue_item.as_dict, collection_name="queue"
        )
        # raise error
        raise ValueError(
            f" Unknown queue item type {queue_item.type} at network {network}"
        )


# processing types


def pull_from_queue_hypervisor_status(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        # get hypervisor static information
        if hypervisor_static := local_db.get_items_from_database(
            collection_name="static", find={"address": queue_item.address}
        ):
            hypervisor_static = hypervisor_static[0]

            if hypervisor := build_db_hypervisor(
                address=queue_item.address,
                network=network,
                block=queue_item.block,
                dex=hypervisor_static["dex"],
                cached=False,
                force_rpcType="private",
            ):
                # save hype
                local_db.set_status(data=hypervisor)

                # remove item from queue
                local_db.del_queue_item(queue_item.id)

                # log total process
                curr_time = time.time()
                logging.getLogger("benchmark").info(
                    f" {network} queue item {queue_item.type}  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
                )

                # set queue from hype status operation
                build_and_save_queue_from_hypervisor_status(
                    hypervisor_status=hypervisor, network=network
                )

                return True
            else:
                logging.getLogger(__name__).error(
                    f"Error building {network}'s hypervisor status for {queue_item.address}. Can't continue queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).error(
                f" {network} No hypervisor static found for {queue_item.address}. Can't continue queue item {queue_item.id}"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s hypervisor status queue item: {e}"
        )

    # free item ?
    to_free_or_not_to_free_item(queue_item=queue_item, local_db=local_db)

    return False


def pull_from_queue_reward_status(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    # check if item block is higher than static rewarder block
    if queue_item.block < queue_item.data["reward_static"]["block"]:
        logging.getLogger(__name__).error(
            f" {network} queue item {queue_item.id} block {queue_item.block} is lower than reward creation block {queue_item.data['reward_static']['block']}.Skipping and removing from queue"
        )
        # remove item from queue
        local_db.del_queue_item(queue_item.id)
        return True

    try:
        if reward_status_list := create_reward_status_from_hype_status(
            hypervisor_status=queue_item.data["hypervisor_status"],
            rewarder_static=queue_item.data["reward_static"],
            network=network,
        ):
            for reward_status in reward_status_list:
                # only save status if rewards per second are greater than 0
                if int(reward_status["rewards_perSecond"]) > 0:
                    local_db.set_rewards_status(data=reward_status)
                else:
                    logging.getLogger(__name__).debug(
                        f" {queue_item.id} has 0 rewards per second. Not saving it to database"
                    )

            # remove item from queue
            local_db.del_queue_item(queue_item.id)

            # log total process
            curr_time = time.time()
            logging.getLogger("benchmark").info(
                f" {network} queue item {queue_item.type}  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
            )

            return True

        else:
            logging.getLogger(__name__).debug(
                f" Cant get any reward status data for {network}'s {queue_item.address} rewarder"
            )
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s rewards status queue item: {e}"
        )

    # free item ?
    to_free_or_not_to_free_item(queue_item=queue_item, local_db=local_db)

    return False


def pull_from_queue_price(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        price_helper = price_scraper(cache=False)

        price, source = price_helper.get_price(
            network=network, token_id=queue_item.address, block=queue_item.block
        )

        if price:
            # save price into database
            database_global(mongo_url=mongo_url).set_price_usd(
                network=network,
                block=queue_item.block,
                token_address=queue_item.address,
                price_usd=price,
                source=source,
            )

            # remove item from queue
            local_db.del_queue_item(queue_item.id)

            # log total process
            curr_time = time.time()
            logging.getLogger("benchmark").info(
                f" {network} queue item {queue_item.type}  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
            )

            return True
        else:
            logging.getLogger(__name__).debug(
                f" Cant get price for {network}'s {queue_item.address} token"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s price queue item: {e}"
        )

    # free item ?
    to_free_or_not_to_free_item(queue_item=queue_item, local_db=local_db)

    return False


def pull_from_queue_block(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        dummy = (
            bep20(address=queue_item.address, network=network, block=queue_item.block)
            if network == "binance"
            else erc20(
                address=queue_item.address, network=network, block=queue_item.block
            )
        )

        if dummy._timestamp:
            # save block into database
            database_global(mongo_url=mongo_url).set_block(
                network=network, block=dummy.block, timestamp=dummy._timestamp
            )

            # remove item from queue
            local_db.del_queue_item(queue_item.id)

            # log total process
            curr_time = time.time()
            logging.getLogger("benchmark").info(
                f" {network} queue item {queue_item.type}  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
            )

            return True
        else:
            logging.getLogger(__name__).debug(
                f" Cant get timestamp for {network}'s block {queue_item.block}"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s block queue item: {e}"
        )

    # free item ?
    to_free_or_not_to_free_item(queue_item=queue_item, local_db=local_db)

    return False


# helper functions


def to_free_or_not_to_free_item(
    queue_item: QueueItem, local_db: database_local
) -> bool:
    """Free item from processing if count is lower than 5,
        so that after 5 fails, next time will need an unlock before processing, taking longer

    Args:
        queue_item (QueueItem):
        local_db (database_local):

    Returns:
        bool: freed or not
    """
    #
    #
    if queue_item.count < 5:
        local_db.free_queue_item(db_queue_item=queue_item.as_dict)
        return True
    else:
        logging.getLogger(__name__).debug(
            f" Not freeing {queue_item.type} {queue_item.id} from queue because it failed {queue_item.count} times. Will need to be unlocked by a 'check' command"
        )
        return False
