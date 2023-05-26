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
from bins.general.enums import Chain
from bins.general.general_utilities import seconds_to_time_passed
from bins.w3.builders import build_db_hypervisor
from bins.mixed.price_utilities import price_scraper
from bins.w3.onchain_utilities.basic import erc20


@dataclass
class scraping_queue:
    type: str
    block: int
    address: str
    data: dict
    processing: float = 0  # timestamp
    id: str | None = None
    creation: float = 0
    _id: str | None = None

    def __post_init__(self):
        if type == "reward_status":
            self.id = f"{self.type}_{self.block}_{self.address}_{self.data['hypervisor_status']['address']}"
        else:
            self.id = f"{self.type}_{self.block}_{self.address}"

        if self.creation == 0:
            self.creation = time.time()

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
        }


# PUSH DATA


def build_and_save_queue_from_operation(operation: dict, network: str):
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
        # hype status
        local_db.set_scraping_queue(
            data=scraping_queue(
                type="hypervisor_status",
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
            local_db.set_scraping_queue(
                data=scraping_queue(
                    type="block",
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

    # token 0 price
    local_db.save_item_to_database(
        collection_name="scraping_queue",
        data=scraping_queue(
            type="price",
            block=hypervisor_status["block"],
            address=hypervisor_status["pool"]["token0"]["address"],
            data=hypervisor_status,
        ).as_dict,
    )

    # token 1 price
    local_db.save_item_to_database(
        collection_name="scraping_queue",
        data=scraping_queue(
            type="price",
            block=hypervisor_status["block"],
            address=hypervisor_status["pool"]["token1"]["address"],
            data=hypervisor_status,
        ).as_dict,
    )

    # Rewards
    # get a list of reward_static rewardToken linked with hypervisor_address
    for reward_static in local_db.get_items_from_database(
        collection_name="reward_static",
        find={"hypervisor_address": hypervisor_status["address"]},
    ):
        # add price
        local_db.save_item_to_database(
            collection_name="scraping_queue",
            data=scraping_queue(
                type="price",
                block=hypervisor_status["block"],
                address=reward_static["rewardToken"],
                data=reward_static,
            ).as_dict,
        )

        # add reward_status
        local_db.save_item_to_database(
            collection_name="scraping_queue",
            data=scraping_queue(
                type="reward_status",
                block=hypervisor_status["block"],
                address=reward_static["rewarder_address"],
                data={
                    "reward_static": reward_static,
                    "hypervisor_status": hypervisor_status,
                },
            ).as_dict,
        )


# PULL DATA


def parallel_pull(network: str):
    args = [
        (network, "hypervisor_status"),
        (network, "block"),
        (network, "price"),
        (network, "reward_status"),
    ] * 5
    with concurrent.futures.ThreadPoolExecutor() as ex:
        for n in ex.map(lambda p: pull_from_queue(*p), args):
            pass


def pull_from_queue(network: str, type: str | None = None):
    # logging.getLogger(__name__).info(f">Processing {network}'s scraping queue")

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    # get first item from queue
    if db_queue_item := local_db.get_scraping_queue(type=type):
        try:
            # convert database queue item to class
            queue_item = scraping_queue(**db_queue_item)

            if queue_item.type == "hypervisor_status":
                return pull_from_queue_hypervisor_status(
                    network=network, queue_item=queue_item
                )
            elif queue_item.type == "rewards_status":
                return pull_from_queue_rewards_status(
                    network=network, queue_item=queue_item
                )
            elif queue_item.type == "price":
                return pull_from_queue_price(network=network, queue_item=queue_item)
            elif queue_item.type == "block":
                return pull_from_queue_block(network=network, queue_item=queue_item)
            else:
                # reset queue item
                queue_item.processing = 0
                local_db.replace_item_to_database(
                    data=queue_item.as_dict, collection_name="scraping_queue"
                )
                raise ValueError(
                    f" Unknown queue item type {queue_item.type} at network {network}"
                )
        except Exception as e:
            # reset queue item
            queue_item.processing = 0
            local_db.replace_item_to_database(
                data=queue_item.as_dict, collection_name="scraping_queue"
            )
            raise e
    # else:
    # no item found
    return True


def pull_from_queue_hypervisor_status(network: str, queue_item: scraping_queue) -> bool:
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
                local_db.del_scraping_queue(queue_item.id)

                # log total process
                curr_time = time.time()
                logging.getLogger(__name__).debug(
                    f" {network} queue item {queue_item.type}:  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
                )

                # set queue from hype status operation
                build_and_save_queue_from_hypervisor_status(
                    hypervisor_status=hypervisor, network=network
                )

                return True
            else:
                logging.getLogger(__name__).error(
                    f"Error building {network}'s hypervisor status for {queue_item.address}. Can't continue scraping queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).error(
                f" {network} No hypervisor static found for {queue_item.address}. Can't continue scraping queue item {queue_item.id}"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s hypervisor status scraping queue: {e}"
        )

    # free item from processing
    local_db.free_scraping_queue(data=queue_item.as_dict)
    return False


def pull_from_queue_rewards_status(network: str, queue_item: scraping_queue) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        if reward_status_list := create_reward_status_from_hype_status(
            hypervisor_status=queue_item.data["hypervisor_status"],
            rewarder_static=queue_item.data["rewarder_static"],
        ):
            for reward_status in reward_status_list:
                local_db.set_rewards_status(data=reward_status)

            # remove item from queue
            local_db.del_scraping_queue(queue_item.id)

            # log total process
            curr_time = time.time()
            logging.getLogger(__name__).debug(
                f" {network} queue item {queue_item.type}:  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
            )

            return True

        else:
            logging.getLogger(__name__).debug(
                f" Cant get any reward status data for {network}'s {queue_item.address} rewarder"
            )
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s rewards status scraping queue: {e}"
        )

    # free item from processing
    local_db.free_scraping_queue(data=queue_item.as_dict)

    return False


def pull_from_queue_price(network: str, queue_item: scraping_queue) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        price_helper = price_scraper(False)

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
            local_db.del_scraping_queue(queue_item.id)

            # log total process
            curr_time = time.time()
            logging.getLogger(__name__).debug(
                f" {network} queue item {queue_item.type}:  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
            )

            return True
        else:
            logging.getLogger(__name__).debug(
                f" Cant get price for {network}'s {queue_item.address} token"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s price scraping queue: {e}"
        )

    # free item from processing
    local_db.free_scraping_queue(data=queue_item.as_dict)

    return False


def pull_from_queue_block(network: str, queue_item: scraping_queue) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        dummy = erc20(
            address=queue_item.address, network=network, block=queue_item.block
        )

        if dummy._timestamp:
            # save block into database
            database_global(mongo_url=mongo_url).set_block(
                network=network, block=dummy.block, timestamp=dummy._timestamp
            )

            # remove item from queue
            local_db.del_scraping_queue(queue_item.id)

            # log total process
            curr_time = time.time()
            logging.getLogger(__name__).debug(
                f" {network} queue item {queue_item.type}:  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
            )

            return True
        else:
            logging.getLogger(__name__).debug(
                f" Cant get timestamp for {network}'s block {queue_item.block}"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s block scraping queue: {e}"
        )

    # free item from processing
    local_db.free_scraping_queue(data=queue_item.as_dict)

    return False
