# hypervisors static data
from dataclasses import dataclass, asdict
import logging
import time
import concurrent.futures

from bins.w3.protocols.gamma.hypervisor import gamma_hypervisor_bep20

from .status import (
    create_and_save_hypervisor_status,
    create_reward_status_from_hype_status,
)
from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import (
    create_id_block,
    create_id_hypervisor_static,
    create_id_hypervisor_status,
    create_id_operation,
    create_id_price,
    create_id_queue,
    create_id_rewards_static,
    create_id_rewards_status,
)
from bins.database.common.db_collections_common import database_global, database_local
from bins.general.enums import Chain, queueItemType, text_to_chain
from bins.general.general_utilities import seconds_to_time_passed
from bins.w3.builders import build_db_hypervisor, build_erc20_helper
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
        if self.type == queueItemType.REWARD_STATUS:
            # reward status should have rewardToken as id
            if "reward_static" in self.data:
                # TODO: change for a combination of queue id + reward id
                self.id = create_id_queue(
                    type=self.type,
                    block=self.block,
                    hypervisor_address=self.data["reward_static"]["hypervisor_address"],
                    rewarder_address=self.data["reward_static"]["rewarder_address"],
                    rewardToken_address=self.data["reward_static"]["rewardToken"],
                )
            else:
                raise ValueError(
                    f" {self.data} is missing reward_static. using id: {self.id}"
                )
                # self.id = create_id_queue(
                #     type=self.type,
                #     block=self.block,
                #     hypervisor_address=self.data['hypervisor_status']['address'],
                #     )
                # self.id = f"{self.type}_{self.block}_{self.address}_{self.data['hypervisor_status']['address']}"
                # logging.getLogger(__name__).error(
                #     f" {self.data} is missing reward_static. using id: {self.id}"
                # )

        elif self.type == queueItemType.OPERATION:
            self.id = create_id_queue(
                type=self.type, block=self.block, hypervisor_address=self.address
            )
            # add operation id
            if "logIndex" in self.data and "transactionHash" in self.data:
                self.id = f"{self.id}_{create_id_operation(logIndex=self.data['logIndex'], transactionHash=self.data['transactionHash'])}"

        else:
            self.id = create_id_queue(
                type=self.type, block=self.block, hypervisor_address=self.address
            )

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

    logging.getLogger(__name__).info(
        f"Processing {network}'s {queue_item.type} queue item id {queue_item.id} at block {queue_item.block}"
    )

    if queue_item.type == queueItemType.HYPERVISOR_STATUS:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_hypervisor_status,
        )
        # return pull_from_queue_hypervisor_status(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.REWARD_STATUS:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_reward_status,
        )
        # return pull_from_queue_reward_status(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.PRICE:
        return pull_common_processing_work(
            network=network, queue_item=queue_item, pull_func=pull_from_queue_price
        )
        # return pull_from_queue_price(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.BLOCK:
        return pull_common_processing_work(
            network=network, queue_item=queue_item, pull_func=pull_from_queue_block
        )
        # return pull_from_queue_block(network=network, queue_item=queue_item)
    elif queue_item.type == queueItemType.OPERATION:
        return pull_common_processing_work(
            network=network, queue_item=queue_item, pull_func=pull_from_queue_operation
        )
    else:
        # reset queue item

        # set queue item as not being processed
        # queue_item.processing = 0
        database_local(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
            db_name=f"{network}_gamma",
        ).replace_item_to_database(data=queue_item.as_dict, collection_name="queue")
        # raise error
        raise ValueError(
            f" Unknown queue item type {queue_item.type} at network {network}"
        )


# processing types


# Main processing function
def pull_common_processing_work(
    network: str, queue_item: QueueItem, pull_func: callable
):
    # build a result variable
    result = pull_func(network=network, queue_item=queue_item)

    # benchmark
    if result:
        # remove item from queue
        if db_return := database_local(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
            db_name=f"{network}_gamma",
        ).del_queue_item(queue_item.id):
            if db_return.deleted_count or db_return.acknowledged:
                logging.getLogger(__name__).debug(
                    f" {network}'s queue item {queue_item.id} has been removed from queue"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" {network}'s queue item {queue_item.id} has not been removed from queue. database returned {db_return.raw_result}"
                )
        else:
            logging.getLogger(__name__).error(
                f"  No database return received when deleting {network}'s queue item {queue_item.id}."
            )

        # log total process
        curr_time = time.time()
        logging.getLogger("benchmark").info(
            f" {network} queue item {queue_item.type}  processing time: {seconds_to_time_passed(curr_time - queue_item.processing)}  total lifetime: {seconds_to_time_passed(curr_time - queue_item.creation)}"
        )
    else:
        # free item ?
        to_free_or_not_to_free_item(network=network, queue_item=queue_item)

    # return result
    return result


# Specific processing functions
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
                if db_return := local_db.set_status(data=hypervisor):
                    # evaluate if price has been saved
                    if (
                        db_return.upserted_id
                        or db_return.modified_count
                        or db_return.matched_count
                    ):
                        logging.getLogger(__name__).debug(
                            f" {network} queue item {queue_item.id} hypervisor status saved to database"
                        )
                        # set queue from hype status operation
                        build_and_save_queue_from_hypervisor_status(
                            hypervisor_status=hypervisor, network=network
                        )
                        # set result
                        return True
                    else:
                        logging.getLogger(__name__).error(
                            f" {network} queue item {queue_item.id} reward status not saved to database. database returned: {db_return.raw_result}"
                        )
                else:
                    logging.getLogger(__name__).error(
                        f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                    )

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

    # return result
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
        return True
    else:
        try:
            if reward_status_list := create_reward_status_from_hype_status(
                hypervisor_status=queue_item.data["hypervisor_status"],
                rewarder_static=queue_item.data["reward_static"],
                network=network,
            ):
                for reward_status in reward_status_list:
                    # only save status if rewards per second are greater than 0
                    if int(reward_status["rewards_perSecond"]) > 0:
                        if db_return := local_db.set_rewards_status(data=reward_status):
                            # evaluate if price has been saved
                            if (
                                db_return.upserted_id
                                or db_return.modified_count
                                or db_return.matched_count
                            ):
                                logging.getLogger(__name__).debug(
                                    f" {network} queue item {queue_item.id} reward status saved to database"
                                )
                                # define result
                                return True
                            else:
                                logging.getLogger(__name__).error(
                                    f" {network} queue item {queue_item.id} reward status not saved to database. database returned: {db_return.raw_result}"
                                )
                        else:
                            logging.getLogger(__name__).error(
                                f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                            )
                    else:
                        logging.getLogger(__name__).debug(
                            f" {queue_item.id} has 0 rewards per second. Not saving it to database"
                        )
                        # define result
                        return True

            else:
                logging.getLogger(__name__).debug(
                    f" Cant get any reward status data for {network}'s {queue_item.address} rewarder"
                )
        except Exception as e:
            logging.getLogger(__name__).error(
                f"Error processing {network}'s rewards status queue item: {e}"
            )

    return False


def pull_from_queue_price(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    try:
        # set price gatherer
        price_helper = price_scraper(
            cache=False, thegraph=False, geckoterminal_sleepNretry=True
        )
        # get price
        price, source = price_helper.get_price(
            network=network, token_id=queue_item.address, block=queue_item.block
        )

        if price:
            # save price into database
            if db_return := database_global(mongo_url=mongo_url).set_price_usd(
                network=network,
                block=queue_item.block,
                token_address=queue_item.address,
                price_usd=price,
                source=source,
            ):
                # evaluate if price has been saved
                if (
                    db_return.upserted_id
                    or db_return.modified_count
                    or db_return.matched_count
                ):
                    logging.getLogger(__name__).debug(f" {network} price saved")

                    return True

                else:
                    logging.getLogger(__name__).error(
                        f" {network} price not saved. Database returned :{db_return.raw_result}"
                    )
            else:
                logging.getLogger(__name__).error(
                    f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).debug(
                f" Cant get price for {network}'s {queue_item.address} token"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s price queue item: {e}"
        )

    # return result
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
            if db_return := database_global(mongo_url=mongo_url).set_block(
                network=network, block=dummy.block, timestamp=dummy._timestamp
            ):
                # evaluate if price has been saved
                if (
                    db_return.upserted_id
                    or db_return.modified_count
                    or db_return.matched_count
                ):
                    logging.getLogger(__name__).debug(
                        f" {network} queue item {queue_item.id} block saved to database"
                    )
                    # define result
                    return True
                else:
                    logging.getLogger(__name__).error(
                        f" {network} queue item {queue_item.id} block not saved to database. database returned: {db_return.raw_result}"
                    )
            else:
                logging.getLogger(__name__).error(
                    f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).debug(
                f" Cant get timestamp for {network}'s block {queue_item.block}"
            )

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error processing {network}'s block queue item: {e}"
        )

    # return result
    return False


def pull_from_queue_operation(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    dumb_erc20 = build_erc20_helper(chain=text_to_chain(network))

    try:
        # the operation is in the 'data' field...
        operation = queue_item.data

        # set operation id (same hash has multiple operations)
        operation["id"] = create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        )
        # lower case address ( to ease comparison )
        operation["address"] = operation["address"].lower()

        # log
        logging.getLogger(__name__).debug(
            f"  -> Processing {network}'s operation {operation['id']}"
        )

        # set timestamp
        operation["timestamp"] = dumb_erc20.timestampFromBlockNumber(
            block=int(operation["blockNumber"])
        )

        # get hype from db
        if hypervisor := local_db.get_items_from_database(
            collection_name="static",
            find={
                "id": create_id_hypervisor_static(
                    hypervisor_address=operation["address"]
                )
            },
        ):
            hypervisor = hypervisor[0]

        else:
            raise ValueError(
                f" No static hypervisor found for {operation['address']} while processing operation {operation['id']}"
            )

        # set tokens data
        operation["decimals_token0"] = hypervisor["pool"]["token0"]["decimals"]
        operation["decimals_token1"] = hypervisor["pool"]["token1"]["decimals"]
        operation["decimals_contract"] = hypervisor["decimals"]

        # save operation to database
        if db_return := local_db.set_operation(data=operation):
            logging.getLogger(__name__).debug(f" Saved operation {operation['id']}")

        # make sure hype is not in status collection already
        if not local_db.get_items_from_database(
            collection_name="status",
            find={
                "id": create_id_hypervisor_status(
                    hypervisor_address=operation["address"],
                    block=operation["blockNumber"],
                )
            },
            projection={"id": 1},
        ):
            # fire scrape event on block regarding hypervisor and rewarders snapshots (status) and token prices
            # build queue events from operation
            build_and_save_queue_from_operation(operation=operation, network=network)

        else:
            logging.getLogger(__name__).debug(
                f"  Not pushing {operation['address']} hypervisor status queue item bcause its already in the database"
            )

        # log
        logging.getLogger(__name__).debug(
            f"  <- Done processing {network}'s operation {operation['id']}"
        )

        return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s operation queue item: {e}"
        )

    # return result
    return False


# DEPRECATED
def pull_from_queue_operation_OLD(network: str, queue_item: QueueItem) -> bool:
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    try:
        # the operation is in the 'data' field...
        operation = queue_item.data

        # set operation id (same hash has multiple operations)
        operation["id"] = create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        )

        # log
        logging.getLogger(__name__).debug(
            f"  -> Processing {network}'s operation {operation['id']}"
        )

        # lower case address ( to ease comparison )
        operation["address"] = operation["address"].lower()

        # save operation to database
        if db_return := local_db.set_operation(data=operation):
            logging.getLogger(__name__).debug(f" Saved operation {operation['id']}")

        # make sure hype is not in status collection already
        if not local_db.get_items_from_database(
            collection_name="status",
            find={
                "id": create_id_hypervisor_status(
                    hypervisor_address=operation["address"],
                    block=operation["blockNumber"],
                )
            },
            projection={"id": 1},
        ):
            # fire scrape event on block regarding hypervisor and rewarders snapshots (status) and token prices
            # build queue events from operation
            build_and_save_queue_from_operation(operation=operation, network=network)

        else:
            logging.getLogger(__name__).debug(
                f"  Not pushing {operation['address']} hypervisor status queue item bcause its already in the database"
            )

        # log
        logging.getLogger(__name__).debug(
            f"  <- Done processing {network}'s operation {operation['id']}"
        )

        return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s operation queue item: {e}"
        )

    # return result
    return False


# helper functions


def to_free_or_not_to_free_item(
    network: str,
    queue_item: QueueItem,
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
        if db_return := database_local(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
            db_name=f"{network}_gamma",
        ).free_queue_item(db_queue_item=queue_item.as_dict):
            if db_return.modified_count or db_return.upserted_id:
                logging.getLogger(__name__).debug(
                    f" Freed {queue_item.type} {queue_item.id} from queue"
                )
                return True
            else:
                logging.getLogger(__name__).error(
                    f" Could not free {queue_item.type} {queue_item.id} from queue. Database returned: {db_return.raw_result}"
                )
        else:
            logging.getLogger(__name__).error(
                f" No database return received while trying to free queue item {queue_item.id}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" Not freeing {queue_item.type} {queue_item.id} from queue because it failed {queue_item.count} times. Will need to be unlocked by a 'check' command"
        )
        # save item with count
        if db_return := database_local(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
            db_name=f"{network}_gamma",
        ).set_queue_item(data=queue_item.as_dict):
            logging.getLogger(__name__).debug(
                f" Saved {queue_item.type} {queue_item.id} with count {queue_item.count} to queue"
            )

    return False
