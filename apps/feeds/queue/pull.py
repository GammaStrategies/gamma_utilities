from datetime import datetime, timezone
import logging
import time
import concurrent.futures
from apps.feeds.latest.mutifeedistribution.item import multifeeDistribution_snapshot
from apps.feeds.queue.helpers import to_free_or_not_to_free_item
from apps.feeds.queue.pulls.mfd import pull_from_queue_latest_multiFeeDistribution
from apps.feeds.queue.push import (
    build_and_save_queue_from_hypervisor_static,
    build_and_save_queue_from_hypervisor_status,
    build_and_save_queue_from_operation,
)
from apps.feeds.queue.queue_item import QueueItem
from apps.feeds.static import _create_hypervisor_static_dbObject
from apps.feeds.utils import get_hypervisor_price_per_share_from_status
from bins.checkers.address import check_is_token
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
    get_latest_price_from_db,
    get_latest_prices_from_db,
    get_price_from_db,
)
from apps.errors.actions import process_error
from bins.errors.general import ProcessingError

from ..status.rewards.general import create_reward_status_from_hype_status

from bins.configuration import CONFIGURATION, TOKEN_ADDRESS_EXCLUDE
from bins.database.common.database_ids import (
    create_id_hypervisor_static,
    create_id_hypervisor_status,
    create_id_latest_multifeedistributor,
    create_id_operation,
)
from bins.database.common.db_collections_common import database_global, database_local
from bins.general.enums import (
    Chain,
    Protocol,
    queueItemType,
    text_to_chain,
    text_to_protocol,
)
from bins.general.general_utilities import log_time_passed, seconds_to_time_passed
from bins.w3.builders import (
    build_db_hypervisor,
    build_erc20_helper,
    build_hypervisor,
    build_db_hypervisor_multicall,
)
from bins.mixed.price_utilities import price_scraper
from bins.w3.protocols.general import erc20, bep20


# PULL DATA


def parallel_pull(network: str):
    # TEST funcion: use parallel_feed.py instead
    args = [
        (network, [queueItemType.HYPERVISOR_STATUS, queueItemType.PRICE], None, None),
        (network, [queueItemType.BLOCK], None, None),
        (network, [queueItemType.REWARD_STATUS], None, None),
    ] * 5
    with concurrent.futures.ThreadPoolExecutor() as ex:
        for n in ex.map(lambda p: pull_from_queue(*p), args):
            pass


def pull_from_queue(
    network: str,
    types: list[queueItemType] | None = None,
    find: dict | None = None,
    sort: list | None = None,
):
    # get first item from queue
    if db_queue_item := get_item_from_queue(
        network=network, types=types, find=find, sort=sort
    ):
        try:
            # convert database queue item to class
            queue_item = QueueItem(**db_queue_item)

            logging.getLogger(__name__).debug(
                f" Processing {queue_item.type} queue item -> count: {queue_item.count} creation: {log_time_passed.get_timepassed_string(datetime.fromtimestamp(queue_item.creation,timezone.utc))} ago"
            )

            # process queue item
            return process_queue_item_type(network=network, queue_item=queue_item)

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error processing {queue_item.type} queue item: {e}"
            )
            # set queue item free but save counter
            if db_result := get_default_localdb(network=network).free_queue_item(
                id=queue_item.id, count=queue_item.count
            ):
                logging.getLogger(__name__).debug(
                    f" {network}'s queue item {queue_item.id} has been set as not being processed, and count has been updated"
                )

            raise e
    else:
        # no item found
        logging.getLogger(__name__).debug(f" No queue item found for {network}")

    return True


def get_item_from_queue(
    network: str,
    types: list[queueItemType] | None = None,
    find: dict | None = None,
    sort: list | None = None,
) -> dict | None:
    """FIFO queue but error count zero have priority over > 0.
    Get first item not being processed

    Args:
        network (str):
        types (list[queueItemType] | None, optional): . Defaults to All.
        find (dict, optional): . Defaults to {"processing": 0, "count": {"$lt": 5}}.
        sort (list, optional): . Defaults to [("count", 1), ("creation", 1)]. 1 is ascending, -1 is descending

    Returns:
        dict | None: queue item
    """
    if not find:
        find = {"processing": 0, "count": {"$lt": 5}}
    if not sort:
        sort = [("count", 1), ("creation", 1)]

    return get_default_localdb(network=network).get_queue_item(
        types=types,
        find=find,
        sort=sort,
    )


# classifier
def process_queue_item_type(network: str, queue_item: QueueItem) -> bool:
    """Get item from queue and process it.

        Items with count>0 will be processed if queue.can_be_processed is True

    Args:
        network (str): network name
        queue_item (QueueItem):


    Returns:
        bool: processed successfully or not
    """

    if queue_item.can_be_processed == False:
        logging.getLogger(__name__).error(
            f" {network}'s queue item {queue_item.id} cannot be processed yet (more cooldown time defined). Will be processed later"
        )
        return False

    logging.getLogger(__name__).info(
        f"Processing {network}'s {queue_item.type} queue item with count {queue_item.count} at block {queue_item.block}"
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

    elif queue_item.type == queueItemType.LATEST_MULTIFEEDISTRIBUTION:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_latest_multiFeeDistribution,
        )

    elif queue_item.type == queueItemType.REVENUE_OPERATION:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_revenue_operation,
        )
    elif queue_item.type == queueItemType.HYPERVISOR_STATIC:
        return pull_common_processing_work(
            network=network,
            queue_item=queue_item,
            pull_func=pull_from_queue_hypervisor_static,
        )

    else:
        # reset queue item

        # set queue item as not being processed
        if db_result := get_default_localdb(network=network).free_queue_item(
            id=queue_item.id
        ):
            logging.getLogger(__name__).debug(
                f" {network}'s queue item {queue_item.id} has been set as not being processed"
            )

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
        if db_return := get_default_localdb(network=network).del_queue_item(
            queue_item.id
        ):
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

            if hypervisor := build_db_hypervisor_multicall(
                address=queue_item.address,
                network=network,
                block=queue_item.block,
                dex=hypervisor_static["dex"],
                pool_address=hypervisor_static["pool"]["address"],
                token0_address=hypervisor_static["pool"]["token0"]["address"],
                token1_address=hypervisor_static["pool"]["token1"]["address"],
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
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s hypervisor status queue item: {e}"
        )

    # return result
    return False


def pull_from_queue_reward_status(network: str, queue_item: QueueItem) -> bool:
    # control var
    _result = False

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
                for idx, reward_status in enumerate(reward_status_list):
                    # only save status if rewards per second are greater than 0
                    tmp = 0
                    try:
                        tmp = int(reward_status["rewards_perSecond"])
                    except Exception as e:
                        logging.getLogger(__name__).warning(
                            f"  rewards per second are float not int at reward status id: {reward_status['dex']}'s hype {reward_status['hypervisor_address']} rewarder address {reward_status['rewarder_address']}  block {reward_status['block']}  rewardsXsec {reward_status['rewards_perSecond']}"
                        )
                        tmp = float(reward_status["rewards_perSecond"])

                    if tmp > 0:
                        if db_return := get_default_localdb(
                            network=network
                        ).set_rewards_status(data=reward_status):
                            # evaluate if price has been saved
                            if (
                                db_return.upserted_id
                                or db_return.modified_count
                                or db_return.matched_count
                            ):
                                logging.getLogger(__name__).debug(
                                    f" {network} queue item {queue_item.id} reward status saved to database -- reward status num. {idx} of {len(reward_status_list)}"
                                )
                                _result = True
                            else:
                                logging.getLogger(__name__).error(
                                    f" {network} queue item {queue_item.id} reward status not saved to database. database returned: {db_return.raw_result} -- reward status num. {idx} of {len(reward_status_list)}"
                                )
                        else:
                            logging.getLogger(__name__).error(
                                f" No database return received while trying to save results for {network} queue item {queue_item.id} -- reward status num. {idx} of {len(reward_status_list)}"
                            )
                    else:
                        logging.getLogger(__name__).debug(
                            f" {queue_item.id} has 0 rewards per second. Not saving it to database -- reward status num. {idx} of {len(reward_status_list)}"
                        )
                        _result = True

            else:
                logging.getLogger(__name__).debug(
                    f" Cant get any reward status data for {network}'s {queue_item.address} rewarder->  dex: {queue_item.data['hypervisor_status'].get('dex', 'unknown')} hype address: {queue_item.data['hypervisor_status'].get('address', 'unknown')}  block: {queue_item.block}."
                )
                # cases log count:
                #   ( count 20 )  rewarder has no status rewards at/before this block ( either there are none or the database is not updated)
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error processing {network}'s rewards status queue item: {e}"
            )

    return _result


def pull_from_queue_price(network: str, queue_item: QueueItem) -> bool:
    # check prices not to process
    if queue_item.address.lower() in TOKEN_ADDRESS_EXCLUDE.get(network, {}):
        logging.getLogger(__name__).debug(
            f" {network} queue item {queue_item.id} price is excluded from processing. Removing from queue"
        )
        # remove from queue
        return True

    # check if address is actually a contract
    if not check_is_token(chain=text_to_chain(network), address=queue_item.address):
        # remove from queue
        logging.getLogger(__name__).debug(
            f" {network} queue item {queue_item.id} address {queue_item.address} is not a contract. Removing from queue"
        )
        return True

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


def pull_from_queue_revenue_operation(network: str, queue_item: QueueItem) -> bool:
    try:
        # the operation is in the 'data' field...
        operation = queue_item.data

        # set operation id (same hash has multiple operations)
        operation["id"] = create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        )
        # lower case address ( to ease comparison ) ( token address )
        operation["address"] = operation["address"].lower()

        # log
        logging.getLogger(__name__).debug(
            f"  -> Processing {network}'s revenue operation {operation['id']}"
        )

        # select token address
        if operation["topic"] == "rewardPaid":
            _token_address = operation["token"]
        elif operation["topic"] == "transfer":
            _token_address = operation["address"]
        else:
            raise ValueError(f" Unknown operation topic {operation['topic']}")

        dumb_erc20 = build_erc20_helper(
            chain=text_to_chain(network), address=_token_address
        )

        # set timestamp
        operation["timestamp"] = dumb_erc20.timestampFromBlockNumber(
            block=int(operation["blockNumber"])
        )
        # set year and month
        operation["year"] = datetime.fromtimestamp(
            operation["timestamp"], timezone.utc
        ).year
        operation["month"] = datetime.fromtimestamp(
            operation["timestamp"], timezone.utc
        ).month

        # set tokens symbol and decimals
        _process_price = True
        try:
            operation["symbol"] = dumb_erc20.symbol
            operation["decimals"] = dumb_erc20.decimals
        except Exception as e:
            # sometimes, the address is not an ERC20 but NFT like or other,
            # so it has no symbol or decimals
            operation["decimals"] = 0
            if not "symbol" in operation:
                operation["symbol"] = "unknown"
            _process_price = False
            operation["usd_value"] = 0

        # process operation by topic
        if operation["topic"] == "rewardPaid":
            # get dex from configured fixed revenue addresses
            if fixed_revenue_addressDex := (
                CONFIGURATION["script"]["protocols"]["gamma"]
                .get("filters", {})
                .get("revenue_wallets", {})
                .get(network, {})
                or {}
            ):
                # TODO: change on new configuration
                try:
                    operation["dex"] = fixed_revenue_addressDex.get(
                        operation["user"], ""
                    ).split("_")[1]
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Cant get dex from fixed revenue address {operation['user']} from {fixed_revenue_addressDex}"
                    )

        elif operation["topic"] == "transfer":
            # get dex from database
            if hypervisor_static := get_from_localdb(
                network=network,
                collection="static",
                find={
                    "id": operation["src"],
                },
            ):
                hypervisor_static = hypervisor_static[0]
                # this is a hypervisor fee related operation
                operation["dex"] = hypervisor_static["dex"]

        else:
            # unknown operation topic
            # raise ValueError(f" Unknown operation topic {operation['topic']}")
            pass

        # get price at block
        price = 0

        # check if this is a mint operation ( nft or hype LP provider as user ...)
        if (
            "src" in operation
            and operation["src"] == "0x0000000000000000000000000000000000000000"
        ):
            logging.getLogger(__name__).debug(
                f" Mint operation found in queue for {network} {operation['address']} at block {operation['blockNumber']}"
            )
            # may be a gamma hypervisor address or other

        # price
        if _process_price:
            # if token address is an hypervisor address, get share price
            if hypervisor_status := get_from_localdb(
                network=network,
                collection="status",
                find={
                    "address": operation["address"],
                    "block": operation["blockNumber"],
                },
            ):
                # this is a hypervisor address
                hypervisor_status = hypervisor_status[0]
                # get token prices from database
                try:
                    price = get_hypervisor_price_per_share_from_status(
                        network=network, hypervisor_status=hypervisor_status
                    )
                except Exception as e:
                    pass
            else:
                # try get price from database
                try:
                    price = get_price_from_db(
                        network=network,
                        block=operation["blockNumber"],
                        token_address=_token_address,
                    )
                except Exception as e:
                    # no database price
                    pass

            if price in [0, None]:
                # scrape price
                price_helper = price_scraper(
                    cache=True, thegraph=False, geckoterminal_sleepNretry=True
                )
                price, source = price_helper.get_price(
                    network=network,
                    token_id=_token_address,
                    block=operation["blockNumber"],
                )

                if price in [0, None]:
                    logging.getLogger(__name__).debug(
                        f"  Cant get price for {network}'s {_token_address} token at block {operation['blockNumber']}. Value will be zero"
                    )
                    price = 0

            try:
                operation["usd_value"] = price * (
                    int(operation["qtty"]) / 10 ** operation["decimals"]
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Setting usd_value = 0 -> Error:  {e}"
                )
                operation["usd_value"] = 0

        # save operation to database
        if db_return := get_default_localdb(network=network).replace_item_to_database(
            data=operation, collection_name="revenue_operations"
        ):
            logging.getLogger(__name__).debug(
                f" Saved revenue operation {operation['id']} - > mod: {db_return.modified_count}  matched: {db_return.matched_count}"
            )

        # log
        logging.getLogger(__name__).debug(
            f"  <- Done processing {network}'s revenue operation {operation['id']}"
        )

        return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s revenue operation queue item: {e}"
        )

    # return result
    return False


def pull_from_queue_hypervisor_static(network: str, queue_item: QueueItem) -> bool:
    if hype_static := _create_hypervisor_static_dbObject(
        address=queue_item.address,
        network=network,
        dex=queue_item.data["protocol"],
        enforce_contract_creation=CONFIGURATION["_custom_"][
            "cml_parameters"
        ].enforce_contract_creation,
    ):
        # add hypervisor static data to database
        if db_return := get_default_localdb(network=network).set_static(
            data=hype_static
        ):
            logging.getLogger(__name__).debug(
                f" {network}'s hypervisor {queue_item.address} static dbResult-> mod:{db_return.modified_count} ups:{db_return.upserted_id} match: {db_return.matched_count}"
            )

            # create a reward static queue item for this hypervisor, if needed
            if queue_item.data.get("create_reward_static", False):
                build_and_save_queue_from_hypervisor_static(
                    hypervisor_static=hype_static, network=network
                )

            return True

        logging.getLogger(__name__).debug(
            f" {network}'s hypervisor {queue_item.address} static could not be saved"
        )

    else:
        logging.getLogger(__name__).debug(
            f" {network}'s hypervisor {queue_item.address} static dictionary object could not be created"
        )
    return False
