from datetime import datetime, timezone
import logging
import concurrent.futures
import time
import tqdm
from apps.feeds.revenue_operations import (
    create_revenue_addresses,
    feed_revenue_operations_from_hypervisors,
    feed_revenue_operations_from_venft,
)

from apps.feeds.status.rewards.general import create_reward_status_from_hype_status
from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_rewards_static
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, Protocol, queueItemType, text_to_chain
from bins.general.general_utilities import seconds_to_time_passed
from bins.w3.builders import build_db_hypervisor, build_db_hypervisor_multicall


## Database re scraping items


## main rescraping func
def manual_reScrape(
    chain: Chain,
    loop_work: callable,
    find: dict = {},
    aggregate: list | None = None,
    sort: list = [("block", -1)],
    db_collection: str = "status",
    threaded: bool = True,
    rewrite: bool | None = None,
    max_workers: int | None = None,
):
    """Rescrape database items

    Args:
        chain (Chain):
        loop_work:  func to loop thru
        find (dict, optional): . Defaults to all.
        sort (list, optional): . Defaults to descending block
        db_collection (str, optional):  database collection to beguin rescraping with ... defaults to hypervisor "status",
        threaded (bool, optional): Defaults to yes,
        rewrite (bool, optional): rewrite items from database when no rewards found. Defaults to False.
    """

    batch_size = 100000

    if not sort:
        sort = [("block", -1)]

    logging.getLogger(__name__).info(
        f" Starting a manual {'threaded ' if threaded else ''}rescraping process using {db_collection} for {chain} {f'using filter: {find}' if find else ''} {f'sorted by : {sort}' if sort else ''} {f'rewrite on' if rewrite else ''}"
    )

    if (
        database_items := get_from_localdb(
            network=chain.database_name,
            collection=db_collection,
            aggregate=aggregate,
            batch_size=batch_size,
        )
        if aggregate
        else get_from_localdb(
            network=chain.database_name,
            collection=db_collection,
            find=find,
            batch_size=batch_size,
            sort=sort,
        )
    ):
        # if database_items := database_local(
        #     mongo_url=mongo_url, db_name=db_name
        # ).get_items_from_database(
        #     collection_name=db_collection,
        #     find=find,
        #     batch_size=batch_size,
        #     sort=sort,
        # ):
        error = 0
        ok = 0
        with tqdm.tqdm(total=len(database_items)) as progress_bar:
            if threaded:
                # prepare arguments for threaded
                args = (
                    (
                        item,
                        chain,
                        rewrite,
                    )
                    for item in database_items
                )
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as ex:
                    for result in ex.map(lambda p: loop_work(*p), args):
                        if result:
                            ok += 1
                        else:
                            error += 1

                        progress_bar.set_description(f" ok: {ok} error: {error}")
                        progress_bar.update(1)
            else:
                for item in database_items:
                    if result := loop_work(item, chain, rewrite):
                        ok += 1
                    else:
                        error += 1

                    progress_bar.set_description(f" ok: {ok} error: {error}")
                    progress_bar.update(1)


## specialized definitions for rescraping
def reScrape_loopWork_hypervisor_status(
    hype_status: dict,
    chain: Chain,
    rewrite: bool = False,
) -> bool:
    """Rescrape hypervisor status"""
    try:
        _starttime = time.time()
        if new_hypervisor := build_db_hypervisor_multicall(
            address=hype_status["address"],
            network=chain.database_name,
            block=hype_status["block"],
            dex=hype_status["dex"],
            pool_address=hype_status["pool"]["address"],
            token0_address=hype_status["pool"]["token0"]["address"],
            token1_address=hype_status["pool"]["token1"]["address"],
            force_rpcType="private",
        ):
            # TODO: compare n log diffs and rewrite
            err = False

            if not rewrite:
                # compare main differences
                if hype_status["block"] != new_hypervisor["block"]:
                    logging.getLogger(__name__).error(
                        f" Blocks differ for hype {hype_status['address']} original: {hype_status['block']} -> new: {new_hypervisor['block']}"
                    )
                    err = True

                check_fields = [
                    "totalSupply",
                    "fees_uncollected.qtty_token0",
                    "fees_uncollected.qtty_token0",
                    "dex",
                    "fee",
                    "pool.dex",
                ]
                for field in check_fields:
                    original = hype_status
                    new = new_hypervisor
                    for subfield in field.split("."):
                        original = original.get(subfield)
                        new = new.get(subfield)
                    if original != new:
                        logging.getLogger(__name__).debug(
                            f" {field} differ for hype {hype_status['address']} block {hype_status['block']} original: {original} -> new: {new}"
                        )

            # add to database
            if not err:
                if db_return := get_default_localdb(
                    network=chain.database_name
                ).set_status(data=new_hypervisor):
                    db_return.modified_count
                    # report object datetime
                    try:
                        object_datetime = datetime.fromtimestamp(
                            new_hypervisor["timestamp"],
                            tz=timezone.utc,
                        )
                    except:
                        object_datetime = None
                    # log database result
                    logging.getLogger(__name__).debug(
                        f"{chain.database_name} hypervisor {new_hypervisor['address']} block {new_hypervisor['block']} [{object_datetime.strftime('%Y-%m-%d') if object_datetime else ''} ] db->mod:{db_return.modified_count} ups:{db_return.upserted_id} match:{db_return.matched_count}"
                    )

                    curr_time = seconds_to_time_passed(time.time() - _starttime)
                    logging.getLogger("benchmark").info(
                        f" {chain.database_name} queue item {queueItemType.HYPERVISOR_STATUS}  processing time: {curr_time}  total lifetime: {curr_time}"
                    )

                    return True
            logging.getLogger(__name__).debug(
                f" {chain.database_name}'s hypervisor {new_hypervisor['address']} at block {new_hypervisor['block']} not saved"
            )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while processing hypervisor status {hype_status['address']} at block {hype_status['block']} -> error {e}"
        )

    return False


def reScrape_loopWork_rewards_status(
    rewarder_status: dict, chain: Chain, rewrite: bool | None = None
) -> bool:
    """Rescrape rewarder status"""
    try:
        _starttime = time.time()
        # get hypervisor static data from database
        if hypervisor_status := get_from_localdb(
            network=chain.database_name,
            collection="status",
            find={
                "address": rewarder_status["hypervisor_address"],
                "block": rewarder_status["block"],
            },
            limit=1,
        ):
            # get the only result item
            hypervisor_status = hypervisor_status[0]

            # get rewarder static
            rewarder_static = get_from_localdb(
                network=chain.database_name,
                collection="rewards_static",
                find={
                    "id": create_id_rewards_static(
                        hypervisor_address=rewarder_status["hypervisor_address"],
                        rewarder_address=rewarder_status["rewarder_address"],
                        rewardToken_address=rewarder_status["rewardToken"],
                    )
                },
            )
            if not rewarder_static:
                logging.getLogger(__name__).error(
                    f" No rewarder static found for hypervisor {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} rewardToken {rewarder_status['rewardToken']}"
                )
                return False
            else:
                rewarder_static = rewarder_static[0]

            # create rewarder status from hype
            for new_rewarder_status in create_reward_status_from_hype_status(
                hypervisor_status=hypervisor_status,
                rewarder_static=rewarder_static,
                network=chain.database_name,
            ):
                # save to database if rewards_perSecond > 0 or rewrite
                if float(new_rewarder_status["rewards_perSecond"]) > 0 or rewrite:
                    err = False
                    # compare main differences
                    if rewarder_status["block"] != new_rewarder_status["block"]:
                        logging.getLogger(__name__).error(
                            f" Blocks differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} original: {rewarder_status['block']} -> new: {new_rewarder_status['block']}"
                        )
                        err = True
                    if (
                        rewarder_status["rewards_perSecond"]
                        != new_rewarder_status["rewards_perSecond"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" rewards_perSecond differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['rewards_perSecond']} -> new: {new_rewarder_status['rewards_perSecond']}"
                        )
                    if (
                        rewarder_status["rewardToken_price_usd"]
                        != new_rewarder_status["rewardToken_price_usd"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" rewardToken_price_usd differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['rewardToken_price_usd']} -> new: {new_rewarder_status['rewardToken_price_usd']}"
                        )
                    if (
                        rewarder_status["token0_price_usd"]
                        != new_rewarder_status["token0_price_usd"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" token0_price_usd differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['token0_price_usd']} -> new: {new_rewarder_status['token0_price_usd']}"
                        )
                    if (
                        rewarder_status["token1_price_usd"]
                        != new_rewarder_status["token1_price_usd"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" token1_price_usd differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['token1_price_usd']} -> new: {new_rewarder_status['token1_price_usd']}"
                        )
                    if rewarder_status["apr"] != new_rewarder_status["apr"]:
                        logging.getLogger(__name__).debug(
                            f" apr differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['apr']} -> new: {new_rewarder_status['apr']}"
                        )

                    if not err:
                        # add to database
                        db_return = get_default_localdb(
                            network=chain.database_name
                        ).set_rewards_status(data=new_rewarder_status)
                        # TODO: log database result
                        curr_time = seconds_to_time_passed(time.time() - _starttime)
                        logging.getLogger("benchmark").info(
                            f" {chain.database_name} queue item {queueItemType.REWARD_STATUS}  processing time: {curr_time}  total lifetime: {curr_time}"
                        )
                        # report object datetime
                        try:
                            object_datetime = datetime.fromtimestamp(
                                new_rewarder_status["timestamp"],
                                tz=timezone.utc,
                            )
                        except:
                            object_datetime = None
                        logging.getLogger(__name__).info(
                            f"{chain.database_name} {new_rewarder_status['hypervisor_address']} {new_rewarder_status['block']} {new_rewarder_status['rewardToken_symbol']} reward status finished correctly [{object_datetime.strftime('%Y-%m-%d') if object_datetime else ''}]"
                        )
                        return True
                else:
                    logging.getLogger(__name__).debug(
                        f" {chain.database_name}'s hypervisor {new_rewarder_status['hypervisor_address']} at block {new_rewarder_status['block']}  {new_rewarder_status['rewardToken_symbol']}  not saved bc has 0 rewards per second"
                    )

                    curr_time = seconds_to_time_passed(time.time() - _starttime)
                    logging.getLogger("benchmark").info(
                        f" {chain.database_name} queue item {queueItemType.REWARD_STATUS}  processing time: {curr_time}  total lifetime: {curr_time}"
                    )

                    return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while processing rewarder status {rewarder_status['id']} -> error {e}"
        )

    return False


# custom fill gaps for revenue operations
def revenueOperations_fillGaps(chain: Chain):
    """Rescrape revenue operations from the first revenue operation block till now
    Already in database revenue operations will not be processed (queued)

    Args:
        chain (Chain):
    """

    logging.getLogger(__name__).info(
        f" {chain.database_name} filling revenue operations gaps, if any"
    )

    max_blocks_step = 5000
    rewrite = False

    # get first revenue operation
    if first_revenue_operation := get_from_localdb(
        network=chain.database_name,
        collection="revenue_operations",
        find={},
        sort=[("timestamp", 1)],
        limit=1,
    ):
        # get first revenue operation
        first_revenue_operation = first_revenue_operation[0]
        # set block ini
        block_ini = first_revenue_operation["blockNumber"]
        block_end = None
        # 1) transfers to hypervisor revenue addresses
        (
            addresses,
            block_ini,
            block_end,
        ) = create_revenue_addresses(
            network=chain.database_name,
            block_ini=block_ini,
            block_end=block_end,
            revenue_address_type="hypervisors",
        )
        if addresses:
            feed_revenue_operations_from_hypervisors(
                chain=chain,
                addresses=addresses,
                block_ini=block_ini,
                block_end=block_end,
                max_blocks_step=max_blocks_step,
                rewrite=rewrite,
            )

        # 2) rewardPaid to revenue addresses
        addresses, block_ini, block_end = create_revenue_addresses(
            network=chain.database_name,
            block_ini=block_ini,
            block_end=block_end,
            revenue_address_type="venft",
        )
        if addresses:
            feed_revenue_operations_from_venft(
                chain=chain,
                addresses=addresses,
                block_ini=block_ini,
                block_end=block_end,
                max_blocks_step=max_blocks_step,
                rewrite=rewrite,
            )
    else:
        logging.getLogger(__name__).info(
            f" {chain.database_name} no revenue operations found"
        )


def main(option=None):
    # TODO: add options logic
    # options are collection, custom find, custom sort, threaded
    main = "gamma"
    # override networks if specified in cml
    networks = (
        CONFIGURATION["_custom_"]["cml_parameters"].networks
        or CONFIGURATION["script"]["protocols"][main]["networks"]
    )
    for network in networks:
        # override protocols if specified in cml
        protocols = (
            CONFIGURATION["_custom_"]["cml_parameters"].protocols
            or CONFIGURATION["script"]["protocols"][main]["networks"][network]
        )
        for protocol in protocols:
            # Hypervisor status  ( first, as its a backbone to all other statuses )
            if option == "status" or option == "all":
                manual_reScrape(
                    chain=text_to_chain(network),
                    loop_work=reScrape_loopWork_hypervisor_status,
                    find={"dex": protocol},
                    sort=[("block", -1)],
                    db_collection="status",
                    threaded=True,
                    rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                )

            # Rewards status
            if option == "rewards_status" or option == "all":
                manual_reScrape(
                    chain=text_to_chain(network),
                    loop_work=reScrape_loopWork_rewards_status,
                    find={"dex": protocol},
                    sort=[("block", -1)],
                    db_collection="rewards_status",
                    threaded=True,
                    rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                )

            # special option
            if option == "status_fees_collected":
                # rescrape when fees_collected field does not exist
                manual_reScrape(
                    chain=text_to_chain(network),
                    loop_work=reScrape_loopWork_hypervisor_status,
                    find={"dex": protocol, "fees_collected": {"$exists": False}},
                    sort=[("block", 1)],
                    db_collection="status",
                    threaded=True,
                    rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                )

        # Revenue operations
        if option == "revenue_operations":
            revenueOperations_fillGaps(chain=text_to_chain(network))
