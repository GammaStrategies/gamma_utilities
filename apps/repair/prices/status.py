import logging

import tqdm
from apps.feeds.queue.queue_item import QueueItem

from bins.configuration import CONFIGURATION, TOKEN_ADDRESS_EXCLUDE
from bins.database.common.database_ids import create_id_price
from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import get_from_localdb
from bins.general.enums import queueItemType


def repair_prices_from_status(
    batch_size: int = 100000, max_repair_per_network: int | None = None
):
    """Check prices not present in database but present in hypervisors and rewards status and add them to the QUEUE to be processed"""

    logging.getLogger(__name__).info(
        f">Check prices not present in database but present in hypervisors and rewards status and add them to the queue to be processed"
    )
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        with tqdm.tqdm(total=len(networks)) as progress_bar:
            for network in networks:
                try:
                    # database name
                    db_name = f"{network}_{protocol}"

                    # database helper
                    def _db():
                        return database_local(mongo_url=mongo_url, db_name=db_name)

                    logging.getLogger(__name__).info(
                        f" Building a price id list of addresses and blocks that should be present in the database for {network}"
                    )
                    # prices to get = all token0 and token1 addresses from hypervisor status + rewarder status blocks
                    # price id = network_block_address
                    (
                        price_ids_shouldBe,
                        blocks_shouldBe,
                    ) = shouldBe_price_ids_from_status_hypervisors(
                        network=network
                    )  # set()
                    # add rewards
                    price_ids_shouldBe.update(
                        shouldBe_price_ids_from_status_rewards(network=network)
                    )  # =  set()
                    # progress
                    progress_bar.set_description(
                        f" {network} should be prices: {len(price_ids_shouldBe)}"
                    )
                    progress_bar.update(0)

                    logging.getLogger(__name__).info(
                        f" Checking if there are {len(price_ids_shouldBe)} prices for {network} in the price database"
                    )

                    if price_ids_diffs := price_ids_shouldBe - set(
                        [
                            id["id"]
                            for id in database_global(
                                mongo_url=mongo_url
                            ).get_items_from_database(
                                collection_name="usd_prices",
                                find={"network": network},
                                batch_size=batch_size,
                                projection={"_id": 0, "id": 1},
                            )
                        ]
                    ):
                        logging.getLogger(__name__).info(
                            f" Found {len(price_ids_diffs)} missing prices for {network}"
                        )

                        try:
                            # check if those prices are already in the queue to be processed
                            # price id : create_id_price(network, block, address)
                            if price_ids_in_queue := set(
                                [
                                    create_id_price(network, x["block"], x["address"])
                                    for x in _db().get_items_from_database(
                                        collection_name="queue",
                                        find={"type": queueItemType.PRICE},
                                        batch_size=batch_size,
                                        projection={
                                            "_id": 0,
                                            "type": 1,
                                            "block": 1,
                                            "address": 1,
                                        },
                                    )
                                ]
                            ):
                                # remove prices already in queue from price_ids_diffs
                                logging.getLogger(__name__).info(
                                    f" Found {len(price_ids_in_queue)} prices already in queue for {network}. Removing them from the process list"
                                )
                                price_ids_diffs = price_ids_diffs - price_ids_in_queue

                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error getting queued prices for {network}"
                            )

                        # do not repair more than max_repair_per_network prices at once to avoid being too much time in the same network
                        if (
                            max_repair_per_network
                            and len(price_ids_diffs) > max_repair_per_network
                        ):
                            logging.getLogger(__name__).info(
                                f" Selecting a random sample of {max_repair_per_network} prices due to maximum repair limit set."
                            )
                            # choose to repair the most recent ones first
                            price_ids_diffs = sorted(price_ids_diffs, reverse=True)[
                                :max_repair_per_network
                            ]
                            # choose to repair the first max_repair_per_network
                            # price_ids_diffs = random.sample(
                            #     price_ids_diffs, max_repair_per_network
                            # )

                        # progress_bar.total += len(price_ids_diffs)

                        def create_queue_item(price_id):
                            network, block, address = price_id.split("_")
                            return QueueItem(
                                type=queueItemType.PRICE,
                                block=int(block),
                                address=address,
                                data={},
                            ).as_dict

                        # create a list of queue items tobe added to database
                        to_queue_items = [
                            create_queue_item(price_id) for price_id in price_ids_diffs
                        ]

                        # TODO: create a list of blocks to be added to database
                        # to_queue_items += [
                        #   QueueItem(
                        #        type=queueItemType.BLOCK,
                        #        block=block,
                        #        address="0x0000000000000000000000000000000000000000",
                        #        data={},
                        #    ).as_dict for block in blocks_shouldBe
                        # ]

                        # add to queue
                        if result := _db().replace_items_to_database(
                            data=to_queue_items, collection_name="queue"
                        ):
                            if (
                                result.inserted_count
                                or result.upserted_count
                                or result.modified_count
                            ):
                                logging.getLogger(__name__).debug(
                                    f" Added {len(to_queue_items)} prices to the queue for {network}."
                                )
                            else:
                                logging.getLogger(__name__).warning(
                                    f" No prices added to the queue for {network}. Database returned:  {result.bulk_api_result}"
                                )
                        else:
                            logging.getLogger(__name__).error(
                                f" No database return while adding prices to the queue for {network}."
                            )
                        progress_bar.update(1)

                    else:
                        logging.getLogger(__name__).info(
                            f" No missing prices found for {network}"
                        )

                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" error in {network} while repairing prices from hype status  {e} "
                    )

                # progress
                progress_bar.update(1)


def shouldBe_price_ids_from_status_rewards(
    network: str, batch_size: int = 100000
) -> set[str]:
    """List of price ids that should be present in the database, built using hypervisor status linked to static rewards

    Args:
        network (str):

    Returns:
        set[str]:
    """

    logging.getLogger(__name__).debug(
        f" Building a 'should be' price id list from rewards for {network}"
    )

    # create a result price ids
    price_ids = set()

    # special query to get the list
    query = [
        {
            "$project": {
                "hypervisor_address": "$hypervisor_address",
                "rewardToken": "$rewardToken",
            }
        },
        {
            "$lookup": {
                "from": "status",
                "localField": "hypervisor_address",
                "foreignField": "address",
                "as": "hypervisor_status",
                "pipeline": [
                    {"$project": {"block": "$block"}},
                ],
            }
        },
        {"$unwind": "$hypervisor_status"},
        {
            "$project": {
                "rewardToken": "$rewardToken",
                "block": "$hypervisor_status.block",
            }
        },
        {"$sort": {"block": 1}},
    ]

    # build a list of token addresses to exclude from this network
    addresses_to_exclude = list(TOKEN_ADDRESS_EXCLUDE.get(network, {}).keys())

    price_ids.update(
        [
            create_id_price(network, item["block"], item["rewardToken"])
            for item in get_from_localdb(
                network=network,
                collection="rewards_static",
                aggregate=query,
                batch_size=batch_size,
            )
            if item["rewardToken"] not in addresses_to_exclude
        ]
    )

    return price_ids


def shouldBe_price_ids_from_status_hypervisors(
    network: str, batch_size: int = 100000
) -> tuple[set[str], set]:
    """List of price ids that should be present in the database, built using hypervisor status
    Args:
        network (str):

    Returns:
        list[str]:  list of price ids and list of blocks
    """

    logging.getLogger(__name__).debug(
        f" Building a 'should be' price id list from hypervisor status for {network} (includes rewards)"
    )

    # create a result price ids
    price_ids = set()
    block_ids = set()

    # get all static rewards
    static_rewards = {
        x["hypervisor_address"]: x
        for x in get_from_localdb(
            network=network,
            collection="rewards_static",
            find={},
            batch_size=batch_size,
            projection={
                "block": 1,
                "timestamp": 1,
                "hypervisor_address": 1,
                "rewardToken": 1,
                "start_rewards_timestamp": 1,
                "end_rewards_timestamp": 1,
            },
        )
    }

    for hype_status in get_from_localdb(
        network=network,
        collection="status",
        find={},
        batch_size=batch_size,
        projection={
            "block": 1,
            "timestamp": 1,
            "pool.token0.address": 1,
            "pool.token1.address": 1,
            "address": 1,
        },
    ):
        # get all hypervisor status blocks and build a price id for each one
        price_ids.add(
            create_id_price(
                network, hype_status["block"], hype_status["pool"]["token0"]["address"]
            )
        )
        price_ids.add(
            create_id_price(
                network, hype_status["block"], hype_status["pool"]["token1"]["address"]
            )
        )
        # add block to block_ids
        block_ids.add(hype_status["block"])

        # add reward token to price ids
        if _static_reward := static_rewards.get(hype_status["address"], None):
            _start_timestamp = _static_reward.get(
                "start_rewards_timestamp", hype_status["timestamp"]
            )
            if _start_timestamp == 0:
                _start_timestamp = hype_status["timestamp"]
            _end_timestamp = _static_reward.get(
                "end_rewards_timestamp", hype_status["timestamp"]
            )
            if _end_timestamp == 0:
                _end_timestamp = hype_status["timestamp"]

            # make sure static reward start/end timestamps are within hypervisor status timestamp
            if _static_reward["block"] <= hype_status["block"]:
                if (
                    _start_timestamp <= hype_status["timestamp"]
                    and _end_timestamp >= hype_status["timestamp"]
                ):
                    # add reward token
                    price_ids.add(
                        create_id_price(
                            network,
                            hype_status["block"],
                            _static_reward["rewardToken"],
                        )
                    )

    return price_ids, block_ids
