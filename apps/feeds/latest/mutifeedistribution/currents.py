# Feed of latest block data

import logging
from apps.feeds.operations import task_enqueue_operations

from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, queueItemType, rewarderType
from bins.w3.builders import build_hypervisor
from bins.w3.protocols.ramses.collectors import (
    create_multiFeeDistribution_data_collector,
)


def feed_latest_multifeedistribution_snapshot():
    """add a multifeedistribution snapshot to the queue

    Args:
        chain (Chain):
    """
    #
    chain = Chain.ARBITRUM
    items_to_queue = create_items_to_feed_latest_multifeedistribution_snapshot(
        chain=chain, rewarder_type=rewarderType.RAMSES_v2
    )

    # add to queue
    task_enqueue_operations(
        operations=items_to_queue,
        network=chain.database_name,
        operation_type=queueItemType.LATEST_MULTIFEEDISTRIBUTION,
    )


def create_items_to_feed_latest_multifeedistribution_snapshot(
    chain: Chain, rewarder_type: rewarderType.RAMSES_v2
):
    """Create item list to feed latest multifeedistribution snapshot"""
    #
    result = []

    # TODO: solve the 'rewarder_type' manual reference to ramses_v2
    rewards_static = get_from_localdb(
        network=chain.database_name,
        collection="rewards_static",
        find={"rewarder_type": rewarder_type},
    )

    logging.getLogger(__name__).debug(
        f" building {len(rewards_static)} mfd item operations to add to queue"
    )

    # get addresses to scrape and its minimum block
    for reward in rewards_static:
        # always the same for snapshots
        result.append(
            {
                "transactionHash": "0x00000000",
                "blockHash": "",
                "blockNumber": 0,
                "address": reward["rewarder_registry"],
                "timestamp": "",
                "user": "",
                "reward_token": "",
                "topic": "snapshot",
                "logIndex": 0,
                "is_last_item": False,
            }
        )

    return result
