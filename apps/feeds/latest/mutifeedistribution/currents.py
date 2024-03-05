# Feed of latest block data

import logging
from apps.feeds.operations import task_enqueue_operations
from bins.configuration import CONFIGURATION

from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, queueItemType, rewarderType
from bins.w3.protocols.ramses.collectors import (
    create_multiFeeDistribution_data_collector,
)


# PULL TO QUEUE
def feed_latest_multifeedistribution_snapshot():
    """add a multifeedistribution snapshot to the queue"""

    logging.getLogger(__name__).info(
        f" Feeding the latest multifeedistribution queue items"
    )
    # TODO: do not hardcode but include into config--
    #  define chains + protocols + rewarder types to be processed in this function
    items_to_process = [
        (Chain.ARBITRUM, Protocol.RAMSES, rewarderType.RAMSES_v2),
        (Chain.AVALANCHE, Protocol.PHARAOH, rewarderType.PHARAOH),
        (Chain.MANTLE, Protocol.CLEOPATRA, rewarderType.CLEOPATRA),
    ]

    for chain, protocol, rewarder_type in items_to_process:
        items_to_queue = create_items_to_feed_latest_multifeedistribution_snapshot(
            chain=chain, rewarder_type=rewarder_type, protocol=protocol
        )

        # only add non existant items to the queue
        addresses_already_queued = [
            x["address"]
            for x in get_from_localdb(
                network=chain.database_name,
                collection="queue",
                find={"type": queueItemType.LATEST_MULTIFEEDISTRIBUTION},
            )
        ]
        if items_to_queue := [
            x for x in items_to_queue if x["address"] not in addresses_already_queued
        ]:
            # add to queue
            task_enqueue_operations(
                operations=items_to_queue,
                network=chain.database_name,
                operation_type=queueItemType.LATEST_MULTIFEEDISTRIBUTION,
            )


def create_items_to_feed_latest_multifeedistribution_snapshot(
    chain: Chain, rewarder_type: rewarderType, protocol: Protocol
):
    """Create item list to feed latest multifeedistribution snapshot"""
    #
    result = []

    rewards_static = get_from_localdb(
        network=chain.database_name,
        collection="rewards_static",
        find={"rewarder_type": rewarder_type},
    )

    hypes_not_included = (
        CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get("gamma", {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(chain.database_name, [])
    )

    logging.getLogger(__name__).debug(
        f" building {len(rewards_static)} mfd item operations to add to queue"
    )

    # get addresses to scrape and its minimum block
    for reward in rewards_static:

        if reward["hypervisor_address"] in hypes_not_included:
            # skip hypervisor
            continue

        # always the same for snapshots
        result.append(
            {
                "transactionHash": "0x00000000",
                "blockHash": "",
                "blockNumber": 0,
                "address": reward["rewarder_registry"],
                "timestamp": "",
                "user": "",
                "reward_token": reward["rewardToken"],
                "topic": "snapshot",
                "logIndex": 0,
                "protocol": protocol.database_name,
                "is_last_item": False,
            }
        )

    return result
