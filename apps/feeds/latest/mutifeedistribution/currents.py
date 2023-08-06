# Feed of latest block data

import tqdm
from dataclasses import dataclass, asdict
from apps.feeds.operations import task_enqueue_operations

from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, queueItemType
from bins.w3.builders import build_hypervisor
from bins.w3.protocols.ramses.collectors import (
    create_multiFeeDistribution_data_collector,
)


@dataclass
class multifeeDistribution_snapshot:
    id: str = None
    block: int = None
    timestamp: int = None
    address: str = None
    dex: str = None
    hypervisor_address: str = None
    rewardToken: str = None
    rewardToken_decimals: int = None
    topic: str = None
    total_staked: int = None
    rewards: dict = None
    last_updated_data: dict = None

    def as_dict(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        # TODO: replace manual return with gettattr + check or similar
        result = {}

        if self.block:
            result["block"] = self.block
        if self.timestamp:
            result["timestamp"] = self.timestamp
        if self.address:
            result["address"] = self.address
        if self.dex:
            result["dex"] = self.dex
        if self.hypervisor_address:
            result["hypervisor_address"] = self.hypervisor_address
        if self.rewardToken:
            result["rewardToken"] = self.rewardToken
        if self.rewardToken_decimals:
            result["rewardToken_decimals"] = self.rewardToken_decimals
        if self.topic:
            result["topic"] = self.topic
        if self.total_staked:
            result["total_staked"] = self.total_staked
        if self.rewards:
            result["rewards"] = self.rewards
        if self.last_updated_data:
            result["last_updated_data"] = self.last_updated_data
        return result


def feed_latest_multifeedistribution_snapshot():
    """add a multifeedistribution snapshot to the queue to be processed asap

    Args:
        chain (Chain):
    """
    # TODO: solve manual chains reference
    chains = [Chain.ARBITRUM]
    for chain in chains:
        # TODO: solve the 'rewarder_type' manual reference to ramses_v2
        #
        # get addresses to scrape and its minimum block
        for reward in get_from_localdb(
            network=chain.database_name,
            collection="rewards_static",
            find={"rewarder_type": "ramses_v2"},
        ):
            # always the same for snapshots
            queue_item = {
                "transactionHash": "0x00000000",
                "blockHash": "",
                "blockNumber": 0,
                "address": reward["rewarder_registry"],
                "timestamp": "",
                "user": "",
                "reward_token": "",
                "topic": "snapshot",
                "logIndex": 0,
            }
            # add to queue
            task_enqueue_operations(
                operations=[queue_item],
                network=chain.database_name,
                operation_type=queueItemType.LATEST_MULTIFEEDISTRIBUTION,
            )


# def latest_mutiFeeDistribution_snapshot(
#     chain: Chain, protocol: Protocol, hypervisor_addresses: list[str]
# ):
#     """Get the latest multiFeeDistribution snapshot for a list of addresses and retrieve the last operations those addresses had
#     on the multiFeeDistribution contract.

#     Args:
#         addresses (list[str]): List of multi fee distributor addresses to get the latest snapshot for.
#     """

#     classified_addresses = {}
#     # get the latest snapshot data for those same addresses
#     for snapshot_in_database in get_from_localdb(
#         network=chain.database_name,
#         collection="latest_multifeedistribution",
#         find={},
#         sort=[("block", 1)],
#     ):
#         # if address not in classified addresses, add it
#         if snapshot_in_database["address"] not in classified_addresses:
#             classified_addresses[snapshot_in_database["address"]] = []
#         # add to classified addresses
#         classified_addresses[snapshot_in_database["address"]].append(
#             snapshot_in_database
#         )

#     # get mfd snapshots
#     for address in hypervisor_addresses:
#         # build hypervisor with private rpc
#         if hypervisor := build_hypervisor(
#             network=chain.database_name,
#             protocol=protocol,
#             block=0,
#             hypervisor_address=address,
#             cached=True,
#         ):
#             # set custom rpc type
#             hypervisor.custom_rpcType = "private"

#             mfd_item = multifeeDistribution_snapshot(
#                 block=hypervisor.block,
#                 timestamp=hypervisor._timestamp,
#                 dex=hypervisor.identify_dex_name(),
#                 hypervisor_address=address,
#             )

#             mfd_item.address = hypervisor.receiver.address

#             for reward_token in hypervisor.gauge.getRewardTokens:
#                 mfd_item.rewardToken = reward_token["token"]
#                 mfd_item.rewardToken_decimals = reward_token["decimals"]

#             # calculate current real rewards
#             # logging.getLogger(__name__).debug(f" Calculating rewards... ")
#             ephemeral_cache["hypervisor_rewards"][
#                 mfd_status["hypervisor_address"]
#             ] = hypervisor.calculate_rewards(
#                 period=hypervisor.current_period,
#                 reward_token=reward_static["rewardToken"],
#             )

#             # get current total staked qtty from multifeedistributor contract
#             ephemeral_cache["mfd_total_staked"][
#                 mfd_status["hypervisor_address"]
#             ] = hypervisor.receiver.totalStakes

#     # get the latest events for the mfd contracts

#     # create collector
#     # TODO: change to global mfd data collector and not Ramses one
#     data_collector = create_multiFeeDistribution_data_collector(
#         network=chain.database_name
#     )

#     with tqdm.tqdm(total=100) as progress_bar:
#         # create callback progress funtion
#         def _update_progress(text=None, remaining=None, total=None):
#             # set text
#             if text:
#                 progress_bar.set_description(text)
#             # set total
#             if total:
#                 progress_bar.total = total
#             # update current
#             if remaining:
#                 progress_bar.update(((total - remaining) - progress_bar.n))
#             else:
#                 progress_bar.update(1)
#             # refresh
#             progress_bar.refresh()

#         # set progress callback to data collector
#         data_collector.progress_callback = _update_progress

#         for operations in data_collector.operations_generator(
#             block_ini=block_ini,
#             block_end=block_end,
#             contracts=[Web3.toChecksumAddress(x) for x in addresses],
#             max_blocks=5000,
#         ):
#             # add operation to result
#             task_enqueue_operations(
#                 operations=operations,
#                 network=chain.database_name,
#                 operation_type=queueItemType.MULTIFEEDISTRIBUTION_STATUS,
#             )


# def po(chain: Chain, protocol: Protocol):
#     """Get the latest multiFeeDistribution snapshot for all ramses hypes"""

#     # TODO: change get all RAMSES hypervisors
#     for hypervisor_static in get_from_localdb(
#         network=chain.database_name,
#         collection="static",
#         aggregate=[
#             {
#                 "$match": {
#                     "dex": protocol.database_name,
#                 }
#             },
#             # // find hype's reward status
#             {
#                 "$lookup": {
#                     "from": "rewards_static",
#                     "let": {"op_address": "$address"},
#                     "pipeline": [
#                         {
#                             "$match": {
#                                 "$expr": {
#                                     "$eq": ["$hypervisor_address", "$$op_address"]
#                                 }
#                             }
#                         },
#                     ],
#                     "as": "rewards_static",
#                 }
#             },
#         ],
#         batch_size=100000,
#     ):
#         # build hypervisor with private rpc
#         if hypervisor := build_hypervisor(
#             network=chain.database_name,
#             protocol=protocol,
#             block=0,
#             hypervisor_address=hypervisor_static["address"],
#             cached=False,
#         ):
#             # set custom rpc type
#             hypervisor.custom_rpcType = "private"

#             for reward_static in hypervisor_static["rewards_static"]:
#                 # build mfd item
#                 mfd_item = multifeeDistribution_snapshot(
#                     block=hypervisor.block,
#                     timestamp=hypervisor._timestamp,
#                     address=hypervisor.receiver.address,
#                     dex=protocol.database_name,
#                     hypervisor_address=hypervisor_static["address"],
#                     rewardToken=reward_static["rewardToken"],
#                     rewardToken_decimals=reward_static["rewardToken_decimals"],
#                     total_staked=hypervisor.receiver.totalStakes,
#                     rewards=hypervisor.calculate_rewards(
#                         period=hypervisor.current_period,
#                         reward_token=reward_static["rewardToken"],
#                     ),
#                 )

#                 # add to result


# def get_latest_mfd_items(chain: Chain) -> list:
#     # get the latest snapshot data for those same addresses
#     for snapshot_in_database in get_from_localdb(
#         network=chain.database_name,
#         collection="latest_multifeedistribution",
#         find={},
#         sort=[("block", 1)],
#     ):
#         # if address not in classified addresses, add it
#         if snapshot_in_database["address"] not in classified_addresses:
#             classified_addresses[snapshot_in_database["address"]] = []
#         # add to classified addresses
#         classified_addresses[snapshot_in_database["address"]].append(
#             snapshot_in_database
#         )
