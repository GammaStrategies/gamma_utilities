from datetime import datetime, timezone
import logging
from apps.feeds.queue.queue_item import QueueItem
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_from_localdb

from bins.general import file_utilities, general_utilities
from bins.general.enums import Chain, queueItemType


def analyze_queues(chains: list[Chain] | None = None):
    """Analize queues

    Args:
        chains (list[Chain], optional): list of chains to analyze. Defaults to None.
    """
    # get chains
    chains = chains or list(Chain)

    # get list of failing queue items
    for chain in chains:
        # load previous results
        folder_path = CONFIGURATION.get("cache", {}).get("save_path", "data/cache")
        previous_results = file_utilities.load_json(
            filename=f"{chain.database_name}_queue_analysis.json",
            folder_path=folder_path,
        )

        # create queue summary from database info
        queue_summary = {
            "timestamp": datetime.now(timezone.utc).timestamp(),
            "types": [],
        }
        query = [
            {
                "$group": {
                    "_id": {"type": "$type", "count": "$count"},
                    "qtty": {"$sum": 1},
                }
            },
            {"$project": {"type": "$_id.type", "count": "$_id.count", "qtty": "$qtty"}},
            {"$sort": {"type": 1, "count": 1, "qtty": 1}},
            {"$project": {"_id": 0}},
        ]
        for item in get_from_localdb(
            network=chain.database_name, collection="queue", aggregate=query
        ):
            queue_summary["types"].append(item)

        # construct result
        pintable_format = {}
        for item in queue_summary["types"]:
            # defne pintable key
            if item["count"] > 0:
                key_name = f"{item['type']}>0"
            else:
                key_name = f"{item['type']}"

            # set key value
            if not key_name in pintable_format:
                pintable_format[key_name] = item["qtty"]
            else:
                pintable_format[key_name] += item["qtty"]

        # log result
        logging.getLogger(__name__).info(f"{chain.fantasy_name} queue content:")
        for key, value in pintable_format.items():
            logging.getLogger(__name__).info(f"     {key:30s}: {value:4.1f}")

        # compare with previous results, if any
        if previous_results:
            seconds_passed = queue_summary["timestamp"] - previous_results["timestamp"]

            # create comparison printable version
            printable_comparison = {}
            for item in previous_results["types"]:
                # defne pintable key
                if item["count"] > 0:
                    key_name = f"{item['type']}>0"
                else:
                    key_name = f"{item['type']}"

                # set key value
                if not key_name in printable_comparison:
                    printable_comparison[key_name] = item["qtty"]
                else:
                    printable_comparison[key_name] += item["qtty"]

            # add keys not present in both printable reports
            for k, v in pintable_format.items():
                if k not in printable_comparison:
                    printable_comparison[k] = 0
            for k, v in printable_comparison.items():
                if k not in pintable_format:
                    pintable_format[k] = 0

            # log differences
            logging.getLogger(__name__).info(
                f"  compared with previous results {general_utilities.log_time_passed.get_timepassed_string(start_time=datetime.fromtimestamp(previous_results['timestamp']), end_time=datetime.fromtimestamp(queue_summary['timestamp']))} before:"
            )
            for key, value in pintable_format.items():
                if key in printable_comparison:
                    _calculated_value = value - printable_comparison[key]
                else:
                    _calculated_value = value

                logging.getLogger(__name__).info(
                    f"     {key:30s}: {_calculated_value:4.1f}  [{(_calculated_value/seconds_passed)*60*60*24:,.1f} items/day]"
                )

        logging.getLogger(__name__).debug(f" Saving results to file")
        # save result to file
        file_utilities.save_json(
            filename=f"{chain.database_name}_queue_analysis.json",
            folder_path=folder_path,
            data=queue_summary,
        )


def get_list_failing_queue_items(chain: Chain, find: dict | None = None):
    """Get a detailed list of failing queue items for the specified network"""

    result = {}

    for queue_item_db in get_from_localdb(
        network=chain.database_name,
        collection="queue",
        find=find or {"count": {"$gt": 8}},
    ):
        # transform
        queue_item = QueueItem(**queue_item_db)

        # prepare result
        if queue_item.type not in result:
            result[queue_item.type] = []

        # check type
        if queue_item.type == queueItemType.LATEST_MULTIFEEDISTRIBUTION:
            rewards_static = get_from_localdb(
                network=chain.database_name,
                collection="rewards_static",
                find={"rewarder_registry": queue_item.address},
            )
            if not rewards_static:
                raise Exception(
                    f"Can't find rewards_static using rewarder_registry {queue_item.address}"
                )

            hypervisor_static = get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"address": rewards_static[0]["hypervisor_address"]},
            )[0]

            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": hypervisor_static["symbol"],
                    "dex": hypervisor_static["dex"],
                    "hypervisor_address": hypervisor_static["address"],
                    "rewards_static": [
                        {"token": x["rewardToken_symbol"], "address": x["rewardToken"]}
                        for x in rewards_static
                    ]
                    if rewards_static
                    else [],
                    "block": queue_item.block,
                }
            )

        elif queue_item.type == queueItemType.PRICE:
            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": "",
                    "hypervisor_address": "",
                    "rewards_static": [],
                    "block": queue_item.block,
                }
            )
        elif queue_item.type == queueItemType.REWARD_STATUS:
            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": queue_item.data["hypervisor_status"]["symbol"],
                    "hypervisor_address": queue_item.data["hypervisor_status"][
                        "address"
                    ],
                    "dex": queue_item.data["hypervisor_status"]["dex"],
                    "rewards_static": [
                        {
                            "token": queue_item.data.get("reward_static", {}).get(
                                "rewardToken_symbol", ""
                            ),
                            "address": queue_item.data.get("reward_static", {}).get(
                                "rewardToken", ""
                            ),
                        }
                    ],
                    "block": queue_item.block,
                }
            )
        elif queue_item.type == queueItemType.HYPERVISOR_STATUS:
            hypervisor_static = get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"address": queue_item.address},
            )[0]
            result[queue_item.type].append(
                {
                    "chain": chain.fantasy_name,
                    "type": queue_item.type,
                    "address": queue_item.address,
                    "hypervisor_symbol": hypervisor_static["symbol"],
                    "hypervisor_address": queue_item.address,
                    "dex": hypervisor_static["dex"],
                    "rewards_static": [],
                    "block": queue_item.block,
                }
            )

    return result
