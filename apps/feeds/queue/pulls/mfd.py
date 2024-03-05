# ORIGINAL
import logging
from apps.errors.actions import process_error
from apps.feeds.latest.mutifeedistribution.item import multifeeDistribution_snapshot
from apps.feeds.queue.queue_item import QueueItem
from bins.database.common.database_ids import create_id_latest_multifeedistributor
from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
    get_latest_price_from_db,
    get_latest_prices_from_db,
)

from bins.errors.general import ProcessingError
from bins.general.enums import Chain, Protocol, text_to_chain, text_to_protocol
from bins.w3.builders import build_hypervisor


# multiFeeDistribution
def pull_from_queue_latest_multiFeeDistribution(
    network: str, queue_item: QueueItem
) -> bool:
    # build a list of itmes to be saved to the database
    if save_todb := build_multiFeeDistribution_from_queueItem(
        network=network, queue_item=queue_item
    ):
        # save to latest_multifeedistribution collection database
        if db_return := get_default_localdb(network=network).replace_items_to_database(
            data=save_todb, collection_name="latest_multifeedistribution"
        ):
            logging.getLogger(__name__).debug(
                f"       db return-> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
            )
        else:
            raise ValueError(" Database has not returned anything on writeBulk")

        # log
        logging.getLogger(__name__).debug(
            f"  <- Done processing {network}'s {queue_item.type} {queue_item.id}"
        )

        return True

    # return result
    return False


def build_multiFeeDistribution_from_queueItem(
    network: str, queue_item: QueueItem
) -> list[dict]:
    # build a list of itmes to be saved to the database
    save_todb = []

    try:
        # log operation processing
        logging.getLogger(__name__).debug(
            f"  -> Processing queue's {network} {queue_item.type} {queue_item.id}"
        )

        _prices = get_latest_prices_from_db(network=network)

        # get rewards related to this mfd
        rewards_related_info = get_rewarders_by_mfd(
            chain=text_to_chain(network),
            rewarder_registry=queue_item.data["address"].lower(),
            rewardToken=queue_item.data["reward_token"].lower(),
            block=queue_item.block,
        )

        # set common vars to be used multiple times
        mfd_address = queue_item.data["address"].lower()
        protocol = text_to_protocol(
            queue_item.data.get("protocol", Protocol.RAMSES.database_name)
        )

        # process mfd rewards ( only one by default )
        if len(rewards_related_info) > 1:
            raise ValueError(
                f" More than one rewarder information found for {mfd_address}. Cant continue"
            )

        # if no rewards status are found, skip it
        if not rewards_related_info or rewards_related_info[0]["rewards_status"] == []:
            logging.getLogger(__name__).warning(
                f"  no rewards status found for queue's {network} {queue_item.type} {queue_item.id} [{rewards_related_info[0]['rewardToken_symbol']}]"
            )
            # remove item from queue if count > 5
            # this item will never be pushed again by the last data script unless it diapear from the queue. So it will be processed again
            if queue_item.count >= 6:
                logging.getLogger(__name__).warning(
                    f"  queue's {network} {queue_item.type} {queue_item.id} has been processed 5 or more times. Removing from queue"
                )
                get_default_localdb(network=network).del_queue_item(id=queue_item.id)
            # exit empty handed
            return []

        reward_static = rewards_related_info[0]

        # set hypervisor address ( easy to access var)
        hypervisor_address = reward_static["hypervisor_address"]

        # build hypervisor at block
        hypervisor_object = build_hypervisor(
            network=network,
            protocol=protocol,
            block=queue_item.block,
            hypervisor_address=hypervisor_address,
            cached=False,
            multicall=True,
        )

        # set custom rpc type
        hypervisor_object.custom_rpcType = "private"

        # fill with multicall
        hypervisor_object.fill_with_multicall(
            pool_address=reward_static["rewards_status"][0]["hypervisor_status"][
                "pool"
            ]["address"],
            token0_address=reward_static["rewards_status"][0]["hypervisor_status"][
                "pool"
            ]["token0"]["address"],
            token1_address=reward_static["rewards_status"][0]["hypervisor_status"][
                "pool"
            ]["token1"]["address"],
        )

        # build a database version of the hypervisor (standard )
        hypervisor_db = hypervisor_object.as_dict(
            convert_bint=True, static_mode=False, minimal=False
        )

        # continuity check: the hypervisor has a position and supply at current state
        if not _check_hypervisor_to_continue(hypervisor_status=hypervisor_db):
            logging.getLogger(__name__).warning(
                f"  hypervisor {hypervisor_address} {hypervisor_object.symbol} at block {hypervisor_object.block} has no position or supply. Skipping"
            )
            return []

        # MDF period hypervisor_object.current_period

        # create multiFeeDistributior snapshot structure (to be saved to database later)
        snapshot = multifeeDistribution_snapshot(
            block=hypervisor_object.block,
            timestamp=hypervisor_object._timestamp,
            address=mfd_address,
            hypervisor_address=hypervisor_address,
            dex=hypervisor_object.identify_dex_name(),
            rewardToken=reward_static["rewardToken"],
            rewardToken_decimals=reward_static["rewardToken_decimals"],
            rewardToken_symbol=reward_static["rewardToken_symbol"],
        )
        # set staked qtty
        if (
            hypervisor_object.receiver.totalStakes == 0
            or hypervisor_object.receiver.totalStakes > hypervisor_object.totalSupply
        ):
            logging.getLogger(__name__).warning(
                f" Setting staked qtty to hypervisors total bc MFD reported {hypervisor_object.receiver.totalStakes} at block {hypervisor_object.block}"
            )
            snapshot.hypervisor_staked = str(hypervisor_object.totalSupply)
        else:
            snapshot.hypervisor_staked = str(hypervisor_object.receiver.totalStakes)

        # set hypervisor share price
        snapshot.hypervisor_share_price_usd = _calculate_hypervisor_price_per_share(
            _prices, hypervisor_db
        )

        # use rewardData
        if rewardData := hypervisor_object.receiver.rewardData(
            rewardToken_address=reward_static["rewardToken"]
        ):
            if rewardData["lastTimeUpdated"] == 0:
                # no rewards?
                logging.getLogger(__name__).warning(
                    f"  lastTimeUpdated is zero for queue's {network} {queue_item.type} {queue_item.id}  -> hypervisor {hypervisor_address} block {hypervisor_object.block}. Initial timestamp calculation for rewards pending will be 2 weeks ago"
                )
                rewardData["lastTimeUpdated"] = snapshot.timestamp - (
                    60 * 60 * 24 * 7 * 2
                )

            # set sumUP vars
            boostedRewards_sinceLastUpdateTime = 0
            baseRewards_sinceLastUpdateTime = 0

            reward_items_periods = []
            # periods from lastUpdateStatus to snapshot
            for item in build_periods_timestamps(
                ini_timestamp=rewardData["lastTimeUpdated"],
                end_timestamp=snapshot.timestamp,
            ):
                # get rewards claimed, base, boosted from past positions within the period
                claimed, base, boosted = get_claimed_data_between_changed_positions(
                    network=network,
                    protocol=protocol,
                    reward_status_list=reward_static["rewards_status"],
                    hypervisor_status=hypervisor_db,
                    hypervisor_address=hypervisor_address,
                    period=item["period"],
                    rewardToken=reward_static["rewardToken"],
                )

                # add current position claimed rewards
                claimed += hypervisor_object.get_already_claimedRewards(
                    period=item["period"],
                    reward_token=reward_static["rewardToken"],
                )

                #  get the rewards per second from the last period
                try:
                    _this_period_rewards_rate = hypervisor_object.calculate_rewards(
                        period=item["period"],
                        reward_token=reward_static["rewardToken"],
                        convert_bint=True,
                    )
                except Exception as e:
                    logging.getLogger(__name__).error(
                        f" Cant calculate latest rewards for hype {hypervisor_address}  rewardToken {reward_static['rewardToken']} -> {e}"
                    )
                    continue

                # total mixed rewards
                item["rewardsSinceLastUpdateTime"] = (
                    float(_this_period_rewards_rate["current_baseRewards"])
                    + boosted
                    + base
                    + float(_this_period_rewards_rate["current_boostedRewards"])
                    - claimed
                )

                # check for negative and zero em
                if item["rewardsSinceLastUpdateTime"] < 0:
                    logging.getLogger(__name__).warning(
                        f" claimed rewards [{claimed}] are bigger than calculated rewards for the period... zeroed rewardsSinceLastUpdateTime"
                    )
                    item["rewardsSinceLastUpdateTime"] = 0

                # unmix proportionally
                total_period_rewards = float(
                    _this_period_rewards_rate["current_baseRewards"]
                ) + float(_this_period_rewards_rate["current_boostedRewards"])
                item["baseRewardsSinceLastUpdateTime"] = (
                    (
                        float(_this_period_rewards_rate["current_baseRewards"])
                        / total_period_rewards
                    )
                    if total_period_rewards
                    else 0
                ) * item["rewardsSinceLastUpdateTime"]
                item["boostedRewardsSinceLastUpdateTime"] = (
                    (
                        float(_this_period_rewards_rate["current_boostedRewards"])
                        / total_period_rewards
                    )
                    if total_period_rewards
                    else 0
                ) * item["rewardsSinceLastUpdateTime"]

                # append result item
                reward_items_periods.append(item)

                # sum up snapshot vars
                baseRewards_sinceLastUpdateTime += item[
                    "baseRewardsSinceLastUpdateTime"
                ]
                boostedRewards_sinceLastUpdateTime += item[
                    "boostedRewardsSinceLastUpdateTime"
                ]

            snapshot.baseRewards_sinceLastUpdateTime = str(
                baseRewards_sinceLastUpdateTime
            )
            snapshot.boostedRewards_sinceLastUpdateTime = str(
                boostedRewards_sinceLastUpdateTime
            )
            snapshot.seconds_sinceLastUpdateTime = (
                snapshot.timestamp - rewardData["lastTimeUpdated"]
            )

            logging.getLogger(__name__).debug(
                f"   total rewards of {network} {queue_item.type} {queue_item.id}   base:{snapshot.baseRewards_sinceLastUpdateTime} boosted:{snapshot.boostedRewards_sinceLastUpdateTime}   seconds:{snapshot.seconds_sinceLastUpdateTime}"
            )

        if snapshot.seconds_sinceLastUpdateTime < 0:
            logging.getLogger(__name__).error(
                f" seconds_sinceLastUpdateTime is negative {snapshot.seconds_sinceLastUpdateTime} for {network} {queue_item.type} {queue_item.id}"
            )
            # info may be sourced from different block numbers
            return []

        # get reward token price
        snapshot.rewardToken_price = _prices.get(reward_static["rewardToken"], None)
        if not snapshot.rewardToken_price:
            logging.getLogger(__name__).error(
                f" Cant get price for {reward_static['rewardToken']} at block {hypervisor_object.block}. Using last known price from block {reward_static['rewards_status'][0]['block']} -> {reward_static['rewards_status'][0]['rewardToken_price_usd']}"
            )
            snapshot.rewardToken_price = reward_static["rewards_status"][0][
                "rewardToken_price_usd"
            ]

        if not snapshot.hypervisor_share_price_usd:
            # we could use as emergency ->  reward_static["rewards_status"][0]["hypervisor_share_price_usd"] but
            logging.getLogger(__name__).error(
                f" Cant get share price for {hypervisor_address} at block {hypervisor_object.block}."
            )

        # TODO: use hype decimals from db
        staked_usd = snapshot.hypervisor_share_price_usd * (
            float(snapshot.hypervisor_staked) / 10**18
        )

        try:
            snapshot.apr_baseRewards = (
                (
                    (
                        (
                            float(snapshot.baseRewards_sinceLastUpdateTime)
                            / 10 ** reward_static["rewardToken_decimals"]
                        )
                        / snapshot.seconds_sinceLastUpdateTime
                    )
                    * 60
                    * 60
                    * 24
                    * 365
                )
                * snapshot.rewardToken_price
            ) / staked_usd
        except ZeroDivisionError as e:
            # staked usd is zero
            logging.getLogger(__name__).error(
                f" There is no staked nor TVL in this hype right now for {network} {queue_item.type} {queue_item.id}"
            )
            snapshot.apr_baseRewards = 0

        except Exception as e:
            if staked_usd:
                logging.getLogger(__name__).warning(
                    f" No staked hype {hypervisor_address} usd value found for {network} {queue_item.type} {queue_item.id}"
                )

            snapshot.apr_baseRewards = 0

        try:
            snapshot.apr_boostedRewards = (
                (
                    (
                        (
                            float(snapshot.boostedRewards_sinceLastUpdateTime)
                            / 10 ** reward_static["rewardToken_decimals"]
                        )
                        / snapshot.seconds_sinceLastUpdateTime
                    )
                    * 60
                    * 60
                    * 24
                    * 365
                )
                * snapshot.rewardToken_price
            ) / staked_usd
        except Exception as e:
            logging.getLogger(__name__).error(
                f" No rewards apr for {network} {queue_item.type} {queue_item.id} {e}"
            )
            snapshot.apr_boostedRewards = 0

        snapshot.apr = snapshot.apr_baseRewards + snapshot.apr_boostedRewards

        # set id
        snapshot.id = create_id_latest_multifeedistributor(
            mfd_address=snapshot.address,
            rewardToken_address=snapshot.rewardToken,
            hypervisor_address=snapshot.hypervisor_address,
        )

        # set item to save
        item_to_save = snapshot.as_dict()

        # add item to be saved
        save_todb.append(item_to_save)

    except ProcessingError as e:
        logging.getLogger(__name__).error(f"{e.message}")
        # process error
        process_error(e)

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s latest_multifeedistribution queue item: {e}"
        )

    # return result
    return save_todb


### HELPERS


def build_periods_timestamps(ini_timestamp: int, end_timestamp: int) -> list:
    """Build a list of periods and timestamps for a given timeframe

    Args:
        ini_timestamp (int):
        end_timestamp (int):

    Returns:
        list: { "period": <number>,
                "from_timestamp": ,
                "to_timestamp"
                total_seconds:
                }
    """
    week_in_seconds = 60 * 60 * 24 * 7
    items_periods = []

    initial_period = ini_timestamp // week_in_seconds
    end_period = end_timestamp // week_in_seconds

    current_timestamp = ini_timestamp
    for i in range(initial_period, end_period + 1):
        if current_timestamp >= end_timestamp:
            logging.getLogger(__name__).error(f" -> current_timestamp >= end_timestamp")
            break

        item_to_add = {
            "period": i,
            "from_timestamp": current_timestamp,
            "to_timestamp": min((i + 1) * week_in_seconds - 1, end_timestamp),
        }
        # define total seconds ( visual debugging)
        item_to_add["total_seconds"] = (
            item_to_add["to_timestamp"] - item_to_add["from_timestamp"]
        )
        # add to list
        items_periods.append(item_to_add)
        # set next timestamp
        current_timestamp = min((i + 1) * week_in_seconds, end_timestamp)

    return items_periods


# MFD HELPERS
def get_rewarders_by_mfd(
    chain: Chain, rewarder_registry: str, rewardToken: str, block: int | None = None
) -> list[dict]:
    """ONE REWARDER PER REWARD TOKEN ( only one item will be returned)

    Args:
        chain (Chain): _description_
        rewarder_registry (str): _description_
        rewardToken (str): _description_
        block (int | None, optional): _description_. Defaults to None.

    Returns:
        list[dict]: _description_
    """
    # setup data filtering. When block is zero, get all available data
    _and_filter = [
        {
            "$eq": [
                "$hypervisor_address",
                "$$op_address",
            ],
        },
        {
            "$eq": [
                "$rewardToken",
                "$$op_rewardToken",
            ],
        },
    ]

    if block:
        _and_filter.append({"$lte": ["$block", block]})

    query = [
        {
            "$match": {
                "rewarder_registry": rewarder_registry.lower(),
                "rewardToken": rewardToken.lower(),
            }
        },
        # find hype's reward status
        {
            "$lookup": {
                "from": "rewards_status",
                "let": {
                    "op_address": "$hypervisor_address",
                    "op_rewardToken": "$rewardToken",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": _and_filter,
                            }
                        }
                    },
                    {"$sort": {"block": -1}},
                    {"$limit": 5},
                    # get hypervisor status
                    {
                        "$lookup": {
                            "from": "status",
                            "let": {
                                "r_address": "$hypervisor_address",
                                "r_block": "$block",
                            },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                {
                                                    "$eq": [
                                                        "$address",
                                                        "$$r_address",
                                                    ]
                                                },
                                                {"$eq": ["$block", "$$r_block"]},
                                            ]
                                        }
                                    }
                                },
                                {"$limit": 1},
                            ],
                            "as": "hypervisor_status",
                        }
                    },
                    {"$unwind": "$hypervisor_status"},
                ],
                "as": "rewards_status",
            }
        },
    ]

    # get rewards related to this mfd, (so that we know this hype's protocol)
    return get_from_localdb(
        network=chain.database_name,
        collection="rewards_static",
        aggregate=query,
        limit=1,
        batch_size=10000,
    )


def get_claimed_data_between_changed_positions(
    network: str,
    protocol: Protocol,
    reward_status_list: list[dict],
    hypervisor_status: dict,
    hypervisor_address: str,
    period: int,
    rewardToken: str,
) -> tuple[float, float, float]:

    # create a return value
    result = {
        "claimed": 0,
        "base": 0,
        "boosted": 0,
    }

    position_ids_already_processed = []
    for reward_status in reward_status_list:
        # check if position is different and has not been processed
        temp_position_id_base = f"{reward_status['hypervisor_status']['baseUpper']}_{reward_status['hypervisor_status']['baseLower']}"
        temp_position_id_limit = f"{reward_status['hypervisor_status']['limitUpper']}_{reward_status['hypervisor_status']['limitLower']}"
        # create a the original position ids
        original_position_id_base = (
            f"{hypervisor_status['baseUpper']}_{hypervisor_status['baseLower']}"
        )
        original_position_id_limit = (
            f"{hypervisor_status['limitUpper']}_{hypervisor_status['limitLower']}"
        )

        # create hype at block and get already claimed rewards for the position
        _tempHypervisor = build_hypervisor(
            network=network,
            protocol=protocol,
            block=reward_status["block"],
            hypervisor_address=hypervisor_address,
            cached=True,
        )

        # check if position is different and has not been processed
        if (
            temp_position_id_base != original_position_id_base
            and temp_position_id_base not in position_ids_already_processed
        ) and (
            temp_position_id_limit != original_position_id_limit
            and temp_position_id_limit not in position_ids_already_processed
        ):
            # CASE Base+Limit positions differ thus we need to get claimed rewards for all positions at the time.
            result["claimed"] += _tempHypervisor.get_already_claimedRewards(
                period=period, reward_token=rewardToken, position=None
            )

            # add base and boosted rewards
            result["base"] += float(reward_status["extra"]["baseRewards"])
            result["boosted"] += float(reward_status["extra"]["boostedRewards"])

            # add position id to already processed so that it is not processed again
            position_ids_already_processed.append(temp_position_id_base)
            position_ids_already_processed.append(temp_position_id_limit)

        elif (
            temp_position_id_base != original_position_id_base
            and temp_position_id_base not in position_ids_already_processed
        ):
            # CASE Base positions differ
            result["claimed"] += _tempHypervisor.get_already_claimedRewards(
                period=period, reward_token=rewardToken, position="base"
            )

            # add base and boosted rewards
            result["base"] += float(reward_status["extra"]["baseRewards"])
            result["boosted"] += float(reward_status["extra"]["boostedRewards"])

            # add position id to already processed so that it is not processed again
            position_ids_already_processed.append(temp_position_id_base)

        elif (
            temp_position_id_limit != original_position_id_limit
            and temp_position_id_limit not in position_ids_already_processed
        ):
            # CASE Limit positions differ
            result["claimed"] += _tempHypervisor.get_already_claimedRewards(
                period=period, reward_token=rewardToken, position="limit"
            )

            # add base and boosted rewards
            result["base"] += float(reward_status["extra"]["baseRewards"])
            result["boosted"] += float(reward_status["extra"]["boostedRewards"])

            # add position id to already processed so that it is not processed again
            position_ids_already_processed.append(temp_position_id_limit)

    # log formality
    logging.getLogger(__name__).debug(
        f" {hypervisor_address} at block {reward_status['block']} aggregated data-> claimed {result['claimed']} base {result['base']} boosted {result['boosted']}"
    )
    #
    return result["claimed"], result["base"], result["boosted"]


def calculate_hypervisor_staked_qtty(
    ephemeral_cache: dict, hypervisor_address: str, reward_static: dict
) -> int:
    """Decide which total staked supply to use as hypervisor staked qtty

    Args:
        ephemeral_cache (dict):
        hypervisor_address (str):
        reward_static (dict):
    """

    # 0) use total staked supply from the mdf contract
    result = int(ephemeral_cache["mfd_total_staked"][hypervisor_address])
    # share price must be the current one
    _token0, _token1 = _xtract_hypervisor_tokens(
        ephemeral_cache["hypervisor_status"][hypervisor_address]
    )
    _price0 = ephemeral_cache["prices"][
        ephemeral_cache["hypervisor_status"][hypervisor_address]["pool"]["token0"][
            "address"
        ]
    ]
    _price1 = ephemeral_cache["prices"][
        ephemeral_cache["hypervisor_status"][hypervisor_address]["pool"]["token1"][
            "address"
        ]
    ]

    _staked_supply = int(
        reward_static["rewards_status"][0]["hypervisor_status"]["totalSupply"]
    )
    if result == 0 or result > _staked_supply:
        logging.getLogger(__name__).warning(
            f" hypervisor staked changed from {result} to {_staked_supply} due to mdf staked being zero or higher than the hypervisor staked supply"
        )
        result = _staked_supply
        # share price must be the current block one
        _token0, _token1 = _xtract_hypervisor_tokens(
            ephemeral_cache["hypervisor_status"][hypervisor_address]
        )

    # set hypervisor price per share in cache var so that can be used later
    ephemeral_cache["hypervisor_share_price"] = (
        _token0 * _price0 + _token1 * _price1
    ) / (
        result
        / (10 ** ephemeral_cache["hypervisor_status"][hypervisor_address]["decimals"])
    )

    return result


def _check_hypervisor_to_continue(hypervisor_status: dict) -> bool:
    """Continuity check:
        Check if the hypervisor has a position and supply

    Args:
        hypervisor_status (dict): as db status

    Returns:
        bool: True if the hypervisor has a position and supply
    """
    if (
        int(hypervisor_status["baseUpper"]) == 0
        and int(hypervisor_status["baseLower"]) == 0
        and int(hypervisor_status["limitUpper"]) == 0
        and int(hypervisor_status["limitLower"]) == 0
    ):
        return False
    if int(hypervisor_status["totalSupply"]) == 0:
        return False

    return True


def _calculate_hypervisor_price_per_share(prices: dict, hypervisor_db: dict) -> float:
    _price0 = prices[hypervisor_db["pool"]["token0"]["address"]]
    _price1 = prices[hypervisor_db["pool"]["token1"]["address"]]
    _token0, _token1 = _xtract_hypervisor_tokens(hypervisor_db)

    try:
        return (_token0 * _price0 + _token1 * _price1) / (
            int(hypervisor_db["totalSupply"]) / (10 ** hypervisor_db["decimals"])
        )

    except ZeroDivisionError as e:
        logging.getLogger(__name__).error(
            f" hypervisor staked is zero {hypervisor_db['totalSupply']} {e}"
        )
        return 0


def _xtract_hypervisor_tokens(hypervisor_status: dict) -> tuple[float, float]:
    # share price must be the current one
    return int(hypervisor_status["totalAmounts"]["total0"]) / (
        10 ** hypervisor_status["pool"]["token0"]["decimals"]
    ), int(hypervisor_status["totalAmounts"]["total1"]) / (
        10 ** hypervisor_status["pool"]["token1"]["decimals"]
    )
