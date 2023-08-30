import logging
from apps.feeds.status.rewards.utils import (
    get_hypervisor_data_for_apr,
    get_reward_pool_prices,
)
from bins.database.helpers import get_default_localdb, get_from_localdb

from bins.general.enums import Chain, Protocol, rewarderType
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.ramses.hypervisor import gamma_hypervisor as ramses_hypervisor


def create_rewards_status_ramses(
    chain: Chain,
    rewarder_static: dict,
    hypervisor_status: dict,
    periods: int | None = None,
) -> list:
    """_summary_

    Args:
        chain (Chain):
        rewarder_static (dict):
        hypervisor_status (dict):
        periods (int | None, optional): how many ramses periods[WEEKs] to include data from. Defaults to 1 period (current).

    Returns:
        list:
    """
    batch_size = 50000

    result = []
    # create ramses hypervisor
    hype_status = ramses_hypervisor(
        address=hypervisor_status["address"],
        network=chain.database_name,
        block=hypervisor_status["block"],
    )

    hypervisor_totalSupply = hype_status.totalSupply

    # current staked
    totalStaked = hype_status.receiver.totalStakes

    if not hypervisor_totalSupply:
        logging.getLogger(__name__).debug(
            f"Can't calculate rewards status for ramses hype {hypervisor_status['symbol']} {hype_status.address} because it has no supply at block {hype_status.block}"
        )
        return []

    # period timeframe
    current_timestamp = hypervisor_status["timestamp"]
    current_period = hype_status.current_period
    # default to 1 period
    if not periods:
        periods = 1
    period_ini = current_period + (1 - abs(periods))
    period_ini_timestamp = period_ini * 60 * 60 * 24 * 7
    # end timestamp of the period
    # period_end_timestamp = ((current_period + 1) * 60 * 60 * 24 * 7) - 1

    # get hypervisor reward tokens from database or onchain
    reward_tokens = get_from_localdb(
        network=chain.database_name,
        collection="rewards_static",
        find={"hypervisor_address": hypervisor_status["address"]},
    )
    if not reward_tokens:
        # build rewards tokes with fields as if they were database entries
        reward_tokens = []
        for temp_reward_token_address in hype_status.gauge.getRewardTokens:
            # build erc20 helper
            erc20_helper = build_erc20_helper(
                chain=chain, address=temp_reward_token_address, cached=True
            )
            reward_tokens.append(
                {
                    "hypervisor_address": hypervisor_status["address"],
                    "rewardToken": temp_reward_token_address.lower(),
                    "rewardToken_symbol": erc20_helper.symbol,
                    "rewardToken_decimals": erc20_helper.decimals,
                }
            )

    # get hypervisor data for apr
    apr_ordered_hypervisor_status_db_list = get_hypervisor_data_for_apr(
        hypervisor_address=hypervisor_status["address"],
        timestamp_ini=period_ini_timestamp,
        timestamp_end=current_timestamp,
    )

    # Modify initial period in case no data is found for current period
    if not apr_ordered_hypervisor_status_db_list:
        logging.getLogger(__name__).warning(
            f" No sufficient hypervisor data found to calculate rewards apr for Ramses {hypervisor_status['address']} between {period_ini_timestamp} and {current_timestamp} on periods from {period_ini} to {current_period}. Modifying initial period to {period_ini-1}"
        )
        # reset period vars
        period_ini = period_ini - 1
        period_ini_timestamp = period_ini * 60 * 60 * 24 * 7
        # get hypervisor data for apr
        apr_ordered_hypervisor_status_db_list = get_hypervisor_data_for_apr(
            hypervisor_address=hypervisor_status["address"],
            timestamp_ini=period_ini_timestamp,
            timestamp_end=current_timestamp,
        )

    if apr_ordered_hypervisor_status_db_list:
        result = []
        # get reward tokens for this hypervisor
        for reward_token in reward_tokens:
            # create control vars
            last_item = None
            # specific data to save in order to calc apr
            items_to_calc_apr = []
            # totals to calc apr
            total_baseRewards = 0
            total_boostedRewards = 0
            total_time_passed = 0

            # loop thru the list of apr ordered hype status
            for _ordered_hype_status_db in apr_ordered_hypervisor_status_db_list:
                # add hype status if not already in the list
                ids = [x["id"] for x in _ordered_hype_status_db["status"]]
                if (
                    hypervisor_status["id"] not in ids
                    and hypervisor_status["block"]
                    >= _ordered_hype_status_db["status"][-1]["block"]
                ):
                    logging.getLogger(__name__).debug(f" adding hype status to list")
                    # add as last item
                    _ordered_hype_status_db["status"].append(hypervisor_status)

                # _ordered_hype_status_db["_id"] = hypervisor_address
                for idx, data in enumerate(_ordered_hype_status_db["status"]):
                    # zero and par indexes refer to initial values
                    if idx == 0 or idx % 2 == 0:
                        # this is an initial value.
                        # create ramses hypervisor
                        _temp_hype = ramses_hypervisor(
                            address=hypervisor_status["address"],
                            network=chain.database_name,
                            block=data["block"],
                        )
                        # add totalStaked to item to calc apr
                        data["totalStaked"] = _temp_hype.receiver.totalStakes

                    else:
                        # calculate time passed since last item
                        time_passed = data["timestamp"] - last_item["timestamp"]

                        # get real rewards for this period using the hype's position
                        real_rewards = create_rewards_status_ramses_get_real_rewards(
                            hypervisor_address=hypervisor_status["address"],
                            network=chain.database_name,
                            block=data["block"],
                            rewardToken_address=reward_token["rewardToken"],
                            time_passed=time_passed,
                        )

                        # add to items to calc apr: transform to float
                        items_to_calc_apr.append(
                            {
                                "base_rewards": real_rewards["base_rewards"]
                                / (10 ** reward_token["rewardToken_decimals"]),
                                "boosted_rewards": real_rewards["boosted_rewards"]
                                / (10 ** reward_token["rewardToken_decimals"]),
                                "time_passed": time_passed,
                                "timestamp_ini": last_item["timestamp"],
                                "timestamp_end": data["timestamp"],
                                "hypervisor": {
                                    "block": last_item["block"],
                                    "totalStaked": last_item["totalStaked"]
                                    / (10 ** last_item["decimals"]),
                                    "totalSupply": int(last_item["totalSupply"])
                                    / (10 ** last_item["decimals"]),
                                    "underlying_token0": (
                                        float(last_item["totalAmounts"]["total0"])
                                        + float(
                                            last_item["fees_uncollected"]["qtty_token0"]
                                        )
                                    )
                                    / (10 ** last_item["pool"]["token0"]["decimals"]),
                                    "underlying_token1": (
                                        float(last_item["totalAmounts"]["total1"])
                                        + float(
                                            last_item["fees_uncollected"]["qtty_token1"]
                                        )
                                    )
                                    / (10 ** last_item["pool"]["token1"]["decimals"]),
                                },
                            }
                        )

                        # add totals
                        total_baseRewards += real_rewards["base_rewards"]
                        total_boostedRewards += real_rewards["boosted_rewards"]
                        total_time_passed += time_passed

                    # set lastitem
                    last_item = data

            # calculate per second rewards
            baseRewards_per_second = (
                total_baseRewards / total_time_passed if total_time_passed else 0
            )
            boostedRewards_per_second = (
                total_boostedRewards / total_time_passed if total_time_passed else 0
            )
            totalRewards_per_second = baseRewards_per_second + boostedRewards_per_second

            if (
                not isinstance(totalRewards_per_second, int)
                and totalRewards_per_second.is_integer()
            ):
                totalRewards_per_second = int(totalRewards_per_second)

            # calculate apr
            if reward_apr_data_to_save := create_rewards_status_ramses_calculate_apr(
                hypervisor_address=hypervisor_status["address"],
                network=chain.database_name,
                block=hypervisor_status["block"],
                rewardToken_address=reward_token["rewardToken"],
                token0_address=hypervisor_status["pool"]["token0"]["address"],
                token1_address=hypervisor_status["pool"]["token1"]["address"],
                items_to_calc_apr=items_to_calc_apr,
            ):
                # build reward data
                reward_data = {
                    "apr": reward_apr_data_to_save["apr"],
                    "apy": reward_apr_data_to_save["apy"],
                    "block": hype_status.block,
                    "timestamp": hype_status._timestamp,
                    "hypervisor_address": hype_status.address.lower(),
                    "hypervisor_symbol": hypervisor_status["symbol"],
                    "dex": Protocol.RAMSES.database_name,
                    "rewarder_address": hype_status.gauge.address.lower(),
                    "rewarder_type": rewarderType.RAMSES_v2,
                    "rewarder_refIds": [],
                    "rewarder_registry": hype_status.receiver.address.lower(),
                    "rewardToken": reward_token["rewardToken"].lower(),
                    "rewardToken_symbol": reward_token["rewardToken_symbol"],
                    "rewardToken_decimals": reward_token["rewardToken_decimals"],
                    "rewardToken_price_usd": reward_apr_data_to_save[
                        "rewardToken_price_usd"
                    ],
                    "token0_price_usd": reward_apr_data_to_save["token0_price_usd"],
                    "token1_price_usd": reward_apr_data_to_save["token1_price_usd"],
                    "rewards_perSecond": str(totalRewards_per_second),
                    "total_hypervisorToken_qtty": str(totalStaked),
                    "hypervisor_share_price_usd": reward_apr_data_to_save[
                        "hypervisor_share_price_usd"
                    ],
                    # extra fields
                    "extra": {
                        "baseRewards": str(total_baseRewards),
                        "boostedRewards": str(total_boostedRewards),
                        "baseRewards_apr": reward_apr_data_to_save["extra"][
                            "baseRewards_apr"
                        ],
                        "baseRewards_apy": reward_apr_data_to_save["extra"][
                            "baseRewards_apy"
                        ],
                        "boostedRewards_apr": reward_apr_data_to_save["extra"][
                            "boostedRewards_apr"
                        ],
                        "boostedRewards_apy": reward_apr_data_to_save["extra"][
                            "boostedRewards_apy"
                        ],
                        "baseRewards_per_second": str(baseRewards_per_second),
                        "boostedRewards_per_second": str(boostedRewards_per_second),
                        # "raw_data": items_to_calc_apr,
                    },
                }
                result.append(reward_data)

    else:
        logging.getLogger(__name__).warning(
            f" No data found to calculate rewards apr for Ramses {hypervisor_status['address']} between {period_ini_timestamp} and {current_timestamp} on periods from {period_ini} to {current_period}. Can't continue"
        )

    # empty result means no rewards at this block
    if not result:
        logging.getLogger(__name__).debug(
            f" Ramses Rewards-> {chain.database_name}'s {rewarder_static.get('rewardToken','None')} has no rewards at block {hypervisor_status['block']}"
        )

    return result


def create_rewards_status_ramses_get_real_rewards(
    hypervisor_address: str,
    network: str,
    block: int,
    rewardToken_address: str,
    time_passed: int,
) -> dict:
    # get rewards for this period using the hype's position

    # create ramses hypervisor
    _temp_hype_status = ramses_hypervisor(
        address=hypervisor_address,
        network=network,
        block=block,
    )
    # calculate rewards for this position at period
    _temp_real_rewards = _temp_hype_status.calculate_rewards(
        period=_temp_hype_status.current_period, reward_token=rewardToken_address
    )
    # calculate rewards per second for this position and perood
    baseRewards_per_second = (
        _temp_real_rewards["current_baseRewards"]
        / _temp_real_rewards["current_period_seconds"]
    )
    boostRewards_per_second = (
        _temp_real_rewards["current_boostedRewards"]
        / _temp_real_rewards["current_period_seconds"]
    )

    # current staked
    totalStaked = _temp_hype_status.receiver.totalStakes

    return {
        "base_rewards": (baseRewards_per_second * time_passed),
        "boosted_rewards": (boostRewards_per_second * time_passed),
        "total_staked": totalStaked,
    }


def create_rewards_status_ramses_calculate_apr(
    hypervisor_address: str,
    network: str,
    block: int,
    rewardToken_address: str,
    token0_address: str,
    token1_address: str,
    items_to_calc_apr: list,
) -> dict:
    """_summary_

    Args:
        hypervisor_address (str):
        network (str):
        block (int):
        rewardToken_address (str):
        token0_address (str):
        token1_address (str):
        items_to_calc_apr (list): {
                            "base_rewards":
                            "boosted_rewards":
                            "time_passed":
                            "timestamp_ini":
                            "timestamp_end":
                            "hypervisor": {
                                "totalStaked
                                "totalSupply":
                                "underlying_token0":
                                "underlying_token1":
                            },
                        }

    Returns:
        dict:  {
            "apr": reward_apr,
            "apy": reward_apy,
            "rewardToken_price_usd": ,
            "token0_price_usd": ,
            "token1_price_usd": ,
            "hypervisor_share_price_usd": ,
            "extra": {
                "baseRewards_apr": ,
                "baseRewards_apy": ,
                "boostedRewards_apr": ,
                "boostedRewards_apy": ,
            },
        }

    Yields:
        Iterator[dict]: _description_
    """
    reward_data = {}
    # apr
    try:
        # get prices
        (
            rewardToken_price,
            hype_token0_price,
            hype_token1_price,
        ) = get_reward_pool_prices(
            network=network,
            block=block,
            reward_token=rewardToken_address.lower(),
            token0=token0_address.lower(),
            token1=token1_address.lower(),
        )

        # end control vars
        cum_reward_return = 0
        cum_baseReward_return = 0
        cum_boostedReward_return = 0
        total_period_seconds = 0

        hypervisor_share_price_usd = 0
        baseRewards_apr = 0
        baseRewards_apy = 0
        boostRewards_apr = 0
        boostRewards_apy = 0

        # statics
        day_in_seconds = 60 * 60 * 24
        year_in_seconds = day_in_seconds * 365

        for item in items_to_calc_apr:
            # discard items with timepassed = 0
            if item["time_passed"] == 0:
                logging.getLogger(__name__).debug(
                    f" ...no time passed found while processing apr for {hypervisor_address} using item {item}"
                )
                continue
            if item["hypervisor"]["totalSupply"] == 0:
                logging.getLogger(__name__).warning(
                    f" ...no supply found while processing apr for {hypervisor_address} using item {item}"
                )
                continue

            # calculate price per share for each item using current prices
            item["hypervisor"]["tvl"] = (
                item["hypervisor"]["underlying_token0"] * hype_token0_price
                + item["hypervisor"]["underlying_token1"] * hype_token1_price
            )
            item["hypervisor"]["price_per_share"] = (
                item["hypervisor"]["tvl"] / item["hypervisor"]["totalSupply"]
            )
            # calculate how much staked is worth in usd
            item["hypervisor"]["totalStaked_tvl"] = (
                item["hypervisor"]["totalStaked"]
                * item["hypervisor"]["price_per_share"]
            )

            # set the TVL value to be used as denominator: totalStaked or totalSupply
            tvl = item["hypervisor"]["totalStaked_tvl"]
            if not tvl:
                logging.getLogger(__name__).warning(
                    f" using total supply to calc ramses reward apr because there is no staked value for {hypervisor_address}"
                )
                tvl = item["hypervisor"]["tvl"]

            # set price per share var ( the last will remain)
            hypervisor_share_price_usd = item["hypervisor"]["price_per_share"]

            item["base_rewards_usd"] = item["base_rewards"] * rewardToken_price
            item["boosted_rewards_usd"] = item["boosted_rewards"] * rewardToken_price
            item["total_rewards_usd"] = (
                item["base_rewards_usd"] + item["boosted_rewards_usd"]
            )

            # calculate period yield
            item["period_yield"] = item["total_rewards_usd"] / tvl if tvl else 0

            # filter outliers
            if item["period_yield"] > 1:
                logging.getLogger(__name__).warning(
                    f" found outlier period yield {item['period_yield']} for {hypervisor_address} using item {item}"
                )
                item["hypervisor"]["tvl"] = 0
                item["hypervisor"]["totalStaked_tvl"] = 0
                item["hypervisor"]["price_per_share"] = 0
                item["base_rewards_usd"] = 0
                item["boosted_rewards_usd"] = 0
                item["total_rewards_usd"] = 0
                item["period_yield"] = 0
                continue

            # add to cumulative yield
            if cum_reward_return:
                cum_reward_return *= 1 + item["period_yield"]
            else:
                cum_reward_return = 1 + item["period_yield"]
            if cum_baseReward_return:
                cum_baseReward_return *= 1 + (
                    item["base_rewards_usd"] / tvl if tvl else 0
                )
            else:
                cum_baseReward_return = 1 + (
                    item["base_rewards_usd"] / tvl if tvl else 0
                )
            if cum_boostedReward_return:
                cum_boostedReward_return *= 1 + (
                    item["boosted_rewards_usd"] / tvl if tvl else 0
                )
            else:
                cum_boostedReward_return = 1 + (
                    item["boosted_rewards_usd"] / tvl if tvl else 0
                )

            # extrapolate rewards to a year

            item["base_rewards_usd_year"] = (
                item["base_rewards_usd"] / item["time_passed"]
            ) * year_in_seconds
            item["boosted_rewards_usd_year"] = (
                item["boosted_rewards_usd"] / item["time_passed"]
            ) * year_in_seconds
            item["total_rewards_usd_year"] = (
                item["base_rewards_usd_year"] + item["boosted_rewards_usd_year"]
            )

            item["total_reward_apr"] = (cum_reward_return - 1) * (
                (year_in_seconds) / item["time_passed"]
            )
            try:
                item["total_reward_apy"] = (
                    1
                    + (cum_reward_return - 1) * ((day_in_seconds) / item["time_passed"])
                ) ** 365 - 1
            except OverflowError as e:
                logging.getLogger(__name__).debug(
                    f"  cant calc apy Overflow err on  total_reward_apy...{e}"
                )
                item["total_reward_apy"] = 0

            item["base_reward_apr"] = (cum_baseReward_return - 1) * (
                (year_in_seconds) / item["time_passed"]
            )
            try:
                item["base_reward_apy"] = (
                    1
                    + (cum_baseReward_return - 1)
                    * ((day_in_seconds) / item["time_passed"])
                ) ** 365 - 1
            except OverflowError as e:
                logging.getLogger(__name__).debug(
                    f"  cant calc apy Overflow err on  base_reward_apy...{e}"
                )
                item["base_reward_apy"] = 0

            item["boosted_reward_apr"] = (cum_boostedReward_return - 1) * (
                (year_in_seconds) / item["time_passed"]
            )
            try:
                item["boosted_reward_apy"] = (
                    1
                    + (cum_boostedReward_return - 1)
                    * ((day_in_seconds) / item["time_passed"])
                ) ** 365 - 1
            except OverflowError as e:
                logging.getLogger(__name__).debug(
                    f"  cant calc apy Overflow err on  boosted_reward_apy...{e}"
                )
                item["boosted_reward_apy"] = 0

            total_period_seconds += item["time_passed"]

        # calculate total apr
        cum_reward_return -= 1
        cum_baseReward_return -= 1
        cum_boostedReward_return -= 1
        reward_apr = (
            cum_reward_return * ((year_in_seconds) / total_period_seconds)
            if total_period_seconds
            else 0
        )
        try:
            reward_apy = (
                1 + cum_reward_return * ((day_in_seconds) / total_period_seconds)
                if total_period_seconds
                else 0
            ) ** 365 - 1
        except OverflowError as e:
            logging.getLogger(__name__).debug(
                f"  cant calc apy Overflow err on  reward_apy...{e}"
            )
            reward_apy = 0

        baseRewards_apr = (
            cum_baseReward_return * ((year_in_seconds) / total_period_seconds)
            if total_period_seconds
            else 0
        )
        try:
            baseRewards_apy = (
                1 + cum_baseReward_return * ((day_in_seconds) / total_period_seconds)
                if total_period_seconds
                else 0
            ) ** 365 - 1
        except OverflowError as e:
            logging.getLogger(__name__).debug(
                f"  cant calc apy Overflow err on  baseRewards_apy...{e}"
            )
            baseRewards_apy = 0

        boostRewards_apr = (
            cum_boostedReward_return * ((year_in_seconds) / total_period_seconds)
            if total_period_seconds
            else 0
        )

        try:
            boostRewards_apy = (
                1 + cum_boostedReward_return * ((day_in_seconds) / total_period_seconds)
                if total_period_seconds
                else 0
            ) ** 365 - 1
        except OverflowError as e:
            logging.getLogger(__name__).debug(
                f"  cant calc apy Overflow err on  boostRewards_apy...{e}"
            )
            boostRewards_apy = 0

        # build reward data
        reward_data = {
            "apr": reward_apr if reward_apr > 0 else 0,
            "apy": reward_apy if reward_apy > 0 else 0,
            "rewardToken_price_usd": rewardToken_price,
            "token0_price_usd": hype_token0_price,
            "token1_price_usd": hype_token1_price,
            "hypervisor_share_price_usd": hypervisor_share_price_usd,
            # extra fields
            "extra": {
                "baseRewards_apr": baseRewards_apr if baseRewards_apr > 0 else 0,
                "baseRewards_apy": baseRewards_apy if baseRewards_apy > 0 else 0,
                "boostedRewards_apr": boostRewards_apr if boostRewards_apr > 0 else 0,
                "boostedRewards_apy": boostRewards_apy if boostRewards_apy > 0 else 0,
            },
        }

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Error while calculating rewards yield for ramses hypervisor {hypervisor_address} reward token {rewardToken_address} at block {block} err: {e}"
        )

    return reward_data
