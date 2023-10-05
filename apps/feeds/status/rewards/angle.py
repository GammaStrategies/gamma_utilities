import logging


from apps.feeds.utils import (
    filter_hypervisor_data_for_apr,
    get_hypervisor_data_for_apr,
    get_hypervisor_price_per_share,
    get_reward_pool_prices,
)
from bins.apis.angle_merkle import angle_merkle_wraper
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, rewarderType
from bins.w3.protocols.angle.rewarder import angle_merkle_distributor_creator


# new method
def build_angle_merkle_rewards_status(
    network: str,
    hypervisor_status: dict,
    rewarder_static: dict,
    max_items: int = 6,
):
    # create result
    result = []

    # TODO: modify query to adjust to slow/fast networks:  get last x data qtty instead of timestamps or blocks
    # current angle endpoint seems to show snapshot every 6-12 hours
    # define ini timestamp to 2 day data before end ( will only use a maximum of 5 items to optimize performance[time basically])
    ini_timestamp = hypervisor_status["timestamp"] - (86400 * 7)

    # get hypervisor data for apr
    if apr_ordered_hypervisor_status_db_list := get_hypervisor_data_for_apr(
        network=network,
        hypervisor_address=hypervisor_status["address"],
        timestamp_ini=ini_timestamp,
        timestamp_end=hypervisor_status["timestamp"],
    ):
        # filter not valid items
        apr_ordered_hypervisor_status_db_list = filter_hypervisor_data_for_apr(
            data=apr_ordered_hypervisor_status_db_list
        )

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
            # add hype status if not already in the list and its not a block after the last one
            ids = [x["id"] for x in _ordered_hype_status_db["status"]]
            if (
                hypervisor_status["id"] not in ids
                and hypervisor_status["block"]
                >= _ordered_hype_status_db["status"][-1]["block"]
                and hypervisor_status["block"] - 1
                != _ordered_hype_status_db["status"][-1]["block"]
            ):
                logging.getLogger(__name__).debug(f" adding hype status to list")
                # add as last item
                _ordered_hype_status_db["status"].append(hypervisor_status)

            # get as many seconds back as needed to accurately calculate apr
            # one day seconds back is a minimum
            min_items = None
            _tmp_seconds = 0
            for i in range(len(_ordered_hype_status_db["status"]) - 1, 0, -2):
                # calculate seconds between 2 items
                _tmp_seconds += (
                    _ordered_hype_status_db["status"][i]["timestamp"]
                    - _ordered_hype_status_db["status"][i - 1]["timestamp"]
                )

                if _tmp_seconds >= 86400:
                    min_items = len(_ordered_hype_status_db["status"]) - i
                    # should be even number
                    if min_items % 2 != 0:
                        # if min_items index is out of _ordered_hype_status_db range, reduce it
                        if min_items > len(_ordered_hype_status_db["status"]) - 1:
                            min_items -= 1
                        else:
                            min_items += 1
                    break

            # limit the list items to select ( should be even number)
            if not min_items:
                # avoid None error
                min_items = max_items

            max_items = max(
                min_items,
                (
                    max_items
                    if len(_ordered_hype_status_db["status"]) > max_items
                    else len(_ordered_hype_status_db["status"])
                ),
            )

            # get the last max_items from the list
            filtered_status_list = _ordered_hype_status_db["status"][-max_items:]

            # get the last max_items from the list
            for idx, data in enumerate(filtered_status_list):
                # build distribution data
                distribution_data = build_distribution_data(
                    network=network,
                    distributor_address=rewarder_static["rewarder_registry"],
                    block=data["block"],
                    pool_address=data["pool"]["address"],
                    hypervisor_address=data["address"],
                )
                # add distribution data
                data["distribution_data"] = distribution_data

                rewards_aquired_period_end = None
                # zero and even indexes refer to initial values
                if idx == 0 or idx % 2 == 0:
                    # this is an initial value.
                    pass
                else:
                    # calculate time passed since last item
                    time_passed = data["timestamp"] - last_item["timestamp"]

                    for distribution_data in data["distribution_data"]:
                        real_rewards_end = build_rewards_from_distribution(
                            network=network,
                            hypervisor_status=data,
                            distribution_data=distribution_data,
                            calculations_data=distribution_data["reward_calculations"],
                        )

                        # add real rewards to distribution_data for later use
                        distribution_data["reward_calculations"] = real_rewards_end

                        # calculate rewards of the period
                        rewards_aquired_period_end = (
                            real_rewards_end["reward_x_second_propFees"]
                            + real_rewards_end["reward_x_second_propToken0"]
                            + real_rewards_end["reward_x_second_propToken1"]
                        ) * time_passed

                        # add to items to calc apr: transform to float
                        items_to_calc_apr.append(
                            {
                                "base_rewards": rewards_aquired_period_end
                                / (10 ** rewarder_static["rewardToken_decimals"]),
                                "boosted_rewards": 0,
                                "time_passed": time_passed,
                                "timestamp_ini": last_item["timestamp"],
                                "timestamp_end": data["timestamp"],
                                "pool": {
                                    "address": last_item["pool"]["address"],
                                    "liquidity": last_item["pool"]["liquidity"],
                                },
                                "hypervisor": {
                                    "block": last_item["block"],
                                    "base_liquidity": last_item["basePosition"][
                                        "liquidity"
                                    ],
                                    "limit_liquidity": last_item["limitPosition"][
                                        "liquidity"
                                    ],
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
                                "distribution_data": distribution_data,
                            }
                        )

                    # add totals
                    total_baseRewards += (
                        rewards_aquired_period_end if rewards_aquired_period_end else 0
                    )
                    total_boostedRewards += 0
                    total_time_passed += time_passed

                # set lastitem
                last_item = data

        # warn if total time passed is less than 1 day
        if total_time_passed < 60 * 60 * 24:
            logging.getLogger(__name__).warning(
                f" Merkle rewards: total time passed is less than 1 day: {total_time_passed} for {hypervisor_status['symbol']} {hypervisor_status['address']} at block {hypervisor_status['block']}"
            )

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

        if len(items_to_calc_apr) == 0:
            logging.getLogger(__name__).warning(
                f" Merkle rewards: There are no items to calc rewards for {network}'s {hypervisor_status['symbol']} {hypervisor_status['address']} at block {hypervisor_status['block']}"
            )
            return

        # calculate apr
        if reward_apr_data_to_save := create_rewards_status_calculate_apr(
            hypervisor_address=hypervisor_status["address"],
            network=network,
            block=hypervisor_status["block"],
            rewardToken_address=rewarder_static["rewardToken"],
            items_to_calc_apr=items_to_calc_apr,
        ):
            # build reward data
            reward_data = {
                "apr": reward_apr_data_to_save["apr"],
                "apy": reward_apr_data_to_save["apy"],
                "block": hypervisor_status["block"],
                "timestamp": hypervisor_status["timestamp"],
                "hypervisor_address": hypervisor_status["address"],
                "hypervisor_symbol": hypervisor_status["symbol"],
                "dex": hypervisor_status["dex"],
                "rewarder_address": rewarder_static["rewarder_address"].lower(),
                "rewarder_type": rewarderType.ANGLE_MERKLE,
                "rewarder_refIds": [],
                "rewarder_registry": rewarder_static["rewarder_registry"].lower(),
                "rewardToken": rewarder_static["rewardToken"].lower(),
                "rewardToken_symbol": rewarder_static["rewardToken_symbol"],
                "rewardToken_decimals": rewarder_static["rewardToken_decimals"],
                "rewardToken_price_usd": reward_apr_data_to_save[
                    "rewardToken_price_usd"
                ],
                "token0_price_usd": reward_apr_data_to_save["token0_price_usd"],
                "token1_price_usd": reward_apr_data_to_save["token1_price_usd"],
                "rewards_perSecond": str(totalRewards_per_second),
                "total_hypervisorToken_qtty": str(hypervisor_status["totalSupply"]),
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
        logging.getLogger(__name__).debug(
            f" No hype status data to construct APR was found in database for {hypervisor_status['symbol']} {hypervisor_status['address']} at block {hypervisor_status['block']}"
        )

    #
    return result


def build_distribution_data(
    network: str,
    distributor_address: str,
    block: int,
    pool_address: str,
    hypervisor_address: str,
) -> list:
    result = []

    # create merkl helper at status block
    distributor_creator = angle_merkle_distributor_creator(
        address=distributor_address,
        network=network,
        block=block,
    )

    # save for later use
    _epoch_duration = distributor_creator.EPOCH_DURATION

    # get active distribution data from merkle
    if distributions := distributor_creator.getActivePoolDistributions(
        address=pool_address
    ):
        for distribution_data in distributions:
            # check if reward token is valid
            if not distributor_creator.isValid_reward_token(
                reward_address=distribution_data["token"].lower()
            ):
                # not a valid reward token
                continue
            if distributor_creator.isBlacklisted(
                reward_data=distribution_data, hypervisor_address=hypervisor_address
            ):
                # blacklisted
                logging.getLogger(__name__).debug(
                    f" {distribution_data['token']} is blacklisted for hype {hypervisor_address} at block {block}"
                )
                continue

            distribution_data["epoch_duration"] = _epoch_duration
            distribution_data[
                "reward_calculations"
            ] = distributor_creator.get_reward_calculations(
                distribution=distribution_data, _epoch_duration=_epoch_duration
            )
            result.append(distribution_data)

    else:
        # no active distributions
        logging.getLogger(__name__).debug(
            f" no active distributions found for {network}'s pool address {pool_address} at block {block}"
        )

    return result


def build_rewards_from_distribution(
    network: str,
    hypervisor_status: dict,
    distribution_data: dict,
    calculations_data: dict,
) -> dict:
    # get token prices
    (
        rewardToken_price,
        token0_price,
        token1_price,
    ) = get_reward_pool_prices(
        network=network,
        block=hypervisor_status["block"],
        reward_token=distribution_data["token"],
        token0=distribution_data["token0_contract"],
        token1=distribution_data["token1_contract"],
    )

    # hypervisor data
    hype_price_per_share = get_hypervisor_price_per_share(
        hypervisor_status=hypervisor_status,
        token0_price=token0_price,
        token1_price=token1_price,
    )
    hype_tvl_usd = (
        int(hypervisor_status["totalSupply"]) / (10 ** hypervisor_status["decimals"])
    ) * hype_price_per_share
    hypervisor_liquidity = int(hypervisor_status["basePosition"]["liquidity"]) + int(
        hypervisor_status["limitPosition"]["liquidity"]
    )
    hypervisor_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
        10 ** hypervisor_status["pool"]["token0"]["decimals"]
    )
    hypervisor_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
        10 ** hypervisor_status["pool"]["token1"]["decimals"]
    )

    # pool data
    pool_liquidity = int(hypervisor_status["pool"]["liquidity"])
    pool_total0 = int(distribution_data["token0_balance_in_pool"]) / (
        10 ** int(distribution_data["token0_decimals"])
    )
    pool_total1 = int(distribution_data["token1_balance_in_pool"]) / (
        10 ** int(distribution_data["token1_decimals"])
    )
    pool_tvl_usd = pool_total0 * token0_price + pool_total1 * token1_price

    # reward x second
    reward_x_second_propFees = (
        (calculations_data["reward_x_second"] * (distribution_data["propFees"] / 10000))
        * (hypervisor_liquidity / pool_liquidity)
        if pool_liquidity
        else 0
    )
    reward_x_second_propToken0 = (
        (
            calculations_data["reward_x_second"]
            * (distribution_data["propToken0"] / 10000)
        )
        * (hypervisor_total0 / pool_total0)
        if pool_total0
        else 0
    )
    reward_x_second_propToken1 = (
        (
            calculations_data["reward_x_second"]
            * (distribution_data["propToken1"] / 10000)
        )
        * (hypervisor_total1 / pool_total1)
        if pool_total1
        else 0
    )

    return {
        "reward_x_second_propFees": reward_x_second_propFees,
        "reward_x_second_propToken0": reward_x_second_propToken0,
        "reward_x_second_propToken1": reward_x_second_propToken1,
        "rewardToken_price": rewardToken_price,
        "token0_price": token0_price,
        "token1_price": token1_price,
        "hype_price_per_share": hype_price_per_share,
        "hype_tvl_usd": hype_tvl_usd,
        "hypervisor_liquidity": hypervisor_liquidity,
        "hypervisor_total0": hypervisor_total0,
        "hypervisor_total1": hypervisor_total1,
        "pool_liquidity": pool_liquidity,
        "pool_total0": pool_total0,
        "pool_total1": pool_total1,
        "pool_tvl_usd": pool_tvl_usd,
    }


def create_rewards_status_calculate_apr(
    hypervisor_address: str,
    network: str,
    block: int,
    rewardToken_address: str,
    items_to_calc_apr: list,
) -> dict:
    """

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
        if len(items_to_calc_apr) == 0:
            raise ValueError(f" There are no items to calc rewards")

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
            # simplify price access
            hype_token0_price = item["distribution_data"]["reward_calculations"][
                "token0_price"
            ]
            hype_token1_price = item["distribution_data"]["reward_calculations"][
                "token1_price"
            ]
            rewardToken_price = item["distribution_data"]["reward_calculations"][
                "rewardToken_price"
            ]

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
            tvl = (
                item["hypervisor"]["underlying_token0"] * hype_token0_price
                + item["hypervisor"]["underlying_token1"] * hype_token1_price
            )

            # set price per share var ( the last will remain)
            hypervisor_share_price_usd = item["distribution_data"][
                "reward_calculations"
            ]["hype_price_per_share"]

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


def create_rewards_status_calculate_apr_otherMethod(
    hypervisor_address: str,
    network: str,
    block: int,
    rewardToken_address: str,
    items_to_calc_apr: list,
) -> dict:
    """

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
        # end control vars
        total_base_rewards = 0
        total_boosted_rewards = 0
        list_of_denominators = []
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
            # simplify price access
            hype_token0_price = item["distribution_data"]["reward_calculations"][
                "token0_price"
            ]
            hype_token1_price = item["distribution_data"]["reward_calculations"][
                "token1_price"
            ]
            rewardToken_price = item["distribution_data"]["reward_calculations"][
                "rewardToken_price"
            ]

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
            tvl = (
                item["hypervisor"]["underlying_token0"] * hype_token0_price
                + item["hypervisor"]["underlying_token1"] * hype_token1_price
            )

            # set price per share var ( the last will remain)
            hypervisor_share_price_usd = item["distribution_data"][
                "reward_calculations"
            ]["hype_price_per_share"]

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

            total_base_rewards += item["base_rewards_usd"]
            total_boosted_rewards += item["boosted_rewards_usd"]
            list_of_denominators.append(tvl)

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

            total_period_seconds += item["time_passed"]

        if total_period_seconds < 60:
            logging.getLogger(__name__).error(
                f" total period seconds {total_period_seconds} for {hypervisor_address} at block {block} is too low to calculate apr"
            )

        # average tvl for the period
        total_tvl_denominator = sum(list_of_denominators) / len(list_of_denominators)
        # period yield
        total_yield_period = (
            total_base_rewards + total_boosted_rewards
        ) / total_tvl_denominator
        # calculate apr
        reward_apr = (total_yield_period / total_period_seconds) * year_in_seconds
        baseRewards_apr = (
            (total_base_rewards / total_tvl_denominator) / total_period_seconds
        ) * year_in_seconds
        boostRewards_apr = (
            (total_boosted_rewards / total_tvl_denominator) / total_period_seconds
        ) * year_in_seconds

        # build reward data
        reward_data = {
            "apr": reward_apr if reward_apr > 0 else 0,
            "apy": reward_apr if reward_apr > 0 else 0,
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
            f" Error while calculating angleMerkl rewards yield for hypervisor {hypervisor_address} reward token {rewardToken_address} at block {block} err: {e}"
        )

    return reward_data


# deprecated method ( use build_angle_merkle_rewards_status) ############
def create_rewards_status_angle_merkle_deprecated(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    logging.getLogger(__name__).warning(
        f" Deprecated method create_rewards_status_angle_merkle. Use build_angle_merkle_rewards_status instead"
    )

    result = []

    # create merkl helper at status block
    distributor_creator = angle_merkle_distributor_creator(
        address=rewarder_static["rewarder_registry"],
        network=network,
        block=hypervisor_status["block"],
    )
    # save for later use
    _epoch_duration = distributor_creator.EPOCH_DURATION

    # get active distribution data from merkle
    distributions = distributor_creator.getActivePoolDistributions(
        address=hypervisor_status["pool"]["address"]
    )
    for distribution_data in distributions:
        # check if reward token is valid
        if distributor_creator.isValid_reward_token(
            reward_address=distribution_data["token"].lower()
        ):
            try:
                # check tokenX == hype tokenX
                if (
                    distribution_data["token0_contract"]
                    != hypervisor_status["pool"]["token0"]["address"]
                    or distribution_data["token1_contract"]
                    != hypervisor_status["pool"]["token1"]["address"]
                ):
                    # big problem
                    logging.getLogger(__name__).error(
                        f" Merkle Rewards - rewarder id {rewarder_static['rewarder_address']} has different pool tokens than the hypervisor it is attached to!!"
                    )

                # get token prices
                (
                    rewardToken_price,
                    token0_price,
                    token1_price,
                ) = get_reward_pool_prices(
                    network=network,
                    block=hypervisor_status["block"],
                    reward_token=distribution_data["token"],
                    token0=distribution_data["token0_contract"],
                    token1=distribution_data["token1_contract"],
                )

                # hypervisor data
                hype_price_per_share = get_hypervisor_price_per_share(
                    hypervisor_status=hypervisor_status,
                    token0_price=token0_price,
                    token1_price=token1_price,
                )
                hype_tvl_usd = (
                    int(hypervisor_status["totalSupply"])
                    / (10 ** hypervisor_status["decimals"])
                ) * hype_price_per_share
                hypervisor_liquidity = int(
                    hypervisor_status["basePosition"]["liquidity"]
                ) + int(hypervisor_status["limitPosition"]["liquidity"])
                hypervisor_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
                    10 ** hypervisor_status["pool"]["token0"]["decimals"]
                )
                hypervisor_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
                    10 ** hypervisor_status["pool"]["token1"]["decimals"]
                )

                # pool data
                pool_liquidity = int(hypervisor_status["pool"]["liquidity"])
                pool_total0 = int(distribution_data["token0_balance_in_pool"]) / (
                    10 ** int(distribution_data["token0_decimals"])
                )
                pool_total1 = int(distribution_data["token1_balance_in_pool"]) / (
                    10 ** int(distribution_data["token1_decimals"])
                )
                pool_tvl_usd = pool_total0 * token0_price + pool_total1 * token1_price

                # multiple reward information
                calculations = distributor_creator.get_reward_calculations(
                    distribution=distribution_data, _epoch_duration=_epoch_duration
                )

                # reward x year decimal
                reward_x_year_decimal_propFees = (
                    calculations["reward_yearly_fees_decimal"]
                ) * (hypervisor_liquidity / pool_liquidity)
                reward_x_year_decimal_propToken0 = (
                    calculations["reward_yearly_token0_decimal"]
                ) * (hypervisor_total0 / pool_total0)
                reward_x_year_decimal_propToken1 = (
                    calculations["reward_yearly_token1_decimal"]
                ) * (hypervisor_total1 / pool_total1)

                # reward x second
                reward_x_second_propFees = (
                    calculations["reward_x_second"]
                    * (distribution_data["propFees"] / 10000)
                ) * (hypervisor_liquidity / pool_liquidity)
                reward_x_second_propToken0 = (
                    calculations["reward_x_second"]
                    * (distribution_data["propToken0"] / 10000)
                ) * (hypervisor_total0 / pool_total0)
                reward_x_second_propToken1 = (
                    calculations["reward_x_second"]
                    * (distribution_data["propToken1"] / 10000)
                ) * (hypervisor_total1 / pool_total1)

                total_yearly_rewards = (
                    reward_x_year_decimal_propFees
                    + reward_x_year_decimal_propToken0
                    + reward_x_year_decimal_propToken1
                )

                fee_APR = (
                    reward_x_year_decimal_propFees * rewardToken_price
                ) / hype_tvl_usd
                token0_APR = (
                    reward_x_year_decimal_propToken0 * rewardToken_price
                ) / hype_tvl_usd
                token1_APR = (
                    reward_x_year_decimal_propToken1 * rewardToken_price
                ) / hype_tvl_usd

                hype_APR = fee_APR + token0_APR + token1_APR
                reward_x_second = int(
                    reward_x_second_propToken1
                    + reward_x_second_propToken0
                    + reward_x_second_propFees
                )

                # build reward base data
                reward_data = distributor_creator.construct_reward_data(
                    distribution_data=distribution_data,
                    hypervisor_address=hypervisor_status["address"],
                    total_hypervisorToken_qtty=hypervisor_status["totalSupply"],
                    epoch_duration=_epoch_duration,
                    convert_bint=True,
                )

                reward_data["rewards_perSecond"] = str(reward_x_second)

                # add status fields ( APR )
                reward_data["hypervisor_symbol"] = hypervisor_status["symbol"]
                reward_data["dex"] = hypervisor_status["dex"]
                reward_data["apr"] = hype_APR
                reward_data["rewardToken_price_usd"] = rewardToken_price
                reward_data["token0_price_usd"] = token0_price
                reward_data["token1_price_usd"] = token1_price
                reward_data["hypervisor_share_price_usd"] = hype_price_per_share

                # add reward to result
                result.append(reward_data)

            except Exception as e:
                logging.getLogger(__name__).error(
                    f" Merkle Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Merkle Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {distributions}"
                )
                # make sure we return an empty list
                return []

        else:
            # this rewarder is not for this hypervisor
            continue

    # empty result means no rewards at this block
    if not result:
        logging.getLogger(__name__).debug(
            f" Merkle Rewards-> {network}'s {rewarder_static['rewardToken']} has no rewards at block {hypervisor_status['block']}"
        )

    return result


# User rewards status


def create_user_rewards_status_merkl(
    chain: Chain,
    already_processed: list,
    rewrite: bool = False,
):
    # TODO: work in progress

    # create merkl helper
    canciller = angle_merkle_wraper()
    # get epochs
    for epoch_data in canciller.get_epochs(chain=chain):
        timestamp = epoch_data["timestamp"]
        epoch = epoch_data["epoch"]
        # get rewards for epoch
        for merkl_proof, merkl_data in canciller.get_rewards(chain=chain, epoch=epoch):
            # boostedAddress = merkl_data["boostedAddress"]
            # boostedReward = merkl_data["boostedReward"]
            # lastUpdateEpoch = merkl_data["lastUpdateEpoch"]
            # pool = merkl_data["pool"]
            # token = merkl_data["token"]
            # tokenDecimals = merkl_data["tokenDecimals"]
            # tokenSymbol = merkl_data["tokenSymbol"]
            # totalAmount = merkl_data["totalAmount"]

            for holder_address, amount_data in merkl_data["holders"].items():
                if gamma_amount := amount_data["breakdown"].get("gamma", 0):
                    # this gamma user has merkl rewards
                    pass
