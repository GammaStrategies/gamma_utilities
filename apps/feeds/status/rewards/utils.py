from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
    get_price_from_db,
)
from bins.formulas.apr import calculate_rewards_apr


def get_reward_pool_prices(
    network: str, block: int, reward_token: str, token0: str, token1: str
) -> tuple[float, float, float]:
    """Get reward token, token0 and token1 prices

    Args:
        network (str):
        block (int):
        reward_token (str): reward token address
        token0 (str): pool token0 address
        token1 (str): pool token1 address

    Returns:
        tuple[float, float, float]:  rewardToken_price, hype_token0_price, hype_token1_price
    """
    rewardToken_price = get_price_from_db(
        network=network,
        block=block,
        token_address=reward_token,
    )
    hype_token0_price = get_price_from_db(
        network=network,
        block=block,
        token_address=token0,
    )
    hype_token1_price = get_price_from_db(
        network=network,
        block=block,
        token_address=token1,
    )

    return rewardToken_price, hype_token0_price, hype_token1_price


def get_hypervisor_price_per_share(
    hypervisor_status: dict, token0_price: float, token1_price: float
) -> float:
    # do not calculate when no supply
    if int(hypervisor_status["totalSupply"]) == 0:
        return 0.0

    # get total amounts in pool ( owed and liquidity providing)
    hype_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
        10 ** hypervisor_status["pool"]["token0"]["decimals"]
    )
    hype_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
        10 ** hypervisor_status["pool"]["token1"]["decimals"]
    )

    # get uncollected fees for the pool at this block
    uncollected_fees0 = float(hypervisor_status["fees_uncollected"]["qtty_token0"])
    uncollected_fees1 = float(hypervisor_status["fees_uncollected"]["qtty_token1"])

    # create an easy to debug totals
    total_token0 = hype_total0 + uncollected_fees0
    total_token1 = hype_total1 + uncollected_fees1

    # calculate price per share
    return (token0_price * total_token0 + token1_price * total_token1) / (
        int(hypervisor_status["totalSupply"]) / (10 ** hypervisor_status["decimals"])
    )


def reward_add_status_fields(
    reward_data: dict,
    hypervisor_status: dict,
    apr: float,
    rewardToken_price: float,
    token0_price: float,
    token1_price: float,
    hype_price_per_share: float | None = None,
) -> dict:
    # add status fields ( APR )
    reward_data["hypervisor_symbol"] = hypervisor_status["symbol"]
    reward_data["dex"] = hypervisor_status["dex"]
    reward_data["apr"] = apr
    reward_data["rewardToken_price_usd"] = rewardToken_price
    reward_data["token0_price_usd"] = token0_price
    reward_data["token1_price_usd"] = token1_price
    reward_data[
        "hypervisor_share_price_usd"
    ] = hype_price_per_share or get_hypervisor_price_per_share(
        hypervisor_status=hypervisor_status,
        token0_price=token0_price,
        token1_price=token1_price,
    )

    return reward_data


def add_apr_process01(
    network: str,
    hypervisor_status: dict,
    reward_data: dict,
    rewardToken_price: float | None = None,
    hype_token0_price: float | None = None,
    hype_token1_price: float | None = None,
    hype_price_per_share: float | None = None,
) -> dict:
    # token_prices
    if not rewardToken_price or not hype_token0_price or not hype_token1_price:
        (
            rewardToken_price,
            hype_token0_price,
            hype_token1_price,
        ) = get_reward_pool_prices(
            network=network,
            block=hypervisor_status["block"],
            reward_token=reward_data["rewardToken"],
            token0=hypervisor_status["pool"]["token0"]["address"],
            token1=hypervisor_status["pool"]["token1"]["address"],
        )

    # hypervisor price per share
    if not hype_price_per_share:
        hype_price_per_share = get_hypervisor_price_per_share(
            hypervisor_status=hypervisor_status,
            token0_price=hype_token0_price,
            token1_price=hype_token1_price,
        )

    if int(reward_data["total_hypervisorToken_qtty"]) and hype_price_per_share:
        # if there is hype qtty staked and price per share
        apr = calculate_rewards_apr(
            token_price=rewardToken_price,
            token_reward_rate=int(reward_data["rewards_perSecond"])
            / (10 ** reward_data["rewardToken_decimals"]),
            total_lp_locked=int(reward_data["total_hypervisorToken_qtty"])
            / (10 ** hypervisor_status["decimals"]),
            lp_token_price=hype_price_per_share,
        )
    else:
        # no apr if no hype qtty staked or no price per share
        apr = 0

    reward_data = reward_add_status_fields(
        reward_data=reward_data,
        hypervisor_status=hypervisor_status,
        apr=apr,
        rewardToken_price=rewardToken_price,
        token0_price=hype_token0_price,
        token1_price=hype_token1_price,
        hype_price_per_share=hype_price_per_share,
    )

    return reward_data


def get_hypervisor_data_for_apr(
    network: str,
    hypervisor_address: str,
    timestamp_ini: int,
    timestamp_end: int,
) -> list[dict]:
    """Create a sorted by time list of hypervisor status, rewards and static data for apr calculation.

    Args:
        network (str):
        hypervisor_address (str):
        timestamp_ini (int): timestamp to start getting data from.
        timestamp_end (int): timestamp to end getting data from.

    Returns:
        list:
    """

    # build query (ease debuging)
    query = get_default_localdb(
        network=network
    ).query_locs_apr_hypervisor_data_calculation(
        hypervisor_address=hypervisor_address,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
    )

    # get hype data from db so that apr can be constructed.
    return get_from_localdb(
        network=network,
        collection="operations",
        aggregate=query,
        batch_size=100000,
    )