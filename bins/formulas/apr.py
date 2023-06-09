def calculate_rewards_apr(
    token_price: float,
    token_reward_rate: float,
    total_lp_locked,
    lp_token_price,
) -> float:
    """Calculate the APR of a reward token

    Args:
        token_price (float): price of the rewarded token
        token_reward_rate (float): reward rate of the rewarded token. Converted to float ( divide by 10** token decimals)
        total_lp_locked (_type_): total lp tokens locked in contract. Converted to float ( divide by 10** token decimals)
        lp_token_price (_type_): lp token price per token

    Returns:
        float: Apy
    """
    # rewardRate * secondsPerYear * price of token) / (totalSupply * price per LP Token)
    secondsPerYear = 365 * 24 * 60 * 60

    return (token_reward_rate * secondsPerYear * token_price) / (
        total_lp_locked * lp_token_price
    )
