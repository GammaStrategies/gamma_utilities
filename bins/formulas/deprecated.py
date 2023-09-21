import logging
from web3 import Web3
from bins.formulas.constants import X128, X256


def subIn256(x, y):
    difference = x - y
    if difference < 0:
        difference += X256

    return difference


def mulDiv(a, b, c):
    result = (a * b) // c

    if not isinstance(result, int):
        logging.getLogger(__name__).error(
            f" -->>  mulDiv error: {a} * {b} // {c} = {result} converting to {int(result)}"
        )
        return int(result)
    if result < 0:
        raise ValueError(f"mulDiv: result is negative -->  {a} * {b} // {c} = {result}")

    return result


def whois_token(token_addressA: str, token_addressB: str) -> tuple[str, str]:
    """return base and quote token addresses in the pool
        token0 is the base token, token1 is the quote token
        The price of the pool is always token1/token0

    Args:
        token_addressA (str): token address in the pool
        token_addressB (str): token address in the pool

    Returns:
        tuple[str, str]: token0, token1
    """
    return (
        (token_addressA, token_addressB)
        if Web3.toChecksumAddress(token_addressA)
        < Web3.toChecksumAddress(token_addressB)
        else (
            token_addressB,
            token_addressA,
        )
    )


######### (for comparison purposes)
######### def as defined at : https://github.com/GammaStrategies/uniswap-v3-performance
def get_uncollected_fees_vGammawire(
    fee_growth_global_0,
    fee_growth_global_1,
    tick_current,
    tick_lower,
    tick_upper,
    fee_growth_outside_0_lower,
    fee_growth_outside_1_lower,
    fee_growth_outside_0_upper,
    fee_growth_outside_1_upper,
    liquidity,
    fee_growth_inside_last_0,
    fee_growth_inside_last_1,
):
    if tick_current >= tick_lower:
        fee_growth_below_pos_0 = fee_growth_outside_0_lower
        fee_growth_below_pos_1 = fee_growth_outside_1_lower
    else:
        fee_growth_below_pos_0 = subIn256(
            fee_growth_global_0, fee_growth_outside_0_lower
        )
        fee_growth_below_pos_1 = subIn256(
            fee_growth_global_1, fee_growth_outside_1_lower
        )

    if tick_current >= tick_upper:
        fee_growth_above_pos_0 = subIn256(
            fee_growth_global_0, fee_growth_outside_0_upper
        )
        fee_growth_above_pos_1 = subIn256(
            fee_growth_global_1, fee_growth_outside_1_upper
        )
    else:
        fee_growth_above_pos_0 = fee_growth_outside_0_upper
        fee_growth_above_pos_1 = fee_growth_outside_1_upper

    fees_accum_now_0 = subIn256(
        subIn256(fee_growth_global_0, fee_growth_below_pos_0),
        fee_growth_above_pos_0,
    )
    fees_accum_now_1 = subIn256(
        subIn256(fee_growth_global_1, fee_growth_below_pos_1),
        fee_growth_above_pos_1,
    )

    uncollectedFees_0 = (
        liquidity * (subIn256(fees_accum_now_0, fee_growth_inside_last_0))
    ) // X128
    uncollectedFees_1 = (
        liquidity * (subIn256(fees_accum_now_1, fee_growth_inside_last_1))
    ) // X128

    return uncollectedFees_0, uncollectedFees_1
