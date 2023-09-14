from dataclasses import dataclass
from math import sqrt
from web3 import Web3
from decimal import Decimal

X32 = 2**32
X96 = 2**96
X96_RESOLLUTION = 96
X128 = 2**128
X256 = 2**256


def subIn256(x, y):
    difference = x - y
    if difference < 0:
        difference += X256

    return difference


def get_uncollected_fees(
    feeGrowthGlobal,
    feeGrowthOutsideLower,
    feeGrowthOutsideUpper,
    feeGrowthInsideLast,
    tickCurrent,
    liquidity,
    tickLower,
    tickUpper,
) -> float:
    """Precise method to calc uncollected fees

    Args:
       feeGrowthGlobal (_type_): _description_
       feeGrowthOutsideLower (_type_): _description_
       feeGrowthOutsideUpper (_type_): _description_
       feeGrowthInsideLast (_type_): _description_
       tickCurrent (_type_): _description_
       liquidity (_type_): _description_
       tickLower (_type_): _description_
       tickUpper (_type_): _description_

    Returns:
       fees
    """

    # convert to decimal for later operations accuracy
    liquidity = Decimal(liquidity)
    feeGrowthOutsideLower = Decimal(feeGrowthOutsideLower)
    feeGrowthOutsideUpper = Decimal(feeGrowthOutsideUpper)
    feeGrowthInsideLast = Decimal(feeGrowthInsideLast)

    feeGrowthBelow = 0
    if tickCurrent >= tickLower:
        feeGrowthBelow = feeGrowthOutsideLower
    else:
        feeGrowthBelow = subIn256(feeGrowthGlobal, feeGrowthOutsideLower)

    feeGrowthAbove = 0
    if tickCurrent < tickUpper:
        feeGrowthAbove = feeGrowthOutsideUpper
    else:
        feeGrowthAbove = subIn256(feeGrowthGlobal, feeGrowthOutsideUpper)

    feeGrowthInside = subIn256(
        subIn256(feeGrowthGlobal, feeGrowthBelow), feeGrowthAbove
    )

    # return a float ( lower accuracy )
    return float(
        (subIn256(feeGrowthInside, feeGrowthInsideLast) * (liquidity)) / Decimal(X128)
    )


def get_fees_collected_inRange():
    """fees ever minus the fees above and below the position’s range at time

    feeGrowthGlobal - fees collected below the lower tick - fees collected above the upper tick
    """
    pass


# liquidity * ( pool fee returns at time T - pool fee returns at time 0 )  / 2^128

# pool fee returns at time 0 = feeGrowthInside_LastX128
# pool fee returns at time T = feeGrowthGlobal_X128 - fees collected below the lower tick - fees collected above the upper tick

# fees collected below the lower tick =
#       if tick >= tickLower
#           feeGrowthOutsideLower
#       else ( if tick < tickLower )
#           feeGrowthGlobal - feeGrowthOutsideLower
# fees collected above the upper tick =
#       if tick >= tickUpper
#           feeGrowthGlobal - feeGrowthOutsideUpper
#       else ( if tick < tickUpper )
#           feeGrowthOutsideUpper


# uncollected fees ###########
@dataclass
class position:
    tick: int
    tickUpper: int
    tickLower: int
    feeGrowthGlobal: int
    feeGrowthOutsideUpper: int
    feeGrowthOutsideLower: int


def fees_collected_below_tickLower(
    tick: int, tickLower: int, feeGrowthGlobal: int, feeGrowthOutsideLower: int
) -> int:
    """fees collected below the lower tick

    Args:
       tick (int): current tick
       tickLower (int): lower tick of the position
       feeGrowthGlobal (int): feeGrowthGlobal
       feeGrowthOutsideLower (int): feeGrowthOutside_X128 of the lower tick of the position

    Returns:
       fees collected below the lower tick
    """
    if tick >= tickLower:
        return feeGrowthOutsideLower
    else:
        return subIn256(feeGrowthGlobal, feeGrowthOutsideLower)


def fees_collected_above_tickUpper(
    tick: int, tickUpper: int, feeGrowthGlobal: int, feeGrowthOutsideUpper: int
) -> int:
    """fees collected above the upper tick

    Args:
       tick (int): current tick
       tickUpper (int): upper tick of the position
       feeGrowthGlobal (int): feeGrowthGlobal
       feeGrowthOutsideUpper (int): feeGrowthOutside_X128 of the upper tick of the position

    Returns:
       fees collected above the upper tick
    """
    if tick >= tickUpper:
        return subIn256(feeGrowthGlobal, feeGrowthOutsideUpper)
    else:
        return feeGrowthOutsideUpper


def fees_returns_at_timeT(
    tick: int,
    tickUpper: int,
    tickLower: int,
    feeGrowthGlobal: int,
    feeGrowthOutsideUpper: int,
    feeGrowthOutsideLower: int,
) -> int:
    """fees ever minus the fees above and below the positions range (at time T)
        feeGrowthGlobal - fees collected below the lower tick - fees collected above the upper tick
    Args:
       tick (int): current tick
       tickUpper (int): upper tick of the position
       tickLower (int): lower tick of the position
       feeGrowthGlobal (int): feeGrowthGlobal
       feeGrowthOutsideUpper (int): feeGrowthOutside_X128 of the upper tick of the position
       feeGrowthOutsideLower (int): feeGrowthOutside_X128 of the lower tick of the position

    Returns:
       fees ever minus the fees above and below the positions range at time
    """
    return subIn256(
        subIn256(
            feeGrowthGlobal,
            fees_collected_below_tickLower(
                tick, tickLower, feeGrowthGlobal, feeGrowthOutsideLower
            ),
        ),
        fees_collected_above_tickUpper(
            tick, tickUpper, feeGrowthGlobal, feeGrowthOutsideUpper
        ),
    )


def fees_uncollected_inRange(
    liquidity: int,
    tick: int,
    tickUpper: int,
    tickLower: int,
    feeGrowthGlobal: int,
    feeGrowthOutsideUpper: int,
    feeGrowthOutsideLower: int,
    feeGrowthInsideLast: int,
) -> int:
    """

        liquidity * ( pool fee returns at time T - pool fee returns at time 0 )  / 2^128

    Args:
        liquidity (int):
        tick (int): current tick
        tickUpper (int): upper tick of the position
        tickLower (int): lower tick of the position
        feeGrowthGlobal (int): feeGrowthGlobal
        feeGrowthOutsideUpper (int): feeGrowthOutside_X128 of the upper tick of the position
        feeGrowthOutsideLower (int): feeGrowthOutside_X128 of the lower tick of the position
        feeGrowthInsideLast (int): feeGrowthInside_X128 of the position ( pool fee returns at time 0)

    Returns:
        int: fees uncollected in range
    """
    return (
        liquidity
        * subIn256(
            fees_returns_at_timeT(
                tick=tick,
                tickUpper=tickUpper,
                tickLower=tickLower,
                feeGrowthGlobal=feeGrowthGlobal,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper,
                feeGrowthOutsideLower=feeGrowthOutsideLower,
            ),
            feeGrowthInsideLast,
        )
        / X128
    )


def feeGrowth_to_fee(feeGrowthX128: int, liquidity: int) -> float:
    """feeGrowth to fee

    Args:
       feeGrowthX128 (int): feeGrowth
       liquidity (int): liquidity

    Returns:
       fee
    """
    # return liquidity * feeGrowthX128 / X128
    return (liquidity * (feeGrowthX128)) / X128


##############################


def get_positionKey(ownerAddress: str, tickLower: int, tickUpper: int) -> str:
    """Position key

    Args:
       ownerAddress (_type_): position owner wallet address
       tickLower (_type_): lower tick
       tickUpper (_type_): upper tick

       Returns:
           position key
    """
    val_types = ["address", "int24", "int24"]
    values = [ownerAddress, tickLower, tickUpper]
    return Web3.solidityKeccak(val_types, values).hex()


def get_positionKey_algebra(ownerAddress: str, tickLower: int, tickUpper: int) -> str:
    return f"{(((int(ownerAddress.lower(),16) << 24) | (tickLower & 0xFFFFFF)) << 24) | (tickUpper & 0xFFFFFF):064x}"


def get_positionKey_ramses(
    ownerAddress: str, tickLower: int, tickUpper: int, index: int = 0
) -> str:
    """Position key for the ramses pool

    Args:
       ownerAddress (_type_): position owner wallet address
       tickLower (_type_): lower tick
       tickUpper (_type_): upper tick
       index (int):
       Returns:
           position key
    """
    val_types = ["address", "uint256", "int24", "int24"]
    values = [ownerAddress, index, tickLower, tickUpper]
    return Web3.solidityKeccak(val_types, values).hex()


def convert_tick_to_price(tick: int) -> float:
    """convert int ticks into not decimal adjusted float price

    Args:
       tick (int)

    Returns:
       float: price (not decimal adjusted)
    """
    return float(1.0001**tick)


def convert_tick_to_price_float(
    tick: int, token0_decimal: int, token1_decimal: int
) -> float:
    """convert int ticks into decimal float price

    Args:
       tick (int)
       token0_decimal

    Returns:
       float: price (not decimal adjusted)
    """
    return convert_tick_to_price(tick) * 10 ** (token0_decimal - token1_decimal)


def sqrtPriceX96_to_price_float(
    sqrtPriceX96: int, token0_decimals: int, token1_decimals: int
) -> float:
    return ((sqrtPriceX96**2) / 2 ** (96 * 2)) * 10 ** (
        token0_decimals - token1_decimals
    )


def sqrtPriceX96_to_price_float_v2(
    sqrtPriceX96: int, token0_decimals: int, token1_decimals: int
) -> float:
    # token1 / token0
    price0 = ((sqrtPriceX96 / (2**96)) ** 2) / 10 ** (
        token1_decimals - token0_decimals
    )
    price1 = 1 / price0

    return price0, price1


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


# ramses related
def pool_token_amounts_from_current_price(
    sqrt_price: int, deviation: int, liquidity: int
) -> tuple[int, int]:
    """
            Returns the token0 and token1 amounts around the price and deviation in basis points (1/10000)

    Args:
        price (int): the pool's current sqrt price
        deviation (int): the deviation from current price, in basis points (1/10000)
        liquidity (int): the amount of liquidity to mint

    Returns:
        tuple[token0_amount,token1_amount]
    """

    sqrt_price = int(sqrt_price)
    price = (sqrt_price**2) / 2 ** (96 * 2)
    high_sqrt_x96 = int(sqrt(price * (10000 + deviation) / 10000) * 2**96)
    low_sqrt_x96 = int(sqrt(price * (10000 - deviation) / 10000) * 2**96)
    position_token0_amount = LiquidityAmounts.getAmount0ForLiquidity(
        high_sqrt_x96, sqrt_price, liquidity, False
    )
    position_token1_amount = LiquidityAmounts.getAmount1ForLiquidity(
        sqrt_price, low_sqrt_x96, liquidity, False
    )

    position_token0_amount_ = get_amount0_delta(
        high_sqrt_x96, sqrt_price, liquidity, False
    )
    position_token1_amount_ = get_amount1_delta(
        sqrt_price, low_sqrt_x96, liquidity, False
    )

    return (position_token0_amount, position_token1_amount)


def get_amount0_delta(
    sqrt_ratio_a_x96: int,
    sqrt_ratio_b_x96: int,
    liquidity: int,
) -> int:
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96

    numerator1 = liquidity * (1 << 96)
    numerator2 = sqrt_ratio_b_x96 - sqrt_ratio_a_x96

    assert sqrt_ratio_a_x96 > 0

    return (numerator1 * numerator2 // sqrt_ratio_b_x96) // sqrt_ratio_a_x96


def get_amount1_delta(
    sqrt_ratio_a_x96: int,
    sqrt_ratio_b_x96: int,
    liquidity: int,
) -> int:
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96

    return (liquidity * (sqrt_ratio_b_x96 - sqrt_ratio_a_x96)) // (1 << 96)


def select_ramses_apr_calc_deviation(
    token0_address, token1_address, token0_symbol, token1_symbol
):
    from bins.w3.protocols.ramses.tokenType import (
        token_type_dict,
        Token_Type,
        weth_address,
        ram_address,
    )

    # from https://github.com/RamsesExchange/Ramses-API/blob/master/cl/pools.py
    # get range delta

    # transform keys
    token_type_dict_moded = {k.lower(): v for k, v in token_type_dict.items()}
    # ini
    token0_type = 500
    token0_type = 500
    # set types
    if token0_symbol in token_type_dict_moded:
        token0_type = token_type_dict_moded[token0_symbol]
    if token1_symbol in token_type_dict_moded:
        token1_type = token_type_dict_moded[token1_symbol]

    pool_type = token0_type * token1_type
    range_delta = 500
    # case: LSD and WETH
    if pool_type == Token_Type["LSD"] and (
        token0_address == weth_address or token1_address == weth_address
    ):
        range_delta = 50  # +-0.5%

    # case: neadRAM
    if pool_type == Token_Type["NEAD"] and (
        token0_address == ram_address or token1_address == ram_address
    ):
        range_delta = 50  # +-0.5%

    # case: STABLE-STABLE
    elif pool_type == 9:
        range_delta = 10  # +-0.1%

    # case: STABLE-LOOSE_STABLE
    elif pool_type >= 4:
        range_delta = 50  # +-0.5%

    # case: all other cases
    else:
        range_delta = 500  # +-5%

    return range_delta


class LiquidityAmounts:
    @staticmethod
    def getLiquidityForAmount0(sqrtRatioAX96, sqrtRatioBX96, amount0) -> int:
        """Computes the amount of liquidity received for a given amount of token0 and price range

        Args:
            sqrtRatioAX96 (_type_): _description_
            sqrtRatioBX96 (_type_): _description_
            amount0 (_type_): _description_

        Returns:
            int: liquidity The amount of returned liquidity
        """
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        intermediate = (RA * RB) / X96
        return int((amount0 * intermediate) / (RB - RA))

    @staticmethod
    def getLiquidityForAmount1(sqrtRatioAX96, sqrtRatioBX96, amount1) -> int:
        """Computes the amount of liquidity received for a given amount of token1 and price range

        Args:
            sqrtRatioAX96 (_type_): _description_
            sqrtRatioBX96 (_type_): _description_
            amount1 (_type_): _description_

        Returns:
            int: liquidity The amount of returned liquidity
        """
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        return int((amount1 * X96) / (RB - RA))

    @staticmethod
    def getLiquidityForAmounts(
        sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, amount0, amount1
    ) -> int:
        """Computes the maximum amount of liquidity received for a given amount of token0, token1, the current
           pool prices and the prices at the tick boundaries

        Args:
           sqrtRatioX96 (_type_): A sqrt price representing the current pool prices
           sqrtRatioAX96 (_type_): A sqrt price representing the first tick boundary
           sqrtRatioBX96 (_type_): A sqrt price representing the second tick boundary
           amount0 (int): The amount of token0 being sent in
           amount1 (int): The amount of token1 being sent in

        Returns:
           int: liquidity
        """
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        if sqrtRatioX96 <= sqrtRatioAX96:
            liquidity = LiquidityAmounts.getLiquidityForAmount0(
                sqrtRatioAX96, sqrtRatioBX96, amount0
            )
        elif sqrtRatioX96 < sqrtRatioBX96:
            liquidity0 = LiquidityAmounts.getLiquidityForAmount0(
                sqrtRatioX96, sqrtRatioBX96, amount0
            )
            liquidity1 = LiquidityAmounts.getLiquidityForAmount1(
                sqrtRatioAX96, sqrtRatioX96, amount1
            )
            # decide
            liquidity = liquidity0 if (liquidity0 < liquidity1) else liquidity1

        else:
            liquidity = LiquidityAmounts.getLiquidityForAmount1(
                sqrtRatioAX96, sqrtRatioBX96, amount1
            )

        # result
        return liquidity

    @staticmethod
    def getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity) -> int:
        """Computes the amount of token0 for a given amount of liquidity and a price range

        Args:
           sqrtRatioAX96 (_type_): A sqrt price representing the first tick boundary
           sqrtRatioBX96 (_type_): A sqrt price representing the second tick boundary
           liquidity (_type_): The liquidity being valued

        Returns:
           int: The amount of token0
        """
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        return int((((liquidity << X96_RESOLLUTION) * (RB - RA)) / RB) / RA)

    @staticmethod
    def getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity) -> int:
        """Computes the amount of token1 for a given amount of liquidity and a price range

        Args:
            sqrtRatioAX96 (_type_): A sqrt price representing the first tick boundary
            sqrtRatioBX96 (_type_): A sqrt price representing the second tick boundary
            liquidity (_type_): The liquidity being valued

        Returns:
            int: The amount of token1
        """
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        return int((liquidity * (RB - RA)) / X96)

    @staticmethod
    def getAmountsForLiquidity(
        sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, liquidity
    ) -> tuple:
        """getAmountsForLiquidity _summary_

        Args:
           sqrtRatioX96 (_type_): _description_
           sqrtRatioAX96 (_type_): _description_
           sqrtRatioBX96 (_type_): _description_
           liquidity (_type_): _description_

        Returns:
           tuple: _description_
        """
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        amount0 = 0
        amount1 = 0
        if sqrtRatioX96 <= sqrtRatioAX96:
            amount0 = LiquidityAmounts.getAmount0ForLiquidity(
                sqrtRatioAX96, sqrtRatioBX96, liquidity
            )
        elif sqrtRatioX96 < sqrtRatioBX96:
            amount0 = LiquidityAmounts.getAmount0ForLiquidity(
                sqrtRatioX96, sqrtRatioBX96, liquidity
            )
            amount1 = LiquidityAmounts.getAmount1ForLiquidity(
                sqrtRatioAX96, sqrtRatioX96, liquidity
            )
        else:
            amount1 = LiquidityAmounts.getAmount1ForLiquidity(
                sqrtRatioAX96, sqrtRatioBX96, liquidity
            )

        return amount0, amount1


class TickMath:
    """TickMath is a library for computing the sqrt ratio at a given tick, and the tick corresponding to a given sqrt ratio
    https://github.com/Convexus-Protocol/convexus-sdk-py"""

    MIN_TICK: int = -887272  # min tick that can be used on any pool
    MAX_TICK: int = -MIN_TICK  # max tick that can be used on any pool

    MIN_SQRT_RATIO: int = 4295128739  # sqrt ratio of the min tick
    MAX_SQRT_RATIO: int = (
        1461446703485210103287273052203988822378723970342  # sqrt ratio of the max tick
    )

    @staticmethod
    def getSqrtRatioAtTick(tick: int) -> int:
        """Calculates sqrt(1.0001^tick) * 2^96

        Args:
             tick (int): The input tick for the above formula

        Returns:
             int: A Fixed point Q64.96 number representing the sqrt of the ratio of the two assets (token1/token0) at the given tick
        """

        if tick < TickMath.MIN_TICK or tick > TickMath.MAX_TICK or type(tick) != int:
            raise ValueError(" Tick is not within uniswap's min-max parameters")

        absTick: int = abs(tick)

        ratio: int = (
            0xFFFCB933BD6FAD37AA2D162D1A594001
            if (absTick & 0x1) != 0
            else 0x100000000000000000000000000000000
        )

        if (absTick & 0x2) != 0:
            ratio = (ratio * 0xFFF97272373D413259A46990580E213A) >> 128

        if (absTick & 0x4) != 0:
            ratio = (ratio * 0xFFF2E50F5F656932EF12357CF3C7FDCC) >> 128

        if (absTick & 0x8) != 0:
            ratio = (ratio * 0xFFE5CACA7E10E4E61C3624EAA0941CD0) >> 128

        if (absTick & 0x10) != 0:
            ratio = (ratio * 0xFFCB9843D60F6159C9DB58835C926644) >> 128

        if (absTick & 0x20) != 0:
            ratio = (ratio * 0xFF973B41FA98C081472E6896DFB254C0) >> 128

        if (absTick & 0x40) != 0:
            ratio = (ratio * 0xFF2EA16466C96A3843EC78B326B52861) >> 128

        if (absTick & 0x80) != 0:
            ratio = (ratio * 0xFE5DEE046A99A2A811C461F1969C3053) >> 128

        if (absTick & 0x100) != 0:
            ratio = (ratio * 0xFCBE86C7900A88AEDCFFC83B479AA3A4) >> 128

        if (absTick & 0x200) != 0:
            ratio = (ratio * 0xF987A7253AC413176F2B074CF7815E54) >> 128

        if (absTick & 0x400) != 0:
            ratio = (ratio * 0xF3392B0822B70005940C7A398E4B70F3) >> 128

        if (absTick & 0x800) != 0:
            ratio = (ratio * 0xE7159475A2C29B7443B29C7FA6E889D9) >> 128

        if (absTick & 0x1000) != 0:
            ratio = (ratio * 0xD097F3BDFD2022B8845AD8F792AA5825) >> 128

        if (absTick & 0x2000) != 0:
            ratio = (ratio * 0xA9F746462D870FDF8A65DC1F90E061E5) >> 128

        if (absTick & 0x4000) != 0:
            ratio = (ratio * 0x70D869A156D2A1B890BB3DF62BAF32F7) >> 128

        if (absTick & 0x8000) != 0:
            ratio = (ratio * 0x31BE135F97D08FD981231505542FCFA6) >> 128

        if (absTick & 0x10000) != 0:
            ratio = (ratio * 0x9AA508B5B7A84E1C677DE54F3E99BC9) >> 128

        if (absTick & 0x20000) != 0:
            ratio = (ratio * 0x5D6AF8DEDB81196699C329225EE604) >> 128

        if (absTick & 0x40000) != 0:
            ratio = (ratio * 0x2216E584F5FA1EA926041BEDFE98) >> 128

        if (absTick & 0x80000) != 0:
            ratio = (ratio * 0x48A170391F7DC42444E8FA2) >> 128

        if tick > 0:
            ratio = ((2**256) - 1) // ratio

        # back to Q96
        return (ratio // X32) + 1 if ratio % X32 > 0 else ratio // X32

    @staticmethod
    def getTickAtSqrtRatio(sqrtRatioX96: int) -> int:
        """
        * Returns the tick corresponding to a given sqrt ratio, s.t. #getSqrtRatioAtTick(tick) <= sqrtRatioX96
        * and #getSqrtRatioAtTick(tick + 1) > sqrtRatioX96
        * @param sqrtRatioX96 the sqrt ratio as a Q64.96 for which to compute the tick
        """

        if (
            sqrtRatioX96 < TickMath.MIN_SQRT_RATIO
            or sqrtRatioX96 > TickMath.MAX_SQRT_RATIO
        ):
            raise ValueError(" Tick is not within uniswap's min-max parameters")

        sqrtRatioX128 = sqrtRatioX96 << 32

        msb = TickMath.mostSignificantBit(sqrtRatioX128)

        if msb >= 128:
            r = sqrtRatioX128 >> (msb - 127)
        else:
            r = sqrtRatioX128 << (127 - msb)

        log_2: int = (msb - 128) << 64

        for i in range(14):
            r = (r**2) >> 127
            f = r >> 128
            log_2 = log_2 | (f << (63 - i))
            r = r >> f

        log_sqrt10001 = log_2 * 255738958999603826347141

        tickLow = (log_sqrt10001 - 3402992956809132418596140100660247210) >> 128
        tickHigh = (log_sqrt10001 + 291339464771989622907027621153398088495) >> 128

        return (
            tickLow
            if tickLow == tickHigh
            else tickHigh
            if TickMath.getSqrtRatioAtTick(tickHigh) <= sqrtRatioX96
            else tickLow
        )

    @staticmethod
    def mostSignificantBit(x: int) -> int:
        assert x > 0, "ZERO"
        assert x <= (2**256) - 1, "MAX"

        msb: int = 0
        for power, min in list(
            map(lambda pow: [pow, 2**pow], [128, 64, 32, 16, 8, 4, 2, 1])
        ):
            if x >= min:
                x = x >> power
                msb += power

        return msb


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
    ) / X128
    uncollectedFees_1 = (
        liquidity * (subIn256(fees_accum_now_1, fee_growth_inside_last_1))
    ) / X128

    return uncollectedFees_0, uncollectedFees_1


def my_uncollected_fees(
    fee_growth_global,
    tick_current,
    tick_lower,
    tick_upper,
    fee_growth_outside_lower,
    fee_growth_outside_upper,
    liquidity,
    fee_growth_inside_last,
):
    fees_accum_now = get_fee_growth_inside_now(
        fee_growth_global=fee_growth_global,
        tick_current=tick_current,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        fee_growth_outside_lower=fee_growth_outside_lower,
        fee_growth_outside_upper=fee_growth_outside_upper,
    )

    return (liquidity * (subIn256(fees_accum_now, fee_growth_inside_last))) / X128


def get_fee_growth_inside_now(
    fee_growth_global,
    tick_current,
    tick_lower,
    tick_upper,
    fee_growth_outside_lower,
    fee_growth_outside_upper,
):
    fee_growth_below_pos = fees_collected_below_tickLower(
        tick=tick_current,
        tickLower=tick_lower,
        feeGrowthGlobal=fee_growth_global,
        feeGrowthOutsideLower=fee_growth_outside_lower,
    )

    fee_growth_above_pos = fees_collected_above_tickUpper(
        tick=tick_current,
        tickUpper=tick_upper,
        feeGrowthGlobal=fee_growth_global,
        feeGrowthOutsideUpper=fee_growth_outside_upper,
    )

    return subIn256(
        subIn256(fee_growth_global, fee_growth_below_pos),
        fee_growth_above_pos,
    )
