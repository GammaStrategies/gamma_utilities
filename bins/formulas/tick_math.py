# https://github.com/chainflip-io/chainflip-uniswapV3-python


from .checks import checkInt24, checkUInt160
from .constants import (
    MAX_SQRT_RATIO,
    MAX_TICK,
    MAX_UINT256,
    MIN_SQRT_RATIO,
    MIN_INT24,
    MAX_INT24,
    X32,
)


def getSqrtRatioAtTick(tick: int) -> int:
    """Calculates sqrt(1.0001^tick) * 2^96

    Args:
        tick (int):

    Returns:
         A Fixed point Q64.96 number representing the sqrt of the ratio of the two assets (token1/token0)
    """
    checkInt24(tick)
    absTick = abs(tick)
    assert absTick <= MAX_TICK, "T"

    ratio = (
        0xFFFCB933BD6FAD37AA2D162D1A594001
        if absTick & 0x1 != 0
        else 0x100000000000000000000000000000000
    )

    if absTick & 0x2 != 0:
        ratio = (ratio * 0xFFF97272373D413259A46990580E213A) >> 128
    if absTick & 0x4 != 0:
        ratio = (ratio * 0xFFF2E50F5F656932EF12357CF3C7FDCC) >> 128
    if absTick & 0x8 != 0:
        ratio = (ratio * 0xFFE5CACA7E10E4E61C3624EAA0941CD0) >> 128
    if absTick & 0x10 != 0:
        ratio = (ratio * 0xFFCB9843D60F6159C9DB58835C926644) >> 128
    if absTick & 0x20 != 0:
        ratio = (ratio * 0xFF973B41FA98C081472E6896DFB254C0) >> 128
    if absTick & 0x40 != 0:
        ratio = (ratio * 0xFF2EA16466C96A3843EC78B326B52861) >> 128
    if absTick & 0x80 != 0:
        ratio = (ratio * 0xFE5DEE046A99A2A811C461F1969C3053) >> 128
    if absTick & 0x100 != 0:
        ratio = (ratio * 0xFCBE86C7900A88AEDCFFC83B479AA3A4) >> 128
    if absTick & 0x200 != 0:
        ratio = (ratio * 0xF987A7253AC413176F2B074CF7815E54) >> 128
    if absTick & 0x400 != 0:
        ratio = (ratio * 0xF3392B0822B70005940C7A398E4B70F3) >> 128
    if absTick & 0x800 != 0:
        ratio = (ratio * 0xE7159475A2C29B7443B29C7FA6E889D9) >> 128
    if absTick & 0x1000 != 0:
        ratio = (ratio * 0xD097F3BDFD2022B8845AD8F792AA5825) >> 128
    if absTick & 0x2000 != 0:
        ratio = (ratio * 0xA9F746462D870FDF8A65DC1F90E061E5) >> 128
    if absTick & 0x4000 != 0:
        ratio = (ratio * 0x70D869A156D2A1B890BB3DF62BAF32F7) >> 128
    if absTick & 0x8000 != 0:
        ratio = (ratio * 0x31BE135F97D08FD981231505542FCFA6) >> 128
    if absTick & 0x10000 != 0:
        ratio = (ratio * 0x9AA508B5B7A84E1C677DE54F3E99BC9) >> 128
    if absTick & 0x20000 != 0:
        ratio = (ratio * 0x5D6AF8DEDB81196699C329225EE604) >> 128
    if absTick & 0x40000 != 0:
        ratio = (ratio * 0x2216E584F5FA1EA926041BEDFE98) >> 128
    if absTick & 0x80000 != 0:
        ratio = (ratio * 0x48A170391F7DC42444E8FA2) >> 128

    if tick > 0:
        ratio = MAX_UINT256 // ratio

    # this divides by 1<<32 rounding up to go from a Q128.128 to a Q128.96.
    # we then downcast because we know the result always fits within 160 bits due to our tick input constraint
    # we round up in the division so getTickAtSqrtRatio of the output price is always consistent

    remainder = 1 if ratio % 2**32 != 0 else 0
    # For some reason doing the division rounding up doesn't give the exact number
    result = (ratio >> 32) + remainder
    checkUInt160(result)
    return result


def getTickAtSqrtRatio(sqrtPriceX96) -> int:
    """Calculates the greatest tick value such that getRatioAtTick(tick) <= ratio

    Args:
        sqrtPriceX96 (int): The sqrt ratio for which to compute the tick as a Q64.96

    Returns:
        int: The greatest tick for which the ratio is less than or equal to the input ratio
    """
    checkUInt160(sqrtPriceX96)
    # second inequality must be < because the price can never reach the price at the max tick
    if sqrtPriceX96 >= MAX_SQRT_RATIO:
        raise ValueError("sqrtPriceX96 is too high")
    if sqrtPriceX96 < MIN_SQRT_RATIO:
        raise ValueError("sqrtPriceX96 is too low")
    ratio = sqrtPriceX96 << 32

    r = ratio
    msb = 0

    (r, msb) = add_bit_to_log_2(r, msb, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, 7)
    (r, msb) = add_bit_to_log_2(r, msb, 0xFFFFFFFFFFFFFFFF, 6)
    (r, msb) = add_bit_to_log_2(r, msb, 0xFFFFFFFF, 5)
    (r, msb) = add_bit_to_log_2(r, msb, 0xFFFF, 4)
    (r, msb) = add_bit_to_log_2(r, msb, 0xFF, 3)
    (r, msb) = add_bit_to_log_2(r, msb, 0xF, 2)
    (r, msb) = add_bit_to_log_2(r, msb, 0x3, 1)
    (r, msb) = add_bit_to_log_2(r, msb, 0x1, 0)

    r = ratio >> (msb - 127) if (msb >= 128) else ratio << (127 - msb)

    # Log2 is int in Solidity (not uint) - can be negative
    log_2 = (msb - 128) << 64

    (r, log_2) = add_fractional_bit(r, log_2, 63)
    (r, log_2) = add_fractional_bit(r, log_2, 62)
    (r, log_2) = add_fractional_bit(r, log_2, 61)
    (r, log_2) = add_fractional_bit(r, log_2, 60)
    (r, log_2) = add_fractional_bit(r, log_2, 59)
    (r, log_2) = add_fractional_bit(r, log_2, 58)
    (r, log_2) = add_fractional_bit(r, log_2, 57)
    (r, log_2) = add_fractional_bit(r, log_2, 56)
    (r, log_2) = add_fractional_bit(r, log_2, 55)
    (r, log_2) = add_fractional_bit(r, log_2, 54)
    (r, log_2) = add_fractional_bit(r, log_2, 53)
    (r, log_2) = add_fractional_bit(r, log_2, 52)
    (r, log_2) = add_fractional_bit(r, log_2, 51)
    (r, log_2) = add_fractional_bit(r, log_2, 50)

    log_sqrt10001 = log_2 * 255738958999603826347141
    # 128.128 number

    # TickLow and TickHi should be 24 bits long (int24)
    tickLow = (log_sqrt10001 - 3402992956809132418596140100660247210) >> 128
    tickHi = (log_sqrt10001 + 291339464771989622907027621153398088495) >> 128

    # Add checks to ensure that the tick is in the correct lenght (<= 24 bits)
    if tickLow < MIN_INT24:
        raise ValueError("TickLow is too low")
    if tickLow > MAX_INT24:
        raise ValueError("tickLow is too high")
    if tickHi < MIN_INT24:
        raise ValueError("tickHi is too low")
    if tickHi > MAX_INT24:
        raise ValueError("tickHi is too high")

    if tickLow == tickHi:
        tick = tickLow
    else:
        if getSqrtRatioAtTick(tickHi) <= sqrtPriceX96:
            tick = tickHi
        else:
            tick = tickLow

    return tick


def add_bit_to_log_2(r, msb, lower_bit_mask, bit):
    gt = 1 if r > lower_bit_mask else 0
    f = gt << bit
    msb = msb | f
    r = r >> f
    return (r, msb)


def add_fractional_bit(r, log_2, bit):
    r = (r * r) >> 127
    f = r >> 128
    log_2 = log_2 | (f << bit)
    # Difference in calculation when bit == 50
    if bit != 50:
        r = r >> f
    return (r, log_2)


## Price conversion functions
def sqrtPriceX96_to_price_float(
    sqrtPriceX96: int, token0_decimals: int, token1_decimals: int
) -> float:
    return ((sqrtPriceX96**2) / 2 ** (96 * 2)) * 10 ** (
        token0_decimals - token1_decimals
    )


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


# deprecated
class TickMath:
    """DEPRECATED
    TickMath is a library for computing the sqrt ratio at a given tick, and the tick corresponding to a given sqrt ratio
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
