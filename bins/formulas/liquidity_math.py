from .full_math import mulDiv
from .constants import MAX_UINT128, X96, FixedPoint96_Q96, FixedPoint96_RESOLUTION


def addDelta(x, y) -> int:
    """Add a signed liquidity delta to liquidity and revert if it overflows or underflows

    Args:
        x (int): The liquidity before change
        y (int): The delta by which liquidity should be changed

    Returns:
        int: The liquidity delta
    """
    if y < 0:
        z = x - abs(y)
        if z < 0:
            #  solidity underflow
            raise ValueError("Liquidity cannot go negative")
    else:
        z = x + abs(y)
        if z > MAX_UINT128:
            # solidity overflow
            raise ValueError("Liquidity cannot go above 2^128 - 1")

    return z


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

    intermediate = mulDiv(RA, RB, FixedPoint96_Q96)
    # toUint128( )
    return mulDiv(amount0, intermediate, RB - RA)


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

    # toUint128( )
    return mulDiv(amount1, FixedPoint96_Q96, RB - RA)


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
        liquidity = getLiquidityForAmount0(sqrtRatioAX96, sqrtRatioBX96, amount0)
    elif sqrtRatioX96 < sqrtRatioBX96:
        liquidity0 = getLiquidityForAmount0(sqrtRatioX96, sqrtRatioBX96, amount0)
        liquidity1 = getLiquidityForAmount1(sqrtRatioAX96, sqrtRatioX96, amount1)
        # decide
        liquidity = liquidity0 if (liquidity0 < liquidity1) else liquidity1

    else:
        liquidity = getLiquidityForAmount1(sqrtRatioAX96, sqrtRatioBX96, amount1)

    # result
    return liquidity


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

    return (
        mulDiv(
            (liquidity << FixedPoint96_RESOLUTION),
            RB - RA,
            RB,
        )
        // RA
    )


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

    return mulDiv(liquidity, RB - RA, FixedPoint96_Q96)


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
        amount0 = getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)
    elif sqrtRatioX96 < sqrtRatioBX96:
        amount0 = getAmount0ForLiquidity(sqrtRatioX96, sqrtRatioBX96, liquidity)
        amount1 = getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioX96, liquidity)
    else:
        amount1 = getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)

    return amount0, amount1


# Deprecated
class LiquidityAmounts:
    """DEPRECATED"""

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

        return int((((liquidity << FixedPoint96_RESOLUTION) * (RB - RA)) / RB) / RA)

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
