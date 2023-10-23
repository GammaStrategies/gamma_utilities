# # https://github.com/chainflip-io/chainflip-uniswapV3-python
from .checks import checkUInt256


def mulDiv(a, b, c) -> int:
    """Calculates floor(a×b÷denominator) with full precision. Throws if result overflows a uint256 or denominator == 0

    Args:
        a (_type_): multiplicand
        b (_type_): multiplier
        c (_type_): divisor

    Returns:
        256-bit result
    """
    result = (a * b) // c
    try:
        checkUInt256(result)
        return result
    except Exception:
        raise


def mulDivRoundingUp(a, b, c) -> int:
    """Calculates ceil(a×b÷denominator) with full precision. Throws if result overflows a uint256 or denominator == 0

    Args:
        a (_type_): multiplicand
        b (_type_): multiplier
        c (_type_): divisor

    Returns:
        256-bit result
    """
    return divRoundingUp(a * b, c)


def divRoundingUp(a, b) -> int:
    """Calculates ceil(a÷denominator) with full precision rounding up. Throws if result overflows a uint256 or denominator == 0

    Args:
        a (_type_): multiplicand
        b (_type_): divisor

    Returns:
        256-bit result
    """
    result = a // b
    if a % b > 0:
        result += 1
    try:
        checkUInt256(result)
        return result
    except Exception:
        raise
