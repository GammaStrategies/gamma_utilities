# https://github.com/chainflip-io/chainflip-uniswapV3-python
import logging
from .checks import checkInputTypes, toUint256
from .constants import MAX_UINT256, MIN_INT256


def add(x, y, ui256: bool = False) -> int:
    """Adds two uint256 numbers and reverts or converts on overflow.

    Args:
        x (_type_):
        y (_type_):
        ui256 (bool, optional): If true, will convert to uint256 if overflow. Defaults to False.

    Returns:
        int
    """
    checkInputTypes(uint256=(x, y))
    z = x + y
    if z > MAX_UINT256:
        if ui256:
            logging.getLogger(__name__).debug(
                f" Addition overflow: {x} + {y} = {z} . Trying to convert to uint256."
            )
            return toUint256(z)
        else:
            raise ValueError("Addition overflow")
    return z


def sub(x, y, ui256: bool = False) -> int:
    """Subtracts two uint256 numbers and reverts or converts on underflow.

    Args:
        x (_type_): _description_
        y (_type_): _description_
        ui256 (bool, optional):  If true, will convert to uint256 if underflow . Defaults to False.

    Raises:
        ValueError: underflow

    Returns:
        int:
    """
    checkInputTypes(uint256=(x, y))
    z = x - y
    if z < 0:
        if ui256:
            logging.getLogger(__name__).debug(
                f" Subtraction underflow: {x} - {y} = {z} . Trying to convert to uint256."
            )
            return toUint256(z)
        else:
            raise ValueError("Subtraction underflow")

    return z


def mul(x, y) -> int:
    checkInputTypes(uint256=(x, y))
    z = x * y
    if z > MAX_UINT256:
        raise ValueError("Multiplication overflow")
    return z


def addInts(x, y):
    checkInputTypes(int256=(x, y))
    z = x + y
    if z < MIN_INT256 or z > MAX_UINT256:
        raise ValueError("addInts overflow")
    return z


def subInts(x, y):
    checkInputTypes(int256=(x, y))
    z = x - y
    if z < MIN_INT256 or z > MAX_UINT256:
        raise ValueError("subInts underflow")
    return z
