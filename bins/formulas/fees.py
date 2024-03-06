import logging

from bins.formulas.constants import X128
from bins.formulas.full_math import mulDiv
from bins.formulas.safe_math import sub
from bins.general.enums import Protocol


# gamma fee
def calculate_gamma_fee(fee_rate: int, protocol: Protocol) -> int:
    """Calculate the gamma fee percentage over accrued fees by the positions

    Returns:
        int: gamma fee percentage as int
    """

    if protocol in [
        Protocol.CAMELOT,
        Protocol.RAMSES,
        Protocol.CLEOPATRA,
        Protocol.PHARAOH,
    ]:
        return fee_rate
    else:
        return int((1 / fee_rate) * 100) if fee_rate < 100 else 10


# non gamma protocol fee  #
def convert_feeProtocol(
    feeProtocol0: int,
    feeProtocol1: int,
    hypervisor_protocol: Protocol,
    pool_protocol: Protocol,
) -> tuple[int, int]:
    """Convert the <feeProtocol> field values from the contract to a 1 to 100 range format

    Args:
        feeProtocol0 (int):
        feeProtocol1 (int):
        hypervisor_protocol (Protocol):
        pool_protocol (Protocol):

    Returns:
        tuple[int, int]: feeProtocol0, feeProtocol1 in 1 to 100 format
    """

    if pool_protocol in [
        Protocol.ALGEBRAv3,
        Protocol.THENA,
        Protocol.ZYBERSWAP,
    ]:
        # factory
        # https://vscode.blockscan.com/bsc/0x1b9a1120a17617D8eC4dC80B921A9A1C50Caef7d
        protocol_fee_0 = (feeProtocol0 / 10) // 1
        protocol_fee_1 = (feeProtocol1 / 10) // 1
    elif pool_protocol == Protocol.CAMELOT:
        # factory
        # https://vscode.blockscan.com/arbitrum-one/0x521aa84ab3fcc4c05cabac24dc3682339887b126
        protocol_fee_0 = (feeProtocol0 / 10) // 1
        protocol_fee_1 = (feeProtocol1 / 10) // 1
    elif pool_protocol in [Protocol.RAMSES, Protocol.CLEOPATRA, Protocol.PHARAOH]:
        # factory
        # https://vscode.blockscan.com/arbitrum-one/0x2d846d6f447185590c7c2eddf5f66e95949e0c66
        protocol_fee_0 = (feeProtocol0 * 5 + 50) // 1
        protocol_fee_1 = (feeProtocol1 * 5 + 50) // 1
    elif hypervisor_protocol == Protocol.RETRO:
        # factory
        # https://vscode.blockscan.com/polygon/0x91e1b99072f238352f59e58de875691e20dc19c1
        protocol_fee_0 = ((100 * feeProtocol0) / 15) // 1
        protocol_fee_1 = ((100 * feeProtocol1) / 15) // 1
    elif hypervisor_protocol == Protocol.SUSHI:
        # factory
        # https://vscode.blockscan.com/arbitrum-one/0xD781F2cdaf16eB422e99C4E455F071F0BB20cf1a
        protocol_fee_0 = (100 / feeProtocol0) // 1 if feeProtocol0 else 0
        protocol_fee_1 = (100 / feeProtocol1) // 1 if feeProtocol1 else 0
    else:
        # https://vscode.blockscan.com/arbitrum-one/0xD781F2cdaf16eB422e99C4E455F071F0BB20cf1a
        protocol_fee_0 = (100 / feeProtocol0) // 1 if feeProtocol0 else 0
        protocol_fee_1 = (100 / feeProtocol1) // 1 if feeProtocol1 else 0

    # should not happen
    if (
        protocol_fee_0
        and not isinstance(protocol_fee_0, int)
        and not protocol_fee_0.is_integer()
    ):
        logging.getLogger(__name__).warning(
            f" convert_feeProtocol protocol_fee_0 {protocol_fee_0} is not integer ! ( will be converted though))"
        )
    if (
        protocol_fee_1
        and not isinstance(protocol_fee_1, int)
        and not protocol_fee_1.is_integer()
    ):
        logging.getLogger(__name__).warning(
            f" convert_feeProtocol protocol_fee_1 {protocol_fee_1} is not integer ! ( will be converted though)"
        )

    return int(protocol_fee_0), int(protocol_fee_1)


# fee growth
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
        return sub(feeGrowthGlobal, feeGrowthOutsideLower, True)


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
        return sub(feeGrowthGlobal, feeGrowthOutsideUpper, True)
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
       fees ever minus the fees above and below the positions range at time ( feeGrowthInside )
    """
    return sub(
        sub(
            feeGrowthGlobal,
            fees_collected_below_tickLower(
                tick, tickLower, feeGrowthGlobal, feeGrowthOutsideLower
            ),
            True,
        ),
        fees_collected_above_tickUpper(
            tick, tickUpper, feeGrowthGlobal, feeGrowthOutsideUpper
        ),
        True,
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

    return mulDiv(
        liquidity,
        sub(
            fees_returns_at_timeT(
                tick=tick,
                tickUpper=tickUpper,
                tickLower=tickLower,
                feeGrowthGlobal=feeGrowthGlobal,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper,
                feeGrowthOutsideLower=feeGrowthOutsideLower,
            ),
            feeGrowthInsideLast,
            True,
        ),
        X128,
    )


def feeGrowth_to_fee(feeGrowthX128: int, liquidity: int) -> int:
    """feeGrowth to fee

    Args:
       feeGrowthX128 (int): feeGrowth
       liquidity (int): liquidity

    Returns:
       fee
    """
    return mulDiv(liquidity, feeGrowthX128, X128)


#
