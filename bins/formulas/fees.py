from bins.general.enums import Protocol


def calculate_gamma_fee(fee_rate: int, protocol: Protocol) -> float:
    """Calculate the gamma fee percentage over accrued fees by the positions

    Returns:
        float: gamma fee percentage
    """

    if protocol in [Protocol.CAMELOT, Protocol.RAMSES]:
        return fee_rate / 100
    else:
        return 1 / fee_rate if fee_rate < 100 else 1 / 10


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
    elif pool_protocol == Protocol.RAMSES:
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

    return protocol_fee_0, protocol_fee_1
