import base58
import binascii


def ipfscidv0_to_byte32(cid):
    """
    Convert ipfscidv0 to 32 bytes hex string.
    https://github.com/emg110/

    Args:
        cid (string): IPFS CID Version 0

    Returns:
        str: 32 Bytes long string
    """
    """bytes32 is converted back into Ipfs hash format."""

    decoded = base58.b58decode(cid)
    sliced_decoded = decoded[2:]
    return binascii.b2a_hex(sliced_decoded).decode("utf-8")


def byte32_to_ipfscidv0(hexstr):
    """
    Convert 32 bytes hex string to ipfscidv0.
    https://github.com/emg110/

    Args:
        hexstr (string): 32 Bytes long string

    Returns:
        str: IPFS CID Version 0
    """

    binary_str = binascii.a2b_hex(hexstr)
    completed_binary_str = b"\x12 " + binary_str
    return base58.b58encode(completed_binary_str).decode("utf-8")
