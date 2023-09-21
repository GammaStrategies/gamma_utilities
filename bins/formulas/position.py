from web3 import Web3


# uniswap v3 position key
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
