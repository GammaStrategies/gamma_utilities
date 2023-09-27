import logging
from bins.database.helpers import get_default_globaldb
from bins.general.enums import Chain
from bins.w3.builders import build_erc20_helper


def check_is_token(chain: Chain, address: str) -> bool:
    """Check if an address is a token
        May return false positive when no RPC is available for the chain
    Args:
        chain (Chain):
        address (str):

    Returns:
        bool: is a token or not
    """

    # get any database price for this specific address
    address = address.lower()
    find = {"address": address, "network": chain.database_name}
    if get_default_globaldb().get_items_from_database(
        collection_name="usd_prices", find=find, limit=1
    ):
        # found prices for this address already in the database
        return True

    # check if this address is a token
    try:
        return build_erc20_helper(chain=chain, address=address).isContract()
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Can't check if address is token. Error: {e}"
        )

    return False
