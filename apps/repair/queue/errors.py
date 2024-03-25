import logging
from apps.feeds.static import _get_contract_creation_block
from bins.apis.etherscan_utilities import etherscan_helper
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, queueItemType
from bins.w3.builders import build_erc20_helper


def repair_token_queue_errors(
    chain: Chain, token_address: str, block_creation: int | None = None
):
    """Remove token prices and status rewards with blocks before token contract creation from queue

    Args:
        chain (Chain):
        token_address (str): token address
        block_creation (int): token contract creation block
    """

    items_to_remove = []

    # get erc20 token symbol
    ercHelper = build_erc20_helper(chain, token_address)
    token_symbol = ercHelper.symbol

    logging.getLogger(__name__).info(
        f" Repairing {chain.database_name} {token_symbol} token {token_address} queue errors."
    )

    contract_creation_data = _get_contract_creation_block(
        network=chain.database_name, contract_address=token_address
    )
    if contract_creation_data is None:
        logging.getLogger(__name__).error(
            f" {chain.database_name} {token_symbol} token {token_address} not found in the database."
        )
        if block_creation is None:
            #
            logging.getLogger(__name__).error(
                f"        Block creation not provided and can't get it from API."
            )
        else:
            # CAREFUL: forcing the use of the provided block_creation
            pass
    else:
        if block_creation:
            # check if different
            if block_creation != contract_creation_data["block"]:
                logging.getLogger(__name__).error(
                    f" {chain.database_name} {token_symbol} token {token_address} contract creation block [{contract_creation_data['block']}] is different from provided block_creation {block_creation}."
                )
                return

        block_creation = contract_creation_data["block"]
        logging.getLogger(__name__).debug(
            f" {chain.database_name} {token_symbol} token {token_address} contract creation block: {block_creation}"
        )

    if block_creation:
        # find all price addresses of the token, lower than block creation
        if queued_prices := get_from_localdb(
            network=chain.database_name,
            collection="queue",
            aggregate=[
                {
                    "$match": {
                        "type": queueItemType.PRICE,
                        "address": token_address,
                        "block": {"$lt": block_creation},
                    }
                },
                {"$sort": {"block": 1}},
            ],
        ):
            logging.getLogger(__name__).info(
                f"        Found {len(queued_prices)} price queued items for {token_symbol} token {token_address}. Should be removed."
            )
            items_to_remove += queued_prices

        # find queued objects related to the token price ( status rewards )
        if queued_reward_status := get_from_localdb(
            network=chain.database_name,
            collection="queue",
            aggregate=[
                {
                    "$match": {
                        "type": queueItemType.REWARD_STATUS,
                        "data.reward_static.rewardToken": token_address,
                        "block": {"$lt": block_creation},
                    }
                },
                {"$sort": {"block": 1}},
            ],
        ):

            logging.getLogger(__name__).info(
                f"        Found {len(queued_reward_status)} reward status queued items for {token_symbol} token {token_address}. Should be removed."
            )
            items_to_remove += queued_reward_status

    # check if the token is a hypervisor
    if hypervisor := get_from_localdb(
        network=chain.database_name,
        collection="static",
        find={"address": token_address},
    ):
        logging.getLogger(__name__).info(
            f"        Token {token_address} is a hypervisor. All its price type queued items will be removed from the queue."
        )
        items_to_remove += get_from_localdb(
            network=chain.database_name,
            collection="queue",
            find={"type": queueItemType.PRICE, "address": token_address},
        )

    if items_to_remove:

        # remove those items
        db_return = get_default_localdb(network=chain.database_name).delete_items(
            data=items_to_remove, collection_name="queue"
        )
        logging.getLogger(__name__).info(
            f"        Removed {db_return.deleted_count} items from the queue, from {len(items_to_remove)}."
        )
