from decimal import Decimal
import logging
from apps.feeds.queue.queue_item import QueueItem
from bins.configuration import STATIC_REGISTRY_ADDRESSES
from bins.database.common.database_ids import create_id_operation
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)
from bins.general.enums import Chain, text_to_chain
from bins.w3.builders import build_erc20_helper


def pull_from_queue_user_operation(network: str, queue_item: QueueItem) -> bool:
    """User operations represent the deposit and withdrawal of tokens from a hypervisor. This function processes the user operations queue items.
        A deposit operation can be a transfer in and a withdrawal operation can be a transfer out.

        --> user address, hype address, block and timestamp, LP token qtty, token0 qtty and token1 qtty ( when depositing or withdrawing).

    Args:
        network (str):
        queue_item (QueueItem):

    """
    # 2 methods: API and web3 calls.
    # identify if operation is proxied or not

    logging.getLogger(__name__).debug(
        f"  -> Processing {network}'s user operation queue id {queue_item.id}"
    )

    try:
        if user_operation := _build_user_operation_from_queue_item(
            network=network, queue_item=queue_item
        ):

            # save user operation to database
            if db_return := get_default_localdb(network).replace_item_to_database(
                data=user_operation, collection_name="user_operations"
            ):
                if db_return.upserted_id:
                    logging.getLogger(__name__).debug(
                        f"  <- Done processing {network}'s user operation queue item {queue_item.id}. Saved successfully"
                    )
                elif db_return.modified_count:
                    logging.getLogger(__name__).debug(
                        f"  <- Done processing {network}'s user operation queue item {queue_item.id}. Modified successfully"
                    )
                else:
                    logging.getLogger(__name__).debug(
                        f"  <- Done processing {network}'s user operation queue item {queue_item.id}. Database returned {db_return.raw_result}"
                    )
        else:
            logging.getLogger(__name__).debug(
                f"  <- Done processing {network}'s user operation queue item {queue_item.id}. No user operation created"
            )

        # return result
        return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s user operation queue item: {e}"
        )

    # return result
    return False


###
### operations_subtopics = ["deposit","withdraw","stake", "unstake", "transfer"]
###


def _build_user_operation_from_queue_item(network: str, queue_item: QueueItem):

    try:
        user_operation = None
        hypervisor_address = queue_item.data["address"]

        # check if the hype address exists in the database
        hypervisor_static = get_from_localdb(
            network=network, collection="static", find={"address": hypervisor_address}
        )
        if not hypervisor_static:
            logging.getLogger(__name__).error(
                f"  <- Done processing {network}'s user operation queue item {queue_item.id}. Hypervisor {hypervisor_address} is not in the database"
            )
            return None
        hypervisor_static = hypervisor_static[0]

        if queue_item.data["topic"] == "transfer":
            user_operation = _build_user_operation_from_transfer(
                network=network,
                operation=queue_item.data,
                hypervisor_static=hypervisor_static,
            )
        elif queue_item.data["topic"] == "withdraw":
            user_operation = _build_user_operation_from_withdraw(
                network=network,
                operation=queue_item.data,
                hypervisor_static=hypervisor_static,
            )
        elif queue_item.data["topic"] == "deposit":
            user_operation = _build_user_operation_from_deposit(
                network=network,
                operation=queue_item.data,
                hypervisor_static=hypervisor_static,
            )
        else:
            raise ValueError(f"Unknown operation topic {queue_item.data['topic']}")

        return user_operation

    except Exception as e:
        raise e


def _build_user_operation_from_transfer(
    network: str, operation: dict, hypervisor_static: dict
) -> dict | None:
    """Build a user operation database object from a LP token transfer operation.

    Args:
        operation (dict): operation data
        network (str): network

    Returns:
        dict: operation database object
    """
    # transfer operations are not proxied
    if operation["topic"] != "transfer":
        raise ValueError(f"Not a transfer operation id: {operation['id']}")

    # user can transfer:
    #    from spNFT/Zyberchef/Masterchef to wallet ( unstake )
    #
    #    from wallet to spNFT/Zyberchef/Masterchef ( stake )
    #
    #    from wallet to wallet ( transfer )

    # get all the known rewarder addresses for the network
    known_rewarder_addresses = _get_all_known_rewarder_addresses(
        network=network, hypervisor_address=operation["address"]
    )

    if not known_rewarder_addresses:
        logging.getLogger(__name__).debug(
            f" -> No rewarders found for {network} {operation['address']} hypervisor"
        )
    # get all known proxy ( helpers) addresses for the network
    proxy_addresses = STATIC_REGISTRY_ADDRESSES.get(network, {}).get(
        "deposit_proxies", []
    )

    # return nothing if this is a LP mint/burn operation ( its handled by the withdraw/deposit )
    if operation["src"].lower() == "0x0000000000000000000000000000000000000000":
        # this is a mint operation ( deposit)
        logging.getLogger(__name__).debug(
            f"  -> Do nothing with a LP mint operation {operation['id']}"
        )
        return None
    elif operation["dst"].lower() == "0x0000000000000000000000000000000000000000":
        # this is a burn operation ( withdraw)
        logging.getLogger(__name__).debug(
            f"  -> Do nothing with a LP burn operation {operation['id']}"
        )
        return None

    # is staking/unstaking or transfering to another wallet
    if operation["src"].lower() in known_rewarder_addresses:
        # dst may be a proxy or a user wallet
        if operation["dst"].lower() in proxy_addresses:
            # proxied transfer-> from rewarder to proxy address
            logging.getLogger(__name__).debug(
                f"  -> Do nothing with a transfer from rewarder to a proxy  {operation['id']} rewarder: {operation['src']} proxy: {operation['dst']}"
            )
            return None
        else:
            # dst is the user wallet and its a unstaking operation
            user_addresses = [operation["dst"].lower()]
            _op_subtopic = "unstake"

    elif operation["dst"].lower() in known_rewarder_addresses:
        # src may be a proxy or a user wallet
        if operation["src"].lower() in proxy_addresses:
            # proxied transfer-> from proxy address to rewarder
            logging.getLogger(__name__).debug(
                f"  -> Do nothing with a transfer from a proxy to rewarder(NFT)  {operation['id']} proxy: {operation['src']} rewarder: {operation['dst']}"
            )
            return None
        else:
            # src is the user wallet and its a staking operation
            user_addresses = [operation["src"].lower()]
            _op_subtopic = "stake"

    else:

        # src may be a proxy or a user wallet
        if operation["src"].lower() in proxy_addresses:
            # proxied transfer-> from proxy address to spNFT or any staking address
            # all user proxied transfers are handled by the deposit proxy function
            logging.getLogger(__name__).debug(
                f" -> Do nothing with a transfer from a proxy to NFT(rewarder)  {operation['id']} proxy: {operation['src']} NFT: {operation['dst']}"
            )
            return None

        elif operation["dst"].lower() in proxy_addresses:
            # proxied transfer-> from user wallet to proxy address
            raise ValueError(
                f"Proxied transfer from user wallet to proxy address {operation['id']}"
            )

        else:
            # transfer between wallets
            user_addresses = [operation["src"].lower(), operation["dst"].lower()]
            _op_subtopic = "transfer"

    # check if sender user has this many LP tokens to send ( database perspective )
    # important because not always the rewarder is identified ( spNFT in this case).
    # This is a universal hard check for all cases.
    # Just because we need the balance before current state, we subtract one to the logIndex ( bc query is lte)
    if operation["logIndex"] == 0:
        # set log index to a number high enough to make sure it will be the last logIndex and subtract one from the block
        current_user_shares = _get_user_shares(
            network=network,
            user_address=operation["src"].lower(),
            block=operation["blockNumber"] - 1,
            logIndex=999999999,
            hypervisor_address=operation["address"],
        )
    else:
        current_user_shares = _get_user_shares(
            network=network,
            user_address=operation["src"].lower(),
            block=operation["blockNumber"],
            logIndex=operation["logIndex"] - 1,
            hypervisor_address=operation["address"],
        )
    if Decimal(operation["qtty"]) > current_user_shares:
        # May be an untracked rewarder address
        # spNFTs are identified by either the sender ( being a proxy) or the receiver ( being a rewarder in the database ).
        logging.getLogger(__name__).error(
            f"  -> User {operation['src']} has not enough shares [{current_user_shares}] to send {operation['qtty']} at block {operation['blockNumber']}. Skipping operation {operation['id']}"
        )
        return None
    #

    #  create user operation
    user_operation = {
        "id": create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        ),
        "blockNumber": operation["blockNumber"],
        "transactionHash": operation["transactionHash"],
        "logIndex": operation["logIndex"],
        "timestamp": operation["timestamp"],
        "hypervisor": {
            "address": operation["address"].lower(),
            "symbol": hypervisor_static["symbol"],
            "dex": hypervisor_static["dex"],
            "decimals": hypervisor_static["decimals"],
            "pool_address": hypervisor_static["pool"]["address"],
            "token0_address": hypervisor_static["pool"]["token0"]["address"],
            "token1_address": hypervisor_static["pool"]["token1"]["address"],
            "token0_symbol": hypervisor_static["pool"]["token0"]["symbol"],
            "token1_symbol": hypervisor_static["pool"]["token1"]["symbol"],
            "token0_decimals": hypervisor_static["pool"]["token0"]["decimals"],
            "token1_decimals": hypervisor_static["pool"]["token1"]["decimals"],
        },
        "user_addresses": user_addresses,
        "shares": operation["qtty"],
        "sender": operation["src"].lower(),  # <-- user wallet depending on the topic
        "to": operation["dst"].lower(),  # <-- user wallet depending on the topic
        "topic": _op_subtopic,
    }
    return user_operation


def _build_user_operation_from_withdraw(
    network: str, operation: dict, hypervisor_static: dict
) -> dict:
    """Build a user operation database object from a LP token withdraw operation.

    Args:
        operation (dict): operation data
        network (str): network

    Returns:
        dict: operation database object
    """
    # withdraw operations are not proxied ( at least yet )
    if operation["topic"] != "withdraw":
        raise ValueError(f"Not a withdraw operation id: {operation['id']}")

    user_address = operation["to"].lower()  # the 'to' field also

    # TODO: check if this user has this many shares in the hypervisor ( database read )

    #  create user operation
    user_operation = {
        "id": create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        ),
        "blockNumber": operation["blockNumber"],
        "transactionHash": operation["transactionHash"],
        "logIndex": operation["logIndex"],
        "timestamp": operation["timestamp"],
        "hypervisor": {
            "address": operation["address"].lower(),
            "symbol": hypervisor_static["symbol"],
            "dex": hypervisor_static["dex"],
            "decimals": hypervisor_static["decimals"],
            "pool_address": hypervisor_static["pool"]["address"],
            "token0_address": hypervisor_static["pool"]["token0"]["address"],
            "token1_address": hypervisor_static["pool"]["token1"]["address"],
            "token0_symbol": hypervisor_static["pool"]["token0"]["symbol"],
            "token1_symbol": hypervisor_static["pool"]["token1"]["symbol"],
            "token0_decimals": hypervisor_static["pool"]["token0"]["decimals"],
            "token1_decimals": hypervisor_static["pool"]["token1"]["decimals"],
        },
        "user_addresses": [user_address],
        "shares": operation["shares"],
        "sender": operation["sender"].lower(),
        "to": user_address,  # <-- user wallet
        "topic": operation["topic"],
        "qtty_token0": operation["qtty_token0"],
        "qtty_token1": operation["qtty_token1"],
    }
    return user_operation


def _build_user_operation_from_deposit(
    network: str, operation: dict, hypervisor_static: dict
) -> dict:
    """Build a user operation database object from a LP token deposit operation.

    Args:
        operation (dict): operation data
        network (str): network
        hypervisor_static (dict): hypervisor static data

    Returns:
        dict: operation database object
    """
    # deposit operations may be proxied
    if operation["topic"] != "deposit":
        raise ValueError(f"Not a deposit operation id: {operation['id']}")

    user_address = ""

    # get all known proxy ( helpers) addresses for the network
    proxy_addresses = STATIC_REGISTRY_ADDRESSES.get(network, {}).get(
        "deposit_proxies", []
    )

    if operation["sender"].lower() in proxy_addresses:
        # this is a proxied deposit
        user_address = _get_user_address_from_proxied_deposit(
            network=network,
            operation=operation,
            proxy_addresses=proxy_addresses,
            hypervisor_static=hypervisor_static,
        )
    else:
        # this is a direct deposit
        user_address = operation["sender"].lower()

    if user_address in [None, ""]:
        raise ValueError(
            f"Could not find user address for proxied deposit id: {operation['id']}"
        )

    #  create user operation
    user_operation = {
        "id": create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        ),
        "blockNumber": operation["blockNumber"],
        "transactionHash": operation["transactionHash"],
        "logIndex": operation["logIndex"],
        "timestamp": operation["timestamp"],
        "hypervisor": {
            "address": operation["address"].lower(),
            "symbol": hypervisor_static["symbol"],
            "dex": hypervisor_static["dex"],
            "decimals": hypervisor_static["decimals"],
            "pool_address": hypervisor_static["pool"]["address"],
            "token0_address": hypervisor_static["pool"]["token0"]["address"],
            "token1_address": hypervisor_static["pool"]["token1"]["address"],
            "token0_symbol": hypervisor_static["pool"]["token0"]["symbol"],
            "token1_symbol": hypervisor_static["pool"]["token1"]["symbol"],
            "token0_decimals": hypervisor_static["pool"]["token0"]["decimals"],
            "token1_decimals": hypervisor_static["pool"]["token1"]["decimals"],
        },
        "user_addresses": [user_address],
        "shares": operation["shares"],
        "sender": user_address,  # <-- user wallet
        "to": operation["to"].lower(),
        "topic": operation["topic"],
        "qtty_token0": operation["qtty_token0"],
        "qtty_token1": operation["qtty_token1"],
    }
    return user_operation


# HELPERS


def _get_user_address_from_proxied_deposit(
    network: str, operation: dict, proxy_addresses: list, hypervisor_static: dict
) -> str | None:
    """Find the user address from a proxied deposit operation."""

    if operation["topic"] != "deposit":
        raise ValueError(f"Not a deposit operation id: {operation['id']}")

    # easy to access vars
    _hypervisor_address = operation["address"].lower()
    _token0_address = hypervisor_static["pool"]["token0"]["address"].lower()
    _token1_address = hypervisor_static["pool"]["token1"]["address"].lower()
    _tx_token0_qtty = int(operation["qtty_token0"])
    _tx_token1_qtty = int(operation["qtty_token1"])

    # get the final user by placing web3 calls to the network
    # read all the transfer events for the transaction
    ercHelper = build_erc20_helper(chain=text_to_chain(network))
    receipt = ercHelper._getTransactionReceipt(operation["transactionHash"])
    decoded_logs = ercHelper.contract.events.Transfer().processReceipt(receipt)

    found_addresses = []
    # find hypervisor address deposit (to)
    for log in decoded_logs:
        _contract_address = log.address.lower()
        _current_qtty = int(log.args["value"])
        _current_from = log.args["from"].lower()
        _current_to = log.args["to"].lower()
        # make sure this is not a transfer to the hypervisor nor a transfer from 0x0000
        if (
            _contract_address in [_token0_address, _token1_address]
            and _current_to in proxy_addresses
            and _contract_address != _hypervisor_address
        ):
            if _tx_token0_qtty == _current_qtty:
                # this is the one
                found_addresses.append(_current_from)
            elif _tx_token1_qtty == _current_qtty:
                # this is the one
                found_addresses.append(_current_from)
            else:
                # this is not the one
                pass

    if not found_addresses:
        logging.getLogger(__name__).error(
            f" ERROR !!   No user addresses found for proxied deposit id: {operation['id']} at block: {operation['blockNumber']}"
        )
        return None

    # compare found user addresses with the deposit event information qtties ( should match )
    if len(found_addresses) != 2:
        logging.getLogger(__name__).error(
            f" ERROR !!   Found {len(found_addresses)} user addresses from a proxied deposit id: {operation['id']} at block: {operation['blockNumber']}"
        )
        return None

    # set easy to access vars ( user address should be the same)
    _user_address = found_addresses[0]

    # check if they are equal
    if found_addresses[0] != found_addresses[1]:
        # not equal! check if one of them is minted
        if "0x0000000000000000000000000000000000000000" in found_addresses:
            # this may be a wrapped eth transaction, or another token wraped by the user directly to proxy contract.
            # because it matches qtty, we assume it is correct and use the other address as user address.
            logging.getLogger(__name__).debug(
                f" Found wrapped token while getting user address from proxied deposit id {operation['id']}. Continue as normal."
            )
            # select the other address
            return [
                x.lower()
                for x in found_addresses
                if x != "0x0000000000000000000000000000000000000000"
            ][0]
        else:
            logging.getLogger(__name__).error(
                f" ERROR !!   Found TWO different user addresses from a proxied deposit id: {operation['id']} at block: {operation['blockNumber']} -> {found_addresses}"
            )
            return None
    else:
        # they are equal
        return _user_address


def _get_all_known_rewarder_addresses(
    network: str, hypervisor_address: str | None = None
) -> list:
    """Get all known rewarder addresses for a network, those being Masterchef, Zyberchef, etc... and spNFT, nitro addresses.
       Known addresses where the LP token can be staked.

    Args:
        network (str): network

    Returns:
        list: list of known staking addresses
    """

    # get all the staking addresses for the network
    staking_addresses = []

    find = {} if not hypervisor_address else {"hypervisor_address": hypervisor_address}
    for static_rewarder in get_from_localdb(
        network=network, collection="rewards_static", find=find
    ):
        staking_addresses.append(static_rewarder["rewarder_address"].lower())
        # add the registry address just in case ( should not affect anything)
        staking_addresses.append(static_rewarder["rewarder_registry"].lower())

    return staking_addresses


def _get_user_shares(
    network: str, user_address: str, block: int, logIndex: int, hypervisor_address: str
) -> int:

    # build a fast query to get the user shares at the specified block
    _query = [
        {
            "$match": {
                "$and": [
                    {
                        "$or": [
                            {"sender": user_address},
                            {"to": user_address},
                        ]
                    },
                    {
                        "$or": [
                            {"blockNumber": {"$lt": block}},
                            {
                                "$and": [
                                    {"blockNumber": block},
                                    {"logIndex": {"$lte": logIndex}},
                                ]
                            },
                        ]
                    },
                    {"hypervisor.address": hypervisor_address},
                ]
            }
        },
        {
            "$project": {
                "_id": 0,
                "hypervisor_address": "$hypervisor.address",
                "user_shares": {
                    "$ifNull": [
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$topic", "deposit"]},
                                        {
                                            "$and": [
                                                {"$eq": ["$topic", "transfer"]},
                                                {
                                                    "$eq": [
                                                        "$to",
                                                        user_address,
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                },
                                {"$toDecimal": {"$ifNull": ["$qtty", "$shares"]}},
                                {
                                    "$cond": [
                                        {
                                            "$or": [
                                                {"$eq": ["$topic", "withdraw"]},
                                                {
                                                    "$and": [
                                                        {"$eq": ["$topic", "transfer"]},
                                                        {
                                                            "$eq": [
                                                                "$sender",
                                                                user_address,
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                        {
                                            "$multiply": [
                                                {
                                                    "$toDecimal": {
                                                        "$ifNull": ["$qtty", "$shares"]
                                                    }
                                                },
                                                -1,
                                            ]
                                        },
                                        0,
                                    ]
                                },
                            ]
                        },
                        0,
                    ]
                },
            }
        },
        {
            "$group": {
                "_id": "$hypervisor_address",
                "user_shares": {"$sum": "$user_shares"},
            }
        },
    ]

    user_shares = get_from_localdb(
        network=network, collection="user_operations", aggregate=_query
    )
    if len(user_shares) > 1:
        logging.getLogger(__name__).error(
            f"  -> More than one user shares found for {user_address} at block {block} in hypervisor {hypervisor_address}"
        )

    if not user_shares:
        return 0

    # convert to decimal n return
    user_shares = get_default_globaldb().convert_d128_to_decimal(user_shares[0])
    return user_shares["user_shares"]
