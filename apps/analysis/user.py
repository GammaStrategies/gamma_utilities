from datetime import datetime, timedelta, timezone
import json
import logging
import os

import tqdm
from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_report
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)

from bins.general.enums import Chain, reportType, text_to_chain
from bins.general.exports import telegram_send_file
from bins.w3.builders import build_erc20_helper


# Traking LP tokens by final user address
# Currently only supporting Arbitrum galxe
# TODO: universalize this function to support all chains. Include API method to get final user address
# be aware that nft transfers containing LP tokens will not be tracked.
def analyze_user_transactions(
    chains: list[Chain] | None = None,
    hypervisor_addresses: list[str] | None = None,
    user_addresses: list[str] | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    send_to_telegram: bool = False,
    save_to_db: bool = True,
) -> dict:
    if user_addresses:
        raise NotImplementedError("user_addresses not implemented yet")

    addresses = hypervisor_addresses or user_addresses or None

    if not ini_timestamp and CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime:
        ini_timestamp = CONFIGURATION["_custom_"][
            "cml_parameters"
        ].ini_datetime.timestamp()

    if not end_timestamp and CONFIGURATION["_custom_"]["cml_parameters"].end_datetime:
        end_timestamp = CONFIGURATION["_custom_"][
            "cml_parameters"
        ].end_datetime.timestamp()

    # control vars to define if we should set timestamps when no value is provided
    _set_ini_timestamp = True if not ini_timestamp else False
    _set_end_timestamp = True if not end_timestamp else False

    # set known proxy addresses
    proxy_addresses = {
        Chain.ARBITRUM: ["0x851b3Fb3c3178Cd3FBAa0CdaAe0175Efa15a30f1".lower()]
    }

    # set chains to process
    chains = (
        chains
        or [
            text_to_chain(x)
            for x in (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
            )
        ]
        or list(Chain)
    )

    # set result var
    result = {}

    # analyze
    for chain in chains:
        logging.getLogger(__name__).info(
            f" Analyzing user deposits in {chain.fantasy_name}"
        )
        # get static hypervisor data
        hypervisors_static_list = {
            x["address"]: x
            for x in get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"address": {"$in": addresses}} if addresses else {},
                batch_size=10000,
                projection={"_id": 0, "address": 1, "pool": 1, "symbol": 1, "dex": 1},
            )
        }

        # get all deposits
        _and = [{"topic": {"$in": ["deposit", "withdraw"]}}]
        find = {
            "$and": [
                {"topic": {"$in": ["deposit", "withdraw"]}},
            ],
        }
        if addresses:
            find["$and"].append({"address": {"$in": addresses}})
            _and.append({"address": {"$in": addresses}})
        if ini_timestamp:
            find["$and"].append({"timestamp": {"$gte": ini_timestamp}})
            _and.append({"timestamp": {"$gte": ini_timestamp}})
        if end_timestamp:
            find["$and"].append({"timestamp": {"$lte": end_timestamp}})
            _and.append({"timestamp": {"$lte": end_timestamp}})
        query = [
            {"$match": {"$and": _and}},
            {
                "$project": {
                    "_id": 0,
                    "address": 1,
                    "sender": 1,
                    "to": 1,
                    "blockNumber": 1,
                    "transactionHash": 1,
                    "timestamp": 1,
                    "qtty_token0": 1,
                    "qtty_token1": 1,
                    "topic": 1,
                }
            },
            {
                "$lookup": {
                    "from": "static",
                    "let": {"op_address": "$address"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$address", "$$op_address"]}}},
                        {"$limit": 1},
                        {
                            "$project": {
                                "address": "$address",
                                "symbol": "$symbol",
                                "pool": {
                                    "address": "$pool.address",
                                    "token0": "$pool.token0.address",
                                    "token1": "$pool.token1.address",
                                    "dex": "$pool.dex",
                                },
                                "dex": "$dex",
                            }
                        },
                        {"$unset": ["_id"]},
                    ],
                    "as": "static",
                }
            },
            {"$unwind": {"path": "$static", "preserveNullAndEmptyArrays": True}},
        ]
        # get operations from database
        database_operations = get_from_localdb(
            network=chain.database_name,
            collection="operations",
            aggregate=query,
            batch_size=10000,
        )

        # get max min block to gather prices
        max_block = database_operations[-1]["blockNumber"]
        min_block = database_operations[0]["blockNumber"]
        for t in database_operations:
            max_block = max(max_block, t["blockNumber"])
            min_block = min(min_block, t["blockNumber"])

        # get and build token prices
        prices_list = {}
        for price in get_default_globaldb().get_items_from_database(
            collection_name="usd_prices",
            find={
                "network": chain.database_name,
                "block": {"$gte": min_block, "$lte": max_block},
            },
        ):
            if price["address"] not in prices_list:
                prices_list[price["address"]] = {}
            prices_list[price["address"]][price["block"]] = price["price"]

        with tqdm.tqdm(total=len(database_operations)) as progress_bar:
            for transaction in database_operations:
                # for deposit in database_operations:
                # set easy to access vars
                _hypervisor_address = transaction["address"].lower()
                _tx_token0_qtty = int(transaction["qtty_token0"])
                _tx_token1_qtty = int(transaction["qtty_token1"])
                _token0_address = hypervisors_static_list[_hypervisor_address]["pool"][
                    "token0"
                ]["address"].lower()
                _token1_address = hypervisors_static_list[_hypervisor_address]["pool"][
                    "token1"
                ]["address"].lower()
                _token0_price = prices_list[_token0_address][transaction["blockNumber"]]
                _token1_price = prices_list[_token1_address][transaction["blockNumber"]]

                # find out if this transaction is identified as proxied
                if (
                    transaction["topic"] == "withdraw"
                    and transaction["to"] in proxy_addresses[chain]
                ):
                    raise ValueError("withdraws should not be proxied")
                elif (
                    transaction["topic"] == "deposit"
                    and transaction["sender"] in proxy_addresses[chain]
                ):
                    # PROXIED TRANSACTION
                    progress_bar.set_description(
                        f" proxied  {transaction['topic']} {hypervisors_static_list[_hypervisor_address]['symbol']}"
                    )
                    progress_bar.refresh()
                    analyze_user_transaction_process_proxied(
                        chain=chain,
                        transaction=transaction,
                        _uniproxy_address=transaction["sender"],
                        _hypervisor_address=_hypervisor_address,
                        _tx_token0_qtty=_tx_token0_qtty,
                        _tx_token1_qtty=_tx_token1_qtty,
                        _token0_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token0"]["decimals"],
                        _token1_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token1"]["decimals"],
                        _token0_address=_token0_address,
                        _token1_address=_token1_address,
                        _token0_price=_token0_price,
                        _token1_price=_token1_price,
                        result=result,
                    )
                else:
                    # NOT PROXIED
                    # (withdraws are not proxied)
                    progress_bar.set_description(
                        f" {transaction['topic']} {hypervisors_static_list[_hypervisor_address]['symbol']}"
                    )
                    progress_bar.refresh()
                    analyze_user_transaction_process(
                        chain=chain,
                        transaction=transaction,
                        _hypervisor_address=_hypervisor_address,
                        _tx_token0_qtty=_tx_token0_qtty,
                        _tx_token1_qtty=_tx_token1_qtty,
                        _token0_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token0"]["decimals"],
                        _token1_decimals=hypervisors_static_list[_hypervisor_address][
                            "pool"
                        ]["token1"]["decimals"],
                        _token0_address=_token0_address,
                        _token1_address=_token1_address,
                        _token0_price=_token0_price,
                        _token1_price=_token1_price,
                        result=result,
                    )

                # set timestamps
                if _set_ini_timestamp:
                    ini_timestamp = (
                        transaction["timestamp"]
                        if not ini_timestamp
                        else min(ini_timestamp, transaction["timestamp"])
                    )
                if _set_end_timestamp:
                    end_timestamp = (
                        transaction["timestamp"]
                        if not end_timestamp
                        else max(end_timestamp, transaction["timestamp"])
                    )

                progress_bar.update(1)

        if send_to_telegram:
            # build caption message
            caption = (
                f"users net position for {' '.join([x for x in chains])} {len(hypervisor_addresses) if hypervisor_addresses else 'all'} hypes from {datetime.fromtimestamp(ini_timestamp,timezone.utc)} to {datetime.fromtimestamp(end_timestamp,timezone.utc)}",
            )

            # send to telegram
            telegram_send_file(
                input_file_content=json.dumps(result).encode("utf-8"),
                full_filename=f"users_net_position.json",
                mime_type="application/json",
                telegram_file_type="Document",
                caption=caption,
            )

        if save_to_db:
            # save to reports collection
            result["id"] = create_id_report(
                chain=chain,
                report_type=reportType.USERS_ACTIVITY,
                customId="users_net_position_Galxe",
            )
            db_return = get_default_localdb(
                network=chain.database_name
            ).replace_item_to_database(collection_name="reports", data=result)
            logging.getLogger(__name__).debug(
                f" Save result of users net position report to database: {db_return.raw_result}"
            )

    return result


def analyze_user_transaction_process_proxied(
    chain: Chain,
    transaction: dict,
    _uniproxy_address: str,
    _hypervisor_address: str,
    _tx_token0_qtty: int,
    _tx_token1_qtty: int,
    _token0_decimals: int,
    _token1_decimals: int,
    _token0_price: float,
    _token1_price: float,
    _token0_address: str,
    _token1_address: str,
    result: dict,
):
    """add user address

    Args:
        chain (Chain):
        transaction (dict):
        _uniproxy_address (str):
        _hypervisor_address (str):
        _tx_token0_qtty (int):
        _tx_token1_qtty (int):
        _token0_price (float):
        _token1_price (float):
        _token0_address (str):
        _token1_address (str):
        result (dict):
    """

    if transaction["topic"] == "withdraw":
        raise ValueError("withdraws should not be proxied")

    ercHelper = build_erc20_helper(chain=chain)
    receipt = ercHelper._getTransactionReceipt(transaction["transactionHash"])
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
            and _current_to == _uniproxy_address
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

    # check if found and if they are equal to each other
    if found_addresses:
        if len(found_addresses) == 2:
            # set easy to access vars
            _user_address = found_addresses[0]
            _tx_token0_qtty /= 10**_token0_decimals
            _tx_token1_qtty /= 10**_token1_decimals
            _deposit_usd_value = (
                _tx_token0_qtty * _token0_price + _tx_token1_qtty * _token1_price
            )

            # check if they are equal
            if found_addresses[0] != found_addresses[1]:
                # not equal! check if one of them is minted
                if "0x0000000000000000000000000000000000000000" in found_addresses:
                    # this may be a wrapped eth transaction, or another token wraped by the user directly to proxy contract.
                    # because it matches qtty, we assume it is correct and use the other address as user address.
                    logging.getLogger(__name__).debug(
                        f" Found wrapped eth {transaction['topic']}!!  "
                    )
                    # select the other address
                    _user_address = [
                        x.lower()
                        for x in found_addresses
                        if x != "0x0000000000000000000000000000000000000000"
                    ][0]
                else:
                    logging.getLogger(__name__).debug(
                        f" Found TWO different addresses!!  {transaction['address']} {transaction['blockNumber']} -> {found_addresses}"
                    )
                    return

            # create user address in result, if needed
            if _user_address not in result:
                result[_user_address] = {
                    "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
                    "total_deposits": {"token0": 0, "token1": 0, "usd": 0},
                    "total_withdraws": {"token0": 0, "token1": 0, "usd": 0},
                    "hypervisors": {},
                }

            # add usd value and qtty to user address
            result[_user_address]["total_net_position"]["usd"] += _deposit_usd_value
            result[_user_address]["total_net_position"]["token0"] += _tx_token0_qtty
            result[_user_address]["total_net_position"]["token1"] += _tx_token1_qtty

            if transaction["topic"] == "deposit":
                result[_user_address]["total_deposits"]["usd"] += _deposit_usd_value
                result[_user_address]["total_deposits"]["token0"] += _tx_token0_qtty
                result[_user_address]["total_deposits"]["token1"] += _tx_token1_qtty
            elif transaction["topic"] == "withdraw":
                result[_user_address]["total_withdraws"]["usd"] += _deposit_usd_value
                result[_user_address]["total_withdraws"]["token0"] += _tx_token0_qtty
                result[_user_address]["total_withdraws"]["token1"] += _tx_token1_qtty

            # add hypervisor
            if _hypervisor_address not in result[_user_address]["hypervisors"]:
                result[_user_address]["hypervisors"][_hypervisor_address] = {
                    "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
                    "total_deposits": {"token0": 0, "token1": 0, "usd": 0},
                    "total_withdraws": {"token0": 0, "token1": 0, "usd": 0},
                    "transactions": [],
                }

            # add hype usd value
            result[_user_address]["hypervisors"][_hypervisor_address][
                "total_net_position"
            ]["usd"] += _deposit_usd_value
            # add qtty to user hype address
            result[_user_address]["hypervisors"][_hypervisor_address][
                "total_net_position"
            ]["token0"] += _tx_token0_qtty
            result[_user_address]["hypervisors"][_hypervisor_address][
                "total_net_position"
            ]["token1"] += _tx_token1_qtty
            if transaction["topic"] == "deposit":
                result[_user_address]["hypervisors"][_hypervisor_address][
                    "total_deposits"
                ]["usd"] += _deposit_usd_value
                result[_user_address]["hypervisors"][_hypervisor_address][
                    "total_deposits"
                ]["token0"] += _tx_token0_qtty
                result[_user_address]["hypervisors"][_hypervisor_address][
                    "total_deposits"
                ]["token1"] += _tx_token1_qtty
            elif transaction["topic"] == "withdraw":
                result[_user_address]["hypervisors"][_hypervisor_address][
                    "total_withdraws"
                ]["usd"] += _deposit_usd_value
                result[_user_address]["hypervisors"][_hypervisor_address][
                    "total_withdraws"
                ]["token0"] += _tx_token0_qtty
                result[_user_address]["hypervisors"][_hypervisor_address][
                    "total_withdraws"
                ]["token1"] += _tx_token1_qtty

            # add transaction
            result[_user_address]["hypervisors"][_hypervisor_address][
                "transactions"
            ].append(
                {
                    "block": transaction["blockNumber"],
                    "txHash": transaction["transactionHash"],
                    "timestamp": transaction["timestamp"],
                    "token0": _tx_token0_qtty,
                    "token1": _tx_token1_qtty,
                    "usd": _deposit_usd_value,
                    "user_address": _user_address,
                    "from": _current_from,
                    "to": _current_to,
                    "topic": transaction["topic"],
                }
            )
        else:
            logging.getLogger(__name__).debug(
                f" ERROR !!  {transaction['address']} {transaction['blockNumber']} -> {found_addresses}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" Not found {transaction['address']} {transaction['blockNumber']} -> {found_addresses}"
        )


def analyze_user_transaction_process(
    chain: Chain,
    transaction: dict,
    _hypervisor_address: str,
    _tx_token0_qtty: int,
    _tx_token1_qtty: int,
    _token0_decimals: int,
    _token1_decimals: int,
    _token0_price: float,
    _token1_price: float,
    _token0_address: str,
    _token1_address: str,
    result: dict,
):
    # set easy to access vars
    _user_address = transaction["sender"].lower()
    _tx_token0_qtty /= 10**_token0_decimals
    _tx_token1_qtty /= 10**_token1_decimals
    _tx_usd_value = (_tx_token0_qtty * _token0_price) + (
        _tx_token1_qtty * _token1_price
    )

    # make sure first user transaction is not a withdraw ( to avoid a negative starting point)
    if transaction["topic"] == "withdraw" and (
        _user_address not in result
        or _hypervisor_address not in result[_user_address]["hypervisors"]
    ):
        logging.getLogger(__name__).debug(
            f" First transaction is a withdraw, skipping {transaction['address']} {transaction['blockNumber']} -> {transaction['topic']}"
        )
        return

    # create user address in result, if needed
    if _user_address not in result:
        result[_user_address] = {
            "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
            "total_deposits": {"token0": 0, "token1": 0, "usd": 0},
            "total_withdraws": {"token0": 0, "token1": 0, "usd": 0},
            "hypervisors": {},
        }
    if _hypervisor_address not in result[_user_address]["hypervisors"]:
        result[_user_address]["hypervisors"][_hypervisor_address] = {
            "total_net_position": {"token0": 0, "token1": 0, "usd": 0},
            "total_deposits": {"token0": 0, "token1": 0, "usd": 0},
            "total_withdraws": {"token0": 0, "token1": 0, "usd": 0},
            "transactions": [],
        }

    # define if this is a deposit or withdraw
    _sign = 1 if transaction["topic"] == "deposit" else -1

    # add to result
    # add usd value
    result[_user_address]["total_net_position"]["usd"] += _tx_usd_value * _sign
    # add qtty to user address
    result[_user_address]["total_net_position"]["token0"] += _tx_token0_qtty * _sign
    result[_user_address]["total_net_position"]["token1"] += _tx_token1_qtty * _sign
    if transaction["topic"] == "deposit":
        result[_user_address]["total_deposits"]["usd"] += _tx_usd_value
        result[_user_address]["total_deposits"]["token0"] += _tx_token0_qtty
        result[_user_address]["total_deposits"]["token1"] += _tx_token1_qtty
    elif transaction["topic"] == "withdraw":
        result[_user_address]["total_withdraws"]["usd"] += _tx_usd_value
        result[_user_address]["total_withdraws"]["token0"] += _tx_token0_qtty
        result[_user_address]["total_withdraws"]["token1"] += _tx_token1_qtty

    # add qtty to hypervisor address
    result[_user_address]["hypervisors"][_hypervisor_address]["total_net_position"][
        "usd"
    ] += (_tx_usd_value * _sign)
    result[_user_address]["hypervisors"][_hypervisor_address]["total_net_position"][
        "token0"
    ] += (_tx_token0_qtty * _sign)
    result[_user_address]["hypervisors"][_hypervisor_address]["total_net_position"][
        "token1"
    ] += (_tx_token1_qtty * _sign)
    if transaction["topic"] == "deposit":
        result[_user_address]["hypervisors"][_hypervisor_address]["total_deposits"][
            "usd"
        ] += _tx_usd_value
        result[_user_address]["hypervisors"][_hypervisor_address]["total_deposits"][
            "token0"
        ] += _tx_token0_qtty
        result[_user_address]["hypervisors"][_hypervisor_address]["total_deposits"][
            "token1"
        ] += _tx_token1_qtty
    elif transaction["topic"] == "withdraw":
        result[_user_address]["hypervisors"][_hypervisor_address]["total_withdraws"][
            "usd"
        ] += _tx_usd_value
        result[_user_address]["hypervisors"][_hypervisor_address]["total_withdraws"][
            "token0"
        ] += _tx_token0_qtty
        result[_user_address]["hypervisors"][_hypervisor_address]["total_withdraws"][
            "token1"
        ] += _tx_token1_qtty

    # add transaction
    result[_user_address]["hypervisors"][_hypervisor_address]["transactions"].append(
        {
            "block": transaction["blockNumber"],
            "txHash": transaction["transactionHash"],
            "timestamp": transaction["timestamp"],
            "token0": _tx_token0_qtty,
            "token1": _tx_token1_qtty,
            "usd": _tx_usd_value,
            "user_address": _user_address,
            "from": transaction["sender"].lower(),
            "to": transaction["to"].lower(),
            "topic": transaction["topic"],
        }
    )
