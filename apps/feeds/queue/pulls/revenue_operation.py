from datetime import datetime, timezone
import logging
from apps.feeds.queue.queue_item import QueueItem
from apps.feeds.utils import get_hypervisor_price_per_share_from_status
from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_operation

from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
    get_price_from_db,
)
from bins.general.enums import text_to_chain
from bins.mixed.price_utilities import price_scraper
from bins.w3.builders import build_erc20_helper


def pull_from_queue_revenue_operation(network: str, queue_item: QueueItem) -> bool:
    try:
        # the operation is in the 'data' field...
        operation = queue_item.data

        # set operation id (same hash has multiple operations)
        operation["id"] = create_id_operation(
            logIndex=operation["logIndex"], transactionHash=operation["transactionHash"]
        )
        # lower case address ( to ease comparison ) ( token address )
        operation["address"] = operation["address"].lower()

        # log
        logging.getLogger(__name__).debug(
            f"  -> Processing {network}'s revenue operation {operation['id']}"
        )

        # select token address
        if operation["topic"] == "rewardPaid":
            _token_address = operation["token"]
        elif operation["topic"] == "transfer":
            _token_address = operation["address"]
        else:
            raise ValueError(f" Unknown operation topic {operation['topic']}")

        dumb_erc20 = build_erc20_helper(
            chain=text_to_chain(network), address=_token_address
        )

        # set timestamp
        operation["timestamp"] = dumb_erc20.timestampFromBlockNumber(
            block=int(operation["blockNumber"])
        )
        # set year and month
        operation["year"] = datetime.fromtimestamp(
            operation["timestamp"], timezone.utc
        ).year
        operation["month"] = datetime.fromtimestamp(
            operation["timestamp"], timezone.utc
        ).month

        # set tokens symbol and decimals
        _process_price = True
        try:
            operation["symbol"] = dumb_erc20.symbol
            operation["decimals"] = dumb_erc20.decimals
        except Exception as e:
            # sometimes, the address is not an ERC20 but NFT like or other,
            # so it has no symbol or decimals
            operation["decimals"] = 0
            if not "symbol" in operation:
                operation["symbol"] = "unknown"
            _process_price = False
            operation["usd_value"] = 0

        # process operation by topic
        if operation["topic"] == "rewardPaid":
            # get dex from configured fixed revenue addresses
            if fixed_revenue_addressDex := (
                CONFIGURATION["script"]["protocols"]["gamma"]
                .get("filters", {})
                .get("revenue_wallets", {})
                .get(network, {})
                or {}
            ):
                # TODO: change on new configuration
                try:
                    operation["dex"] = fixed_revenue_addressDex.get(
                        operation["user"], ""
                    ).split("_")[1]
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Cant get dex from fixed revenue address {operation['user']} from {fixed_revenue_addressDex}"
                    )

        elif operation["topic"] == "transfer":
            # get dex from database
            if hypervisor_static := get_from_localdb(
                network=network,
                collection="static",
                find={
                    "id": operation["src"],
                },
            ):
                hypervisor_static = hypervisor_static[0]
                # this is a hypervisor fee related operation
                operation["dex"] = hypervisor_static["dex"]

        else:
            # unknown operation topic
            # raise ValueError(f" Unknown operation topic {operation['topic']}")
            pass

        # get price at block
        price = 0

        # check if this is a mint operation ( nft or hype LP provider as user ...)
        if (
            "src" in operation
            and operation["src"] == "0x0000000000000000000000000000000000000000"
        ):
            logging.getLogger(__name__).debug(
                f" Mint operation found in queue for {network} {operation['address']} at block {operation['blockNumber']}"
            )
            # may be a gamma hypervisor address or other

        # price
        if _process_price:
            # if token address is an hypervisor address, get share price
            if hypervisor_status := get_from_localdb(
                network=network,
                collection="status",
                find={
                    "address": operation["address"],
                    "block": operation["blockNumber"],
                },
            ):
                # this is a hypervisor address
                hypervisor_status = hypervisor_status[0]
                # get token prices from database
                try:
                    price = get_hypervisor_price_per_share_from_status(
                        network=network, hypervisor_status=hypervisor_status
                    )
                except Exception as e:
                    pass
            else:
                # try get price from database
                try:
                    price = get_price_from_db(
                        network=network,
                        block=operation["blockNumber"],
                        token_address=_token_address,
                    )
                except Exception as e:
                    # no database price
                    pass

            if price in [0, None]:
                # scrape price
                price_helper = price_scraper(
                    cache=True, thegraph=False, geckoterminal_sleepNretry=True
                )
                price, source = price_helper.get_price(
                    network=network,
                    token_id=_token_address,
                    block=operation["blockNumber"],
                )

                if price in [0, None]:
                    logging.getLogger(__name__).debug(
                        f"  Cant get price for {network}'s {_token_address} token at block {operation['blockNumber']}. Value will be zero"
                    )
                    price = 0

            try:
                operation["usd_value"] = price * (
                    int(operation["qtty"]) / 10 ** operation["decimals"]
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Setting usd_value = 0 -> Error:  {e}"
                )
                operation["usd_value"] = 0

        # save operation to database
        if db_return := get_default_localdb(network=network).replace_item_to_database(
            data=operation, collection_name="revenue_operations"
        ):
            logging.getLogger(__name__).debug(
                f" Saved revenue operation {operation['id']} - > mod: {db_return.modified_count}  matched: {db_return.matched_count}"
            )

        # log
        logging.getLogger(__name__).debug(
            f"  <- Done processing {network}'s revenue operation {operation['id']}"
        )

        return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s revenue operation queue item: {e}"
        )

    # return result
    return False
