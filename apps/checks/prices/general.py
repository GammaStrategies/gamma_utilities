import logging

import tqdm
from apps.checks.base_objects import analysis_item, base_analyzer_object
from bins.database.common.database_ids import create_id_price

from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import get_default_globaldb, get_from_localdb
from bins.general.enums import Chain
from bins.mixed.price_utilities import price_scraper


class price_analyzer(base_analyzer_object):
    def __init__(self):
        super().__init__()

    def check_status_prices(self, chain: Chain):
        """Check that all status tokens have usd prices"""
        # get all prices + address + block
        prices = {
            x["id"]
            for x in get_default_globaldb().get_unique_prices_addressBlock(
                network=chain.database_name
            )
        }

        # get tokens and blocks present in database
        prices_todo = set()

        for x in get_from_localdb(network=chain.database_name, collection="status"):
            for i in [0, 1]:
                db_id = create_id_price(
                    network=chain.database_name,
                    block=x["pool"][f"token{i}"]["block"],
                    token_address=x["pool"][f"token{i}"]["address"],
                )

                if db_id not in prices:
                    prices_todo.add(db_id)

        if prices_todo:
            # create item
            self.items.append(
                analysis_item(
                    name="prices",
                    data=prices_todo,
                    log_message=f" Found {len(prices_todo)} token blocks without price, from a total of {len(prices)} ({len(prices_todo) / len(prices):,.1%}) in {chain.database_name}",
                    telegram_message=f" Found {len(prices_todo)} token blocks without price, from a total of {len(prices)} ({len(prices_todo) / len(prices):,.1%}) in {chain.database_name}",
                )
            )

    def check_stable_prices(self, chain: Chain):
        """Search database for predefined stable tokens usd price devisations from 1
            and log it

        Args:
            network (str): _description_
            local_db_manager (database_local):
            global_db_manager (database_global):
        """
        logging.getLogger(__name__).debug(
            f" Seek deviations of {chain.database_name}'s stable token usd prices from 1 usd"
        )

        stables_symbol_list = ["USDC", "USDT", "LUSD", "DAI"]
        stables = {
            x["pool"]["token0"]["symbol"]: x["pool"]["token0"]["address"]
            for x in get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"pool.token0.symbol": {"$in": stables_symbol_list}},
            )
        } | {
            x["pool"]["token1"]["symbol"]: x["pool"]["token1"]["address"]
            for x in get_from_localdb(
                network=chain.database_name,
                collection="static",
                find={"pool.token1.symbol": {"$in": stables_symbol_list}},
            )
        }

        # database ids var
        db_ids = []

        for x in get_default_globaldb().get_items_from_database(
            collection_name="usd_prices",
            find={
                "address": {"$in": list(stables.values())},
                "network": chain.database_name,
            },
        ):
            # check if deviation from 1 is significative
            if abs(x["price"] - 1) > 0.3:
                logging.getLogger(__name__).warning(
                    f" Stable {x['network']}'s {x['address']} usd price is {x['price']} at block {x['block']}"
                )
                # add id
                db_ids.append(x["_id"])

        if db_ids:
            self.items.append(
                analysis_item(
                    name="prices",
                    data=db_ids,
                    log_message=f" Found {len(db_ids)} token errors in {chain.database_name}",
                    telegram_message=f" Found {len(db_ids)} token errors in {chain.database_name}",
                )
            )

    def check_tokens_without_price(self, chains: list[Chain] | None = None):
        """Get a list of tokens that can't get prices from by using the price_scraper (current configuration).
            Log it to telegram

        Args:
            chains (list[Chain] | None, optional): list of chains to process. Defaults to All.

        """
        total = {
            "tokens": 0,
            "chains": 0,
            "tokens_without_price": 0,
        }
        chains = chains or list(Chain)

        with tqdm.tqdm(total=len(chains)) as progress_bar:
            for chain in chains:
                # add to total chains
                total["chains"] += 1

                # get tokens from static collection
                static_hypes = get_from_localdb(
                    network=chain.database_name, collection="static", find={}
                )
                if not static_hypes:
                    logging.getLogger(__name__).warning(
                        f" No tokens found in {chain.database_name} static collection"
                    )
                    continue

                # create a list of unique tokens from the hypervisor['pool'] token0 and token1 fields
                tokens = {
                    x["pool"]["token0"]["address"]: x["pool"]["token0"]["symbol"]
                    for x in static_hypes
                } | {
                    x["pool"]["token1"]["address"]: x["pool"]["token1"]["symbol"]
                    for x in static_hypes
                }
                if not tokens:
                    logging.getLogger(__name__).error(
                        f" No tokens found in {chain.database_name} static collection"
                    )
                    continue
                # try get prices for all those at current block
                price_helper = price_scraper(thegraph=False)

                for token_address, token_symbol in tokens.items():
                    # add to total tokens
                    total["tokens"] += 1

                    try:
                        _tmpPrice, _tmpSource = price_helper.get_price(
                            network=chain.database_name,
                            token_id=token_address,
                            block=0,
                        )
                        if not _tmpPrice:
                            self.items.append(
                                analysis_item(
                                    name="missing_price",
                                    data={
                                        "address": token_address,
                                        "symbol": token_symbol,
                                    },
                                    log_message=f" {chain.fantasy_name} {token_symbol} token is not getting the price correctly {token_address}",
                                    telegram_message=f"<b> {chain.fantasy_name} {token_symbol} token is not getting the price correctly </b><pre>{token_address}</pre>",
                                )
                            )

                            # add to total tokens without price
                            total["tokens_without_price"] += 1

                    except Exception as e:
                        logging.getLogger(__name__).exception(
                            f" Error getting price for {chain.database_name} {token_symbol} {token_address} -> {e}"
                        )

                    progress_bar.set_description(
                        f" Just checked {chain.fantasy_name} {token_symbol} "
                    )
                    progress_bar.refresh()
                progress_bar.update(1)

        # send summary conclusion
        self.items.append(
            analysis_item(
                name="summary",
                data=total,
                log_message=f" Token price check summary:\n processed chains: {total['chains']:,.0f}\n processed tokens: {total['tokens']:,.0f} {chains[0].fantasy_name if len(chains) == 1 else ''}\n found tokens without price: {total['tokens_without_price']:,.0f} {total['tokens_without_price']/total['tokens'] if total['tokens'] else 0:.0%}",
                telegram_message=f"<b> Token price check summary:</b>\n<i> processed</i> <b> chains:</b> {total['chains']:,.0f} {chains[0].fantasy_name if len(chains) == 1 else ''}\n<i> processed</i> <b> tokens:</b> {total['tokens']:,.0f}\n<b> found tokens without price:</b> {total['tokens_without_price']:,.0f} {total['tokens_without_price']/total['tokens'] if total['tokens'] else 0:.0%}",
            )
        )
