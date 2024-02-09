from datetime import datetime
import logging
import random

import concurrent.futures

import tqdm
from bins.apis.coingecko_utilities import coingecko_price_helper
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global
from bins.database.helpers import get_from_localdb

from bins.general.enums import Chain, databaseSource
from bins.general.file_utilities import load_json, save_json
from bins.mixed.price_utilities import price_scraper


def feed_latest_usd_prices(threaded: bool = True):
    """Feed current usd prices for all tokens specified in data/price_token_address.json file"""

    logging.getLogger(__name__).info(
        "Feeding current usd prices for all tokens specified in data/price_token_address.json file"
    )

    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db = database_global(mongo_url=mongo_url)

    if CONFIGURATION["sources"].get("coingeko_api_key", None):
        price_helper = price_scraper(
            cache=False,
            thegraph=False,
            coingecko=True,
            geckoterminal_sleepNretry=True,
            source_order=[
                databaseSource.ONCHAIN,
                databaseSource.COINGECKO,
                databaseSource.GECKOTERMINAL,
            ],
        )
    else:
        price_helper = price_scraper(
            cache=False,
            thegraph=False,
            coingecko=True,
            geckoterminal_sleepNretry=True,
            source_order=[
                databaseSource.ONCHAIN,
                databaseSource.GECKOTERMINAL,
                databaseSource.COINGECKO,
            ],
        )

    cg_helper = coingecko_price_helper(retries=3, request_timeout=25)

    def loopme_coingecko(network, addresses) -> tuple[str, list[str], bool]:
        result = []
        addresses_processed = []
        prices = {}

        try:
            # get prices from coingecko
            prices = cg_helper.get_prices(
                network, contract_addresses=list(addresses.keys())
            )
        except ValueError as e:
            if "status" in e.args[0]:
                if (
                    "error_code" in e.args[0]["status"]
                    and e.args[0]["status"]["error_code"] == 429
                ):
                    # too many requests
                    logging.getLogger(__name__).error(
                        f" Too many requests to coingecko while gathering latest prices"
                    )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Exception at coingecko's price gathering       error-> {e}"
            )

        if prices:
            # loop through prices
            for token_address, price in prices.items():
                if not token_address in addresses_processed:
                    if "usd" in price:
                        # add price to result

                        result.append(
                            {
                                "id": f"{network}_{token_address}",
                                "network": network,
                                "timestamp": int(datetime.now().timestamp()),
                                "address": token_address,
                                "price": float(price["usd"]),
                                "source": databaseSource.COINGECKO,
                            }
                        )
                        addresses_processed.append(token_address)
                    else:
                        logging.getLogger(__name__).error(
                            f"{network} - {token_address} has no usd price -> {price}"
                        )
                else:
                    logging.getLogger(__name__).error(
                        f"{network} - {token_address} is repeated in price_token_address.json file"
                    )

        # save prices to database in bulk
        if result:
            # check if all addresses were processed
            # if len(result) != len(addresses):
            #    # differences = set(addresses_processed.keys()).difference(set(addresses))
            #     logging.getLogger(__name__).debug(
            #         f" result lengh: {len(result)} is different than addresses lengh: {len(addresses)}"
            #     )

            if response := db.replace_items_to_database(
                data=result, collection_name="current_usd_prices"
            ):
                logging.getLogger(__name__).debug(
                    f"{network} - {len(result)} prices saved to database"
                )
                return network, addresses_processed, True
            else:
                logging.getLogger(__name__).debug(
                    f"{network} - {len(result)} prices not saved to database"
                )
        else:
            logging.getLogger(__name__).debug(f"{network} - no prices found")

        return network, addresses, False

    def loopme(network, address) -> tuple[str, str, bool]:
        # get price
        price, source = price_helper.get_price(network=network, token_id=address)
        # save price
        if price:
            db.set_current_price_usd(
                network=network,
                token_address=address,
                price_usd=price,
                source=source,
            )

            return network, f"{address} saved", True

        return network, f"{address} - no price found", False

    # load token addresses by chain
    if token_addresses := load_json(filename="price_token_address", folder_path="data"):
        _errors = 0

        # build arguments list for coingecko
        args = [
            (network, addresses.copy())
            for network, addresses in token_addresses.items()
        ]

        logging.getLogger(__name__).debug(
            f"  Using coingecko to gather prices for {len(args)} tokens at once"
        )
        with tqdm.tqdm(total=len(args)) as progress_bar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for network, addresses, result in ex.map(
                    lambda p: loopme_coingecko(*p), args
                ):
                    if result:
                        for address in addresses:
                            # remove address from token_addresses
                            token_addresses[network].pop(address)
                    # progress
                    progress_bar.update(1)

        # build arguments list with the remaining addresses
        args = [
            (network, address)
            for network, addresses in token_addresses.items()
            for address in addresses
        ]
        # shuffle args so that prices get updated in a random order
        random.shuffle(args)
        logging.getLogger(__name__).debug(
            f"  Using multiple sources to gather prices for {len(args)} tokens left"
        )
        with tqdm.tqdm(total=len(args)) as progress_bar:
            # limit max workers to avoid too many requests at once
            if threaded:
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                    for network, address, result in ex.map(lambda p: loopme(*p), args):
                        if not result:
                            _errors += 1
                            logging.getLogger(__name__).error(
                                f" {network} USD price for {address} was not found"
                            )
                        # progress
                        progress_bar.set_description(
                            f" [errors: {_errors}] {network} USD price for {address}"
                        )
                        progress_bar.update(1)

            else:
                for network, address in args:
                    # get price
                    net, addr, result = loopme(network=network, address=address)

                    if not result:
                        _errors += 1
                        logging.getLogger(__name__).error(
                            f" {net} USD price for {addr} was not found"
                        )
                    # progress
                    progress_bar.set_description(
                        f" [errors: {_errors}] {net} USD price for {addr}"
                    )
                    progress_bar.update(1)
        #
        logging.getLogger(__name__).info(
            f" {_errors} errors found while feeding current prices"
        )
    else:
        logging.getLogger(__name__).error(
            f" price_token_address.json file not found in data folder"
        )


def create_latest_usd_prices_address_json():
    # get all 1st rewarder status from database
    batch_size = 100000
    filename = "price_token_address"
    folder_path = "data"
    logging.getLogger(__name__).info(f" Creating current usd prices address json file")

    result = {}

    # add manually tokens not found in the database
    manual_tokens = {
        # Chain.CELO.database_name: {
        #     "0xc16b81af351ba9e64c1a069e3ab18c244a1e3049".lower(): {"symbol": "ageur"},
        # },
        Chain.POLYGON.database_name: {
            "0xd838290e877e0188a4a44700463419ed96c16107".lower(): {"symbol": "nct"},
        }
    }

    # add tokens from static collections
    for chain in Chain:
        # hypervisors
        for item in get_from_localdb(
            network=chain.database_name,
            collection="static",
            find={},
            batch_size=batch_size,
            projection={
                "_id": 0,
                "pool.token0.address": 1,
                "pool.token0.symbol": 1,
                "pool.token1.address": 1,
                "pool.token1.symbol": 1,
            },
        ):
            # create chain if not exists
            if not chain.database_name in result:
                result[chain.database_name] = {}

            if item["pool"]["token0"]["address"] not in result[chain.database_name]:
                result[chain.database_name][item["pool"]["token0"]["address"]] = {
                    "symbol": item["pool"]["token0"]["symbol"]
                }
            # else:
            #     logging.getLogger(__name__).debug(
            #         f" {item['pool']['token0']} already in list"
            #     )
            if item["pool"]["token1"]["address"] not in result[chain.database_name]:
                result[chain.database_name][item["pool"]["token1"]["address"]] = {
                    "symbol": item["pool"]["token1"]["symbol"]
                }
            # else:
            #     logging.getLogger(__name__).debug(
            #         f" {item['pool']['token1']} already in list"
            #     )

        # rewards
        for item in get_from_localdb(
            network=chain.database_name,
            collection="rewards_static",
            find={},
            batch_size=batch_size,
            projection={"_id": 0, "rewardToken": 1, "rewardToken_symbol": 1},
        ):
            if item["rewardToken"] not in result[chain.database_name]:
                result[chain.database_name][item["rewardToken"]] = {
                    "symbol": item["rewardToken_symbol"]
                }

    # add manual tokens to result
    if manual_tokens:
        for network, tokens in manual_tokens.items():
            if network not in result:
                result[network] = {}

            for address, data in tokens.items():
                if address not in result[network]:
                    result[network][address] = data
                else:
                    logging.getLogger(__name__).warning(
                        f" manually added {network} {address} is already in the database so its safe to remove it from the manual list"
                    )

    # save to file
    save_json(filename=filename, data=result, folder_path=folder_path)
