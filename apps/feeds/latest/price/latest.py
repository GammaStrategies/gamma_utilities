from datetime import datetime
import logging
import random

import concurrent.futures

import tqdm
from bins.apis.coingecko_utilities import coingecko_price_helper
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_global

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
    price_helper = price_scraper(
        cache=False,
        thegraph=False,
        geckoterminal_sleepNretry=True,
        source_order=[
            databaseSource.ONCHAIN,
            databaseSource.GECKOTERMINAL,
            databaseSource.THEGRAPH,
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
        # loop through prices
        for token_address, price in prices.items():
            if not token_address in addresses_processed:
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
            #
            if threaded:
                with concurrent.futures.ThreadPoolExecutor() as ex:
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


def create_latest_usd_prices_address_json():
    # get all 1st rewarder status from database
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    batch_size = 100000
    db = database_global(mongo_url=mongo_url)
    filename = "price_token_address"
    folder_path = "data"
    logging.getLogger(__name__).info(f" Creating current usd prices address json file")

    query = [
        {
            "$group": {
                "_id": {
                    "network": "$network",
                    "address": "$address",
                },
            },
        },
    ]
    result = {}

    # add manually tokens not found in the database
    manual_tokens = {
        Chain.CELO.database_name: {
            "0xc16b81af351ba9e64c1a069e3ab18c244a1e3049".lower(): {"symbol": "ageur"},
            "0x471ece3750da237f93b8e339c536989b8978a438".lower(): {"symbol": "celo"},
            "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73".lower(): {"symbol": "ceur"},
            "0x765de816845861e75a25fca122bb6898b8b1282a".lower(): {"symbol": "cusd"},
            "0x02de4766c272abc10bc88c220d214a26960a7e92".lower(): {"symbol": "nct"},
            "0x46c9757c5497c5b1f2eb73ae79b6b67d119b0b58".lower(): {"symbol": "pact"},
            "0x37f750b7cc259a2f741af45294f6a16572cf5cad".lower(): {"symbol": "usdc"},
            "0x12055ae73a83730d766a7cfed62f1797987d5fa5".lower(): {"symbol": "vmbt"},
            "0x66803fb87abd4aac3cbb3fad7c3aa01f6f3fb207".lower(): {"symbol": "weth"},
        },
        Chain.POLYGON.database_name: {
            "0xef6ab48ef8dfe984fab0d5c4cd6aff2e54dfda14".lower(): {"symbol": "crispm"},
        },
        Chain.ETHEREUM.database_name: {
            "0xf5581dfefd8fb0e4aec526be659cfab1f8c781da".lower(): {"symbol": "hopr"},
        },
        Chain.AVALANCHE.database_name: {
            "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e".lower(): {"symbol": "usdc"},
            "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7".lower(): {"symbol": "usdt"},
        },
    }

    # get tokens from database
    for item in db.get_items_from_database(
        collection_name="usd_prices", aggregate=query, batch_size=batch_size
    ):
        if item["_id"]["network"] not in result:
            result[item["_id"]["network"]] = {}

        if "address" not in result[item["_id"]["network"]]:
            result[item["_id"]["network"]][item["_id"]["address"]] = {}
        else:
            raise Exception(f" {item['_id']['address']} already in list")

    # add manual tokens to result
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
