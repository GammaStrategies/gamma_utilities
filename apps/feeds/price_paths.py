import logging
import concurrent.futures

# from multiprocessing import Pool
# from functools import partial

import tqdm
from bins.configuration import CONFIGURATION, USDC_TOKEN_ADDRESSES  # DEX_POOLS
from bins.config.price.pools_price_paths import DEX_POOLS

from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_default_globaldb,
    get_from_localdb,
    get_latest_prices_from_db,
)
from bins.general.enums import Chain
from bins.general.file_utilities import save_json, load_json
from bins.w3.builders import build_protocol_pool, convert_dex_protocol


def create_price_paths_json():
    """Create json file with all price paths"""

    # load old token paths if exist
    old_token_price_paths = load_json(filename="token_paths", folder_path="data")

    # build token paths
    token_price_paths = build_token_paths_threaded(
        max_depth=6, old_token_price_paths=old_token_price_paths
    )

    save_json(filename="token_paths", data=token_price_paths, folder_path="data")
    logging.getLogger(__name__).info(
        "  token paths json file saved at data/token_paths.json"
    )


# TODO: save token paths to database
def save_token_paths_to_db():
    # build token paths
    token_price_paths = build_token_paths(max_depth=6)

    # save token paths to database
    get_default_globaldb().save_item_to_database()


def convert_DEX_POOLS(DEX_POOLS: dict) -> dict:
    """convert DEX_POOLS config var format to token_pools format

    Args:
        DEX_POOLS (dict): { <Chain>: {
                                "<pool name>": {
                                    "address":<pool address> ,
                                    "protocol":< Protocol> ,
                                    },
                                ...
                           },
                            ...

    Returns:
        dict: <Chain>: {
                <token_address>: {
                    <token_address> : {
                        "protocol":< Protocol> ,
                        "address" : <pool address> ,
                }
                }
    """
    # prepare token_pools
    token_pools = {}

    # load old conversion, if exist, or initialize it
    dex_pools_converted = load_json(filename="dex_pools_converted", folder_path="data")
    if not dex_pools_converted:
        dex_pools_converted = {}

    for chain, data_dict in tqdm.tqdm(DEX_POOLS.items()):
        token_pools[chain] = {}
        for name, pool in data_dict.items():
            token0 = None
            token1 = None
            address = None
            # check if token0 and token1 keys are in pool
            if "token0" in pool and "token1" in pool:
                token0 = pool["token0"].lower()
                token1 = pool["token1"].lower()
                address = pool["address"].lower()
            else:
                # check if pool is already converted
                if chain in dex_pools_converted and name in dex_pools_converted[chain]:
                    logging.getLogger(__name__).debug(
                        f" pool {name} does not have token0 or token1 keys. Using latest saved conversion file."
                    )
                    token0 = dex_pools_converted[chain][name]["token0"]
                    token1 = dex_pools_converted[chain][name]["token1"]
                    address = dex_pools_converted[chain][name]["address"]
                else:
                    logging.getLogger(__name__).debug(
                        f" pool {name} does not have token0 or token1 keys. Scraping it"
                    )
                    # build pool from scratch
                    w3pool = build_protocol_pool(
                        chain=chain,
                        protocol=convert_dex_protocol(pool["protocol"]),
                        pool_address=pool["address"].lower(),
                        cached=True,
                    )
                    token0 = w3pool.token0.address.lower()
                    token1 = w3pool.token1.address.lower()
                    address = pool["address"].lower()

                    # add to dex_pools_converted
                    if chain not in dex_pools_converted:
                        dex_pools_converted[chain] = {}
                    dex_pools_converted[chain][name] = {
                        "token0": token0,
                        "token1": token1,
                        "address": address,
                    }

            # token is already in token_pools
            if token0 not in token_pools[chain]:
                token_pools[chain][token0] = {}
            if token1 not in token_pools[chain]:
                token_pools[chain][token1] = {}

            # add tokens as keys
            token_pools[chain][token0][token1] = {
                "protocol": pool["protocol"],
                "address": address,
                "min_block": pool["min_block"],
            }
            token_pools[chain][token1][token0] = {
                "protocol": pool["protocol"],
                "address": address,
                "min_block": pool["min_block"],
            }

    # save conversion
    if dex_pools_converted:
        save_json(
            filename="dex_pools_converted", data=dex_pools_converted, folder_path="data"
        )

    return token_pools


def build_token_paths(max_depth: int = 6, old_token_price_paths: dict = None) -> dict:
    logging.getLogger(__name__).info(" Building all network pools paths to USDC price")
    result = {}
    with tqdm.tqdm(total=len(Chain)) as progress_bar:

        def progress_hook(description: str, update: int):
            if description != "":
                progress_bar.set_description(description)

            progress_bar.update(update)

        # 0) add all manually set pools available to token_pools
        token_pools = convert_DEX_POOLS(DEX_POOLS=DEX_POOLS)

        for chain in Chain:
            # add chain to result
            result[chain] = {}

            # add chain to token_pools, if not already there
            if not chain in token_pools:
                token_pools[chain] = {}

            # progress bar
            progress_hook(f""" Chain: {chain} """, 0)

            # 0) add all database pools available to token_pools
            add_database_pools_to_paths(token_pools=token_pools, chain=chain)

            # 1) construct paths to usdc
            if not chain in USDC_TOKEN_ADDRESSES:
                logging.getLogger(__name__).warning(
                    f"  no usdc token addresses found in USDC_TOKEN_ADDRESSES configuration var for {chain}"
                )

            token_pools_paths = []
            for usdc_token_address in USDC_TOKEN_ADDRESSES.get(chain, []):
                if usdc_token_address in token_pools[chain]:
                    # build paths to usdc only
                    path = []
                    processed_pools = []
                    for token, pool_data in token_pools[chain][
                        usdc_token_address
                    ].items():
                        # do not add the same pool address in the path twice
                        if not pool_data["address"] in processed_pools:
                            token_pools_paths.append(
                                [
                                    {
                                        "token_to": usdc_token_address,
                                        "token_from": token,
                                        "protocol": pool_data["protocol"],
                                        "address": pool_data["address"],
                                        "min_block": pool_data["min_block"],
                                    }
                                ]
                            )
                            processed_pools.append(pool_data["address"])

                        progress_hook(
                            f""" Chain: {chain} USDC paths: {len(token_pools_paths)}""",
                            0,
                        )

                else:
                    logging.getLogger(__name__).warning(
                        f" {usdc_token_address} token defined as USD for {chain} was not found in token_pools ( gamma's + manual DEX POOLS)."
                    )

            # 2) add non usdc tokens to paths
            for path in token_pools_paths:
                new_paths = create_paths(
                    path=path,
                    token_pools=token_pools,
                    chain=chain,
                    max_depth=max_depth,
                    progress_hook=progress_hook,
                )
                token_pools_paths.extend(new_paths)

            # 3) sort paths and select the shorter ones
            logging.getLogger(__name__).debug(f"  sorting paths for {chain}")
            for path in token_pools_paths:
                # reverse path
                path.reverse()
                if path[0]["token_from"] in result[chain]:
                    # check if path is shorter than existing one
                    if len(path) < len(result[chain][path[0]["token_from"]]):
                        logging.getLogger(__name__).debug(
                            f"  shorter path found for {path[0]['token_from']} -> from {len(result[chain][path[0]['token_from']])} to {len(path)}"
                        )
                        result[chain][path[0]["token_from"]] = path
                else:
                    result[chain][path[0]["token_from"]] = path

                if len(path) > 1:
                    # update min_block as the maximum "min_block" field of all pools in the path
                    min_block = 0
                    for pool in path:
                        if pool["min_block"] > min_block:
                            min_block = pool["min_block"]
                    path[0]["min_block"] = min_block

            # update progress bar
            progress_hook("", 1)

    return result


# TODO: multiprocessing
def build_token_paths_threaded(
    max_depth: int = 6, old_token_price_paths: dict = None
) -> dict:
    logging.getLogger(__name__).info(" Building all network pools paths to USDC price")
    result = {}
    with tqdm.tqdm(total=len(Chain)) as progress_bar:

        def progress_hook(description: str, update: int):
            if description != "":
                progress_bar.set_description(description)

            progress_bar.update(update)

        # 0) add all manually set pools available to token_pools
        token_pools = convert_DEX_POOLS(DEX_POOLS=DEX_POOLS)

        for chain in Chain:
            # add chain to result
            result[chain] = {}

            # add chain to token_pools, if not already there
            if not chain in token_pools:
                token_pools[chain] = {}

            # progress bar
            progress_hook(f""" Chain: {chain} """, 0)

            # 0) add all database pools available to token_pools
            add_database_pools_to_paths(token_pools=token_pools, chain=chain)

            # 1) construct paths to usdc
            if not chain in USDC_TOKEN_ADDRESSES:
                logging.getLogger(__name__).warning(
                    f"  no usdc token addresses found in USDC_TOKEN_ADDRESSES configuration var for {chain}"
                )

            token_pools_paths = []
            for usdc_token_address in USDC_TOKEN_ADDRESSES.get(chain, []):
                if usdc_token_address in token_pools[chain]:
                    # build paths to usdc only
                    path = []
                    processed_pools = []
                    for token, pool_data in token_pools[chain][
                        usdc_token_address
                    ].items():
                        # do not add the same pool address in the path twice
                        if not pool_data["address"] in processed_pools:
                            token_pools_paths.append(
                                [
                                    {
                                        "token_to": usdc_token_address,
                                        "token_from": token,
                                        "protocol": pool_data["protocol"],
                                        "address": pool_data["address"],
                                        "min_block": pool_data["min_block"],
                                    }
                                ]
                            )
                            processed_pools.append(pool_data["address"])

                        progress_hook(
                            f""" Chain: {chain} USDC paths: {len(token_pools_paths)}""",
                            0,
                        )

                else:
                    logging.getLogger(__name__).warning(
                        f" {usdc_token_address} token defined as USD for {chain} was not found in token_pools ( gamma's + manual DEX POOLS)."
                    )

            # 2) add non usdc tokens to paths
            args = (
                (
                    path,
                    token_pools,
                    chain,
                    max_depth,
                    progress_hook,
                )
                for path in token_pools_paths
            )
            # scrape missing status
            _errors = 0
            with concurrent.futures.ThreadPoolExecutor() as ex:
                for new_paths in ex.map(lambda p: create_paths(*p), args):
                    if new_paths:
                        token_pools_paths.extend(new_paths)
                    else:
                        # error found
                        _errors += 1

            # 3) sort paths and select the shorter ones
            logging.getLogger(__name__).debug(f"  sorting paths for {chain}")
            for path in token_pools_paths:
                # reverse path
                path.reverse()
                if path[0]["token_from"] in result[chain]:
                    # check if path is shorter than existing one
                    if len(path) < len(result[chain][path[0]["token_from"]]):
                        logging.getLogger(__name__).debug(
                            f"  shorter path found for {path[0]['token_from']} -> from {len(result[chain][path[0]['token_from']])} to {len(path)}"
                        )
                        result[chain][path[0]["token_from"]] = path
                else:
                    result[chain][path[0]["token_from"]] = path

                if len(path) > 1:
                    # update min_block as the maximum "min_block" field of all pools in the path
                    min_block = 0
                    for pool in path:
                        if pool["min_block"] > min_block:
                            min_block = pool["min_block"]
                    path[0]["min_block"] = min_block

            # update progress bar
            progress_hook("", 1)

    return result


# def build_token_paths_multiprocessed(
#     max_depth: int = 6, old_token_price_paths: dict = None
# ) -> dict:
#     logging.getLogger(__name__).info(" Building all network pools paths to USDC price")
#     result = {}
#     with tqdm.tqdm(total=len(Chain)) as progress_bar:

#         def progress_hook(description: str, update: int):
#             if description != "":
#                 progress_bar.set_description(description)

#             progress_bar.update(update)

#         # 0) add all manually set pools available to token_pools
#         token_pools = convert_DEX_POOLS(DEX_POOLS=DEX_POOLS)

#         for chain in Chain:
#             # add chain to result
#             result[chain] = {}

#             # add chain to token_pools, if not already there
#             if not chain in token_pools:
#                 token_pools[chain] = {}

#             # progress bar
#             progress_hook(f""" Chain: {chain} """, 0)

#             # 0) add all database pools available to token_pools
#             add_database_pools_to_paths(token_pools=token_pools, chain=chain)

#             # 1) construct paths to usdc
#             if not chain in USDC_TOKEN_ADDRESSES:
#                 logging.getLogger(__name__).warning(
#                     f"  no usdc token addresses found in USDC_TOKEN_ADDRESSES configuration var for {chain}"
#                 )

#             token_pools_paths = []
#             for usdc_token_address in USDC_TOKEN_ADDRESSES.get(chain, []):
#                 if usdc_token_address in token_pools[chain]:
#                     # build paths to usdc only
#                     path = []
#                     processed_pools = []
#                     for token, pool_data in token_pools[chain][
#                         usdc_token_address
#                     ].items():
#                         # do not add the same pool address in the path twice
#                         if not pool_data["address"] in processed_pools:
#                             token_pools_paths.append(
#                                 [
#                                     {
#                                         "token_to": usdc_token_address,
#                                         "token_from": token,
#                                         "protocol": pool_data["protocol"],
#                                         "address": pool_data["address"],
#                                         "min_block": pool_data["min_block"],
#                                     }
#                                 ]
#                             )
#                             processed_pools.append(pool_data["address"])

#                         progress_hook(
#                             f""" Chain: {chain} USDC paths: {len(token_pools_paths)}""",
#                             0,
#                         )

#                 else:
#                     logging.getLogger(__name__).warning(
#                         f" {usdc_token_address} token defined as USD for {chain} was not found in token_pools ( gamma's + manual DEX POOLS)."
#                     )

#             # 2) add non usdc tokens to paths
#             args = (
#                 (
#                     path,
#                     token_pools,
#                     chain,
#                     max_depth,
#                     None,
#                 )
#                 for path in token_pools_paths
#             )
#             # scrape missing status
#             _errors = 0
#             with Pool(5) as p:
#                 # partial_func=partial(create_paths, token_pools, energy_interval=energy_interval, tstart=tstart, tstop=tstop, trigger_time=trigger_time)
#                 for new_paths in p.map(lambda p: create_paths(*p), args):
#                     if new_paths:
#                         token_pools_paths.extend(new_paths)
#                     else:
#                         # error found
#                         _errors += 1

#             # 3) sort paths and select the shorter ones
#             logging.getLogger(__name__).debug(f"  sorting paths for {chain}")
#             for path in token_pools_paths:
#                 # reverse path
#                 path.reverse()
#                 if path[0]["token_from"] in result[chain]:
#                     # check if path is shorter than existing one
#                     if len(path) < len(result[chain][path[0]["token_from"]]):
#                         logging.getLogger(__name__).debug(
#                             f"  shorter path found for {path[0]['token_from']} -> from {len(result[chain][path[0]['token_from']])} to {len(path)}"
#                         )
#                         result[chain][path[0]["token_from"]] = path
#                 else:
#                     result[chain][path[0]["token_from"]] = path

#                 if len(path) > 1:
#                     # update min_block as the maximum "min_block" field of all pools in the path
#                     min_block = 0
#                     for pool in path:
#                         if pool["min_block"] > min_block:
#                             min_block = pool["min_block"]
#                     path[0]["min_block"] = min_block

#             # update progress bar
#             progress_hook("", 1)

#     return result


def add_database_pools_to_paths(token_pools: dict, chain: Chain):
    """add all database pools to token_pools

    Args:
        token_pools (dict):
        chain (Chain):
    """

    batch_size = 10000

    # 0) get all prices from database
    _prices = get_latest_prices_from_db(network=chain.database_name)

    # 1) add all database pools available: LAST STATUS FOUND
    # get the last available status for each hype --> pools qtty will be less than hype's
    for hype_pool in get_from_localdb(
        network=chain.database_name,
        collection="static",
        aggregate=database_local.query_last_status_all_static(),
        batch_size=batch_size,
    ):
        # easy to acces vars
        token0_address = hype_pool["pool"]["token0"]
        token1_address = hype_pool["pool"]["token1"]

        # Choose hypervisor position to calc liquidity usd value
        if int(hype_pool["basePosition"]["liquidity"]):
            # base position
            base_usd = _prices[token0_address] * int(
                hype_pool["basePosition"]["amount0"]
            ) + _prices[token1_address] * int(hype_pool["basePosition"]["amount1"])
            pool_tvl = (base_usd / int(hype_pool["basePosition"]["liquidity"])) * int(
                hype_pool["pool"]["liquidity"]
            )
        elif int(hype_pool["limitPosition"]["liquidity"]):
            # limit position
            limit_usd = _prices[token0_address] * int(
                hype_pool["limitPosition"]["amount0"]
            ) + _prices[token1_address] * int(hype_pool["limitPosition"]["amount1"])
            pool_tvl = (limit_usd / int(hype_pool["limitPosition"]["liquidity"])) * int(
                hype_pool["pool"]["liquidity"]
            )
        else:
            # no liquidity, skip pool
            logging.getLogger(__name__).debug(
                f" {chain.database_name} There is no liquidity in hype {hype_pool['symbol']} {hype_pool['address']}. Skipping it."
            )
            continue

        # skip pools with less than X USD TVL
        if pool_tvl < 5000:
            # skip this pool
            logging.getLogger(__name__).debug(
                f" {chain.database_name} skipping pool {hype_pool['pool']['address']} because tvl= {pool_tvl:,.0f} USD "
            )
            continue

        # check if tokens are in token_pools
        if not token0_address in token_pools.get(chain, []):
            token_pools[chain][token0_address] = {}
        if not token1_address in token_pools.get(chain, []):
            token_pools[chain][token1_address] = {}

        # add token0 as key
        token_pools[chain][token0_address][token1_address] = {
            "protocol": convert_dex_protocol(hype_pool["pool"]["dex"]),
            "address": hype_pool["pool"]["address"],
            "min_block": hype_pool["pool"]["block"],
        }

        # add token1 as key
        token_pools[chain][token1_address][token0_address] = {
            "protocol": convert_dex_protocol(hype_pool["pool"]["dex"]),
            "address": hype_pool["pool"]["address"],
            "min_block": hype_pool["pool"]["block"],
        }


def create_paths(
    path: list, token_pools: dict, chain: Chain, max_depth: int = 5, progress_hook=None
) -> list[list]:
    """create a list of paths using token addresses in pools recursively"""

    # limit the depth of the path to avoid infinite loops
    if len(path) >= max_depth:
        logging.getLogger(__name__).debug(f" {chain.database_name} max depth reached")
        return []

    path_result = []
    # get path last token from ( bc we go in reverse order)
    token_from = path[-1]["token_from"]
    # min_block_from = path[-1]["min_block"]

    # get already processed items
    processed_pools = []
    processed_tokens = []
    for path_item in path:
        processed_pools.append(path_item["address"])
        processed_tokens.append(path_item["token_from"])
        processed_tokens.append(path_item["token_to"])

    # find token_from in token_pools paths to get to all possible paths
    if token_from in token_pools[chain]:
        for token, pool_data in token_pools[chain][token_from].items():
            # has to be different than already processed
            if (
                not pool_data["address"] in processed_pools
                and not token in processed_tokens
            ):
                # create new path
                new_path = path.copy()
                new_path.append(
                    {
                        "token_to": token_from,
                        "token_from": token,
                        "protocol": pool_data["protocol"],
                        "address": pool_data["address"],
                        "min_block": pool_data["min_block"],
                    }
                )
                # add new path to result ( this is a closed path)
                path_result.append(new_path)

                # create new paths from this newly created and closed path
                if new_paths := create_paths(
                    path=new_path,
                    token_pools=token_pools,
                    chain=chain,
                    max_depth=max_depth,
                    progress_hook=progress_hook,
                ):
                    # add new paths to result
                    path_result.extend(new_paths)

            # update progress bar
            if progress_hook:
                progress_hook(
                    f""" Chain: {chain} path: {len(path):02d} paths: {len(path_result):02d}""".format(),
                    0,
                )

    return path_result
