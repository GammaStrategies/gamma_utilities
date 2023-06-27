import logging

import tqdm
from bins.configuration import CONFIGURATION, DEX_POOLS, USDC_TOKEN_ADDRESSES
from bins.database.common.db_collections_common import database_local
from bins.general.enums import Chain
from bins.general.file_utilities import save_json
from bins.w3.builders import build_protocol_pool, convert_dex_protocol


def create_price_paths_json():
    """Create json file with all price paths"""
    # build token paths
    token_price_paths = build_token_paths(max_depth=6)

    save_json(filename="token_paths", data=token_price_paths, folder_path="data")
    logging.getLogger(__name__).info(
        "  token paths json file saved at data/token_paths.json"
    )


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

            # token is already in token_pools
            if token0 not in token_pools[chain]:
                token_pools[chain][token0] = {}
            if token1 not in token_pools[chain]:
                token_pools[chain][token1] = {}

            # if token1 in token_pools[chain][token0]:
            #     po = ""
            # if token0 in token_pools[chain][token1]:
            #     po = ""

            # add tokens as keys
            token_pools[chain][token0][token1] = {
                "protocol": pool["protocol"],
                "address": address,
            }
            token_pools[chain][token1][token0] = {
                "protocol": pool["protocol"],
                "address": address,
            }

    return token_pools


def build_token_paths(max_depth: int = 6):
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

            # add chain to token_pools
            if not chain in token_pools:
                token_pools[chain] = {}

            # progress bar
            progress_hook(f""" Chain: {chain} """, 0)

            # 0) add all database pools available to token_pools
            add_database_pools_to_paths(token_pools=token_pools, chain=chain)

            # 1) construct paths to usdc
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
                                    }
                                ]
                            )
                            processed_pools.append(pool_data["address"])

                        progress_hook(
                            f""" Chain: {chain} USDC paths: {len(token_pools_paths)}""",
                            0,
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

            # update progress bar
            progress_hook("", 1)

    return result


def add_database_pools_to_paths(token_pools: dict, chain: Chain):
    """add all database pools to token_pools

    Args:
        token_pools (dict):
        chain (Chain):
    """

    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    batch_size = 10000

    # 0) add all database pools available
    for hype_pool in database_local(
        mongo_url=mongo_url, db_name=f"{chain.database_name}_gamma"
    ).get_items_from_database(
        collection_name="static",
        find={},
        batch_size=batch_size,
        projection={"pool"},
    ):
        token0_address = hype_pool["pool"]["token0"]["address"]
        token1_address = hype_pool["pool"]["token1"]["address"]

        if not token0_address in token_pools.get(chain, []):
            token_pools[chain][token0_address] = {}
        if not token1_address in token_pools.get(chain, []):
            token_pools[chain][token1_address] = {}

        # add token0 as key
        token_pools[chain][token0_address][token1_address] = {
            "protocol": convert_dex_protocol(hype_pool["pool"]["dex"]),
            "address": hype_pool["pool"]["address"],
        }

        # add token1 as key
        token_pools[chain][token1_address][token0_address] = {
            "protocol": convert_dex_protocol(hype_pool["pool"]["dex"]),
            "address": hype_pool["pool"]["address"],
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
