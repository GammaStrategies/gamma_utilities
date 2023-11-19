import logging
import random

import tqdm
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol


#### DATABASE ####
# get information from database to have data to compare with
# choose as many different cashuistics as possible


def get_static_hypes(chain: Chain, protocol: Protocol | None = None) -> list[dict]:
    find = {}
    if protocol:
        find["dex"] = protocol.database_name

    return get_from_localdb(
        network=chain.database_name,
        collection="static",
        find=find,
        batch_size=50000,
    )


def get_static_hypes_of_each_protocol(chain: Chain, qtty: int = 1) -> list[dict]:
    """_summary_

    Args:
        chain (Chain): network to get static hypes from
        qtty (int, optional): Quantity of static hypes to return per each protocol. Defaults to 1.

    Returns:
        list[dict]: list of static hypes as returned by datadase
    """
    query = [{"$group": {"_id": "$dex", "hypes": {"$push": "$$ROOT"}}}]

    result = []
    for itm in get_from_localdb(
        network=chain.database_name,
        collection="static",
        aggregate=query,
    ):
        for i in range(min(len(itm["hypes"]), qtty)):
            result.append(itm["hypes"][i])

    return result


def get_status_hypes_of_each_protocol(
    chain: Chain,
    qtty: int = 1,
    cashuistics: bool = True,
    protocols: list[Protocol] = None,
    brake_on_notfound: bool = True,
) -> list[dict]:
    """Get status of hypes of each protocol

    Args:
        chain (Chain): network to get status hypes from
        qtty (int, optional): lazy desired qtty per protocol ( depends on cashuistics ). Defaults to 1.
        cashuistics (bool, optional): Get as many cashuistics as possible ( and as defined as cashuistic). Defaults to True.
        protocols (list[Protocol], optional): list of protocols to get status from. Defaults to None.
        brake_on_notfound (bool, optional): only get status with all casuhistics. May return empty list. Defaults to True.
    Returns:
        list[dict]: hypervisor status
    """

    logging.getLogger(__name__).debug(
        f"  Building a representative list of at least {qtty} hypervisor status for each protocol found in {chain}"
    )

    # get all
    query = [{"$group": {"_id": "$dex", "hypes": {"$push": "$$ROOT"}}}]

    # filter by protocols when set
    if protocols:
        query.insert(
            0, {"$match": {"dex": {"$in": [p.database_name for p in protocols]}}}
        )

    result = []
    _grouped_hypes = get_from_localdb(
        network=chain.database_name,
        collection="static",
        aggregate=query,
    )
    with tqdm.tqdm(total=len(_grouped_hypes)) as progress_bar:
        for itm in _grouped_hypes:
            # one dex multiple hypes
            #
            # randomize hype order
            if itm["hypes"]:
                random.shuffle(itm["hypes"])

            # progress
            progress_bar.set_description(
                f" {len(itm['hypes'])} {itm['_id']} hypes found: evaluating cashuistics"
            )
            progress_bar.refresh()

            # get status for each protocol ( with as many cashuistics as possible)
            #   uncollected > 0
            #   uncollected = 0
            #   out of range positions
            #   in range positions
            result += get_cashuistics_many(
                chain=chain,
                hypervisor_addresses=[x["address"] for x in itm["hypes"]],
                qtty_each=qtty,
            )

            # progress
            progress_bar.update(1)

    # return
    return result


def get_cashuistics(
    chain: Chain, static_hypervisor: dict, brake_on_notfound: bool = True
) -> list[dict]:
    """Get all cashuistics for a given static hypervisor

    Args:
        chain (Chain): network
        static_hypervisor (dict):  static hypervisor fictionary
        brake_on_notfound (bool, optional): return an empty list when this is true and any casuhistic is not found. Defaults to True.

    Returns:
        list[dict]:
    """
    output = []
    # 1st uncollected > 0
    if uncollected := get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={
            "address": static_hypervisor["address"],
            "$or": [
                {"fees_uncollected.qtty_token0": {"$ne": "0"}},
                {"fees_uncollected.qtty_token1": {"$ne": "0"}},
            ],
            "basePosition_data": {"$exists": True},
            "limitPosition_data": {"$exists": True},
            "basePosition_ticksLower": {"$exists": True},
            "limitPosition_ticksLower": {"$exists": True},
            "basePosition_ticksUpper": {"$exists": True},
            "limitPosition_ticksUpper": {"$exists": True},
        },
        limit=1,
    ):
        output.append(uncollected[0])
    else:
        logging.getLogger(__name__).debug(
            f"  found no 'uncollected > 0' cashuistic for {chain.database_name} {static_hypervisor['dex']} {static_hypervisor['symbol']}"
        )
        if brake_on_notfound:
            return []

    # 2nd uncollected = 0
    if uncollected := get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={
            "address": static_hypervisor["address"],
            "$or": [
                {"fees_uncollected.qtty_token0": "0"},
                {"fees_uncollected.qtty_token1": "0"},
            ],
            "basePosition_data": {"$exists": True},
            "limitPosition_data": {"$exists": True},
            "basePosition_ticksLower": {"$exists": True},
            "limitPosition_ticksLower": {"$exists": True},
            "basePosition_ticksUpper": {"$exists": True},
            "limitPosition_ticksUpper": {"$exists": True},
        },
        limit=1,
    ):
        output.append(uncollected[0])
    else:
        logging.getLogger(__name__).debug(
            f"  found no 'uncollected = 0' cashuistic for {chain.database_name} {static_hypervisor['dex']} {static_hypervisor['symbol']}"
        )
        if brake_on_notfound:
            return []

    # 3rd out of range positions
    if out_of_range := get_from_localdb(
        network=chain.database_name,
        collection="status",
        aggregate=[
            {
                "$match": {
                    "address": static_hypervisor["address"],
                    "basePosition_data": {"$exists": True},
                    "limitPosition_data": {"$exists": True},
                    "basePosition_ticksLower": {"$exists": True},
                    "limitPosition_ticksLower": {"$exists": True},
                    "basePosition_ticksUpper": {"$exists": True},
                    "limitPosition_ticksUpper": {"$exists": True},
                }
            },
            {
                "$addFields": {
                    "deleteme": {
                        "currentTick": {"$toDecimal": "$currentTick"},
                        "baseLower": {"$toDecimal": "$baseLower"},
                        "baseUpper": {"$toDecimal": "$baseUpper"},
                        "limitLower": {"$toDecimal": "$limitLower"},
                        "limitUpper": {"$toDecimal": "$limitUpper"},
                    },
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$or": [
                            {"$gt": ["$deleteme.baseLower", "$deleteme.currentTick"]},
                            {"$lt": ["$deleteme.baseUpper", "$deleteme.currentTick"]},
                            {"$gt": ["$deleteme.limitLower", "$deleteme.currentTick"]},
                            {"$lt": ["$deleteme.limitUpper", "$deleteme.currentTick"]},
                        ]
                    }
                }
            },
            {"$unset": "deleteme"},
        ],
        limit=1,
    ):
        output.append(out_of_range[0])
    else:
        logging.getLogger(__name__).debug(
            f"  found no 'position out of range' cashuistic for {chain.database_name} {static_hypervisor['dex']} {static_hypervisor['symbol']}"
        )
        if brake_on_notfound:
            return []

    # 4th in range positions
    if in_range := get_from_localdb(
        network=chain.database_name,
        collection="status",
        aggregate=[
            {
                "$match": {
                    "address": static_hypervisor["address"],
                    "basePosition_data": {"$exists": True},
                    "limitPosition_data": {"$exists": True},
                    "basePosition_ticksLower": {"$exists": True},
                    "limitPosition_ticksLower": {"$exists": True},
                    "basePosition_ticksUpper": {"$exists": True},
                    "limitPosition_ticksUpper": {"$exists": True},
                }
            },
            {
                "$addFields": {
                    "deleteme": {
                        "currentTick": {"$toDecimal": "$currentTick"},
                        "baseLower": {"$toDecimal": "$baseLower"},
                        "baseUpper": {"$toDecimal": "$baseUpper"},
                        "limitLower": {"$toDecimal": "$limitLower"},
                        "limitUpper": {"$toDecimal": "$limitUpper"},
                    }
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$or": [
                            {
                                "$and": [
                                    {
                                        "$lte": [
                                            "$deleteme.baseLower",
                                            "$deleteme.currentTick",
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            "$deleteme.baseUpper",
                                            "$deleteme.currentTick",
                                        ]
                                    },
                                ]
                            },
                            {
                                "$and": [
                                    {
                                        "$lte": [
                                            "$deleteme.limitLower",
                                            "$deleteme.currentTick",
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            "$deleteme.limitUpper",
                                            "$deleteme.currentTick",
                                        ]
                                    },
                                ]
                            },
                        ]
                    }
                }
            },
            {"$unset": "deleteme"},
        ],
        limit=1,
    ):
        output.append(in_range[0])
    else:
        logging.getLogger(__name__).debug(
            f"  found no 'position in range' cashuistic for {chain.database_name} {static_hypervisor['dex']} {static_hypervisor['symbol']}"
        )
        if brake_on_notfound:
            return []

    # return
    return output


def get_cashuistics_many(
    chain: Chain, hypervisor_addresses: list[str], qtty_each: int = 1
) -> list[dict]:
    """Get all cashuistics for a given hypervisor address list
            Try get as different hypes as possible

    Args:
        chain (Chain): network
        hypervisor_addresses (list[str]):  list of hypervisor addresses
        qtty_each (int, optional): Quantity of cashuistics to return per each hypervisor. Defaults to 1.

    Returns:
        list[dict]: list of hype status
    """
    # select
    output = []
    _addresses_processed = {}
    # 1st uncollected > 0
    output += get_cashuistics_many_helper(
        get_from_localdb(
            network=chain.database_name,
            collection="status",
            find={
                "address": {"$in": hypervisor_addresses},
                "$or": [
                    {"fees_uncollected.qtty_token0": {"$ne": "0"}},
                    {"fees_uncollected.qtty_token1": {"$ne": "0"}},
                ],
                "basePosition_data": {"$exists": True},
                "limitPosition_data": {"$exists": True},
                "basePosition_ticksLower": {"$exists": True},
                "limitPosition_ticksLower": {"$exists": True},
                "basePosition_ticksUpper": {"$exists": True},
                "limitPosition_ticksUpper": {"$exists": True},
            },
            limit=int(10 * qtty_each),
        ),
        _addresses_processed,
        qtty=qtty_each,
    )
    # 2nd uncollected = 0
    output += get_cashuistics_many_helper(
        get_from_localdb(
            network=chain.database_name,
            collection="status",
            find={
                "address": {"$in": hypervisor_addresses},
                "$or": [
                    {"fees_uncollected.qtty_token0": "0"},
                    {"fees_uncollected.qtty_token1": "0"},
                ],
                "basePosition_data": {"$exists": True},
                "limitPosition_data": {"$exists": True},
                "basePosition_ticksLower": {"$exists": True},
                "limitPosition_ticksLower": {"$exists": True},
                "basePosition_ticksUpper": {"$exists": True},
                "limitPosition_ticksUpper": {"$exists": True},
            },
            limit=int(10 * qtty_each),
        ),
        _addresses_processed,
        qtty=qtty_each,
    )
    # 3rd out of range positions
    output += get_cashuistics_many_helper(
        get_from_localdb(
            network=chain.database_name,
            collection="status",
            aggregate=[
                {
                    "$match": {
                        "address": {"$in": hypervisor_addresses},
                        "basePosition_data": {"$exists": True},
                        "limitPosition_data": {"$exists": True},
                        "basePosition_ticksLower": {"$exists": True},
                        "limitPosition_ticksLower": {"$exists": True},
                        "basePosition_ticksUpper": {"$exists": True},
                        "limitPosition_ticksUpper": {"$exists": True},
                    }
                },
                {"$limit": 2000},
                {
                    "$addFields": {
                        "deleteme": {
                            "currentTick": {"$toDecimal": "$currentTick"},
                            "baseLower": {"$toDecimal": "$baseLower"},
                            "baseUpper": {"$toDecimal": "$baseUpper"},
                            "limitLower": {"$toDecimal": "$limitLower"},
                            "limitUpper": {"$toDecimal": "$limitUpper"},
                        },
                    }
                },
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {
                                    "$gt": [
                                        "$deleteme.baseLower",
                                        "$deleteme.currentTick",
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$deleteme.baseUpper",
                                        "$deleteme.currentTick",
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$deleteme.limitLower",
                                        "$deleteme.currentTick",
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$deleteme.limitUpper",
                                        "$deleteme.currentTick",
                                    ]
                                },
                            ]
                        }
                    }
                },
                {"$unset": "deleteme"},
            ],
            limit=int(10 * qtty_each),
        ),
        _addresses_processed,
        qtty=qtty_each,
    )
    # 4th in range positions
    output += get_cashuistics_many_helper(
        get_from_localdb(
            network=chain.database_name,
            collection="status",
            aggregate=[
                {
                    "$match": {
                        "address": {"$in": hypervisor_addresses},
                        "basePosition_data": {"$exists": True},
                        "limitPosition_data": {"$exists": True},
                        "basePosition_ticksLower": {"$exists": True},
                        "limitPosition_ticksLower": {"$exists": True},
                        "basePosition_ticksUpper": {"$exists": True},
                        "limitPosition_ticksUpper": {"$exists": True},
                    }
                },
                {"$limit": 2000},
                {
                    "$addFields": {
                        "deleteme": {
                            "currentTick": {"$toDecimal": "$currentTick"},
                            "baseLower": {"$toDecimal": "$baseLower"},
                            "baseUpper": {"$toDecimal": "$baseUpper"},
                            "limitLower": {"$toDecimal": "$limitLower"},
                            "limitUpper": {"$toDecimal": "$limitUpper"},
                        }
                    }
                },
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {
                                    "$and": [
                                        {
                                            "$lte": [
                                                "$deleteme.baseLower",
                                                "$deleteme.currentTick",
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                "$deleteme.baseUpper",
                                                "$deleteme.currentTick",
                                            ]
                                        },
                                    ]
                                },
                                {
                                    "$and": [
                                        {
                                            "$lte": [
                                                "$deleteme.limitLower",
                                                "$deleteme.currentTick",
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                "$deleteme.limitUpper",
                                                "$deleteme.currentTick",
                                            ]
                                        },
                                    ]
                                },
                            ]
                        }
                    }
                },
                {"$unset": "deleteme"},
            ],
            limit=int(10 * qtty_each),
        ),
        _addresses_processed,
        qtty=qtty_each,
    )

    # return
    return output


def get_cashuistics_many_helper(hypervisors: list, addresses: dict, qtty: int = 1):
    output = []

    # shuffle
    random.shuffle(hypervisors)
    # process
    for idx, hype in enumerate(hypervisors):
        if hype["address"] not in addresses:
            # create
            addresses[hype["address"]] = [hype["block"]]
            # add to output
            output.append(hype)
        elif (
            hype["address"] in addresses
            and hype["block"] not in addresses[hype["address"]]
        ):
            # add to output
            output.append(hype)
            # add block
            addresses[hype["address"]].append(hype["block"])

        elif idx == len(hypervisors) - 1 and len(output) == 0:
            # add this one as the only possible
            # create
            addresses[hype["address"]] = [hype["block"]]
            # add to output
            output.append(hype)

        # check if we already have enough
        if len(output) >= qtty:
            break

    # return
    return output


####   COMPARE     ####


def compare_dictionaries(dict1: dict, dict2: dict) -> tuple[bool, str]:
    """Compare two dictionaries recursively

    Args:
        dict1 (dict): dictionary 1
        dict2 (dict): dictionary 2

    Returns:
        tuple[bool,str]::   True if both dictionaries are equal, False otherwise
                            str field where the difference was found
    """

    if len(dict1) != len(dict2):
        return False, "_lenght_"

    for key in dict1:
        # check if key is in dict2
        if key not in dict2:
            return False, key

        if isinstance(dict1[key], dict):
            # add field
            _sub_result, _subField = compare_dictionaries(dict1[key], dict2[key])
            if not _sub_result:
                return False, ".".join({".".join([key, _subField])})
        else:
            if not compare_types(dict1[key], dict2[key]):
                logging.getLogger(__name__).error(
                    f"  {key}  different types !!!->>  {dict1[key]} != {dict2[key]} "
                )
                return False, key
            elif dict1[key] != dict2[key]:
                return False, key

    return True, key


def compare_types(value1, value2) -> bool:
    """Compare two values types

    Args:
        value1 (any): value 1
        value2 (any): value 2

    Returns:
        bool: True if both values are of the same type, False otherwise
    """
    return type(value1) == type(value2)
