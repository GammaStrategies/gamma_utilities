import logging
from concurrent.futures import ProcessPoolExecutor
import random
import tqdm
from apps.database_checker import get_price
from apps.database_reScrape import manual_reScrape, reScrape_loopWork_rewards_status
from bins.database.helpers import get_default_globaldb, get_from_localdb
from bins.formulas.general import itentify_valid_and_outliers
from bins.general.enums import Chain
from bins.mixed.price_utilities import price_scraper


def repair_all_outlier_prices_from_rewardsdb(
    chain: Chain, hypervisor_addresses: list[str] | None = None, limit: int = 100
) -> list[dict]:
    # get all static rewards
    _find = (
        {}
        if hypervisor_addresses is None
        else {"hypervisor_address": {"$in": hypervisor_addresses}}
    )
    _static_rewards_list = get_from_localdb(
        network=chain.database_name, collection="rewards_static", find=_find
    )

    logging.getLogger(__name__).info(
        f" Found {len(_static_rewards_list)} static rewards to check price outliers for"
    )

    # find outliers for each of the token addresses, using monthly chunks of data ( more specifically, using blocks corresponding to 30 days )
    for _static_reward in _static_rewards_list:
        # get all price std deviations > 1
        _query = [
            {
                "$match": {
                    "hypervisor_address": _static_reward["hypervisor_address"],
                }
            },
            {
                "$setWindowFields": {
                    "partitionBy": "$rewardToken",
                    "sortBy": {"block": 1},
                    "output": {
                        "stdeviation": {
                            "$stdDevSamp": "$apr",
                            "window": {"documents": [-100, "current"]},
                        },
                        "average": {
                            "$avg": "$apr",
                            "window": {"documents": [-100, "current"]},
                        },
                    },
                }
            },
            {
                "$addFields": {
                    "zScore": {
                        "$cond": [
                            {"$eq": ["$stdeviation", 0]},
                            0,
                            {
                                "$divide": [
                                    {"$subtract": ["$price", "$average"]},
                                    "$stdeviation",
                                ]
                            },
                        ]
                    }
                }
            },
            {"$sort": {"stdeviation": -1}},
            {"$limit": limit},
        ]

        _rewards_status_list = get_from_localdb(
            network=chain.database_name, collection="rewards_status", aggregate=_query
        )

        if _rewards_status_list:
            logging.getLogger(__name__).debug(
                f" Found {len(_rewards_status_list)} rewards status APR outliers for {hypervisor_addresses} in {chain.database_name}. Rescraping..."
            )
        else:
            logging.getLogger(__name__).debug(
                f" No rewards status APR outliers found for {hypervisor_addresses} in {chain.database_name}. Skipping..."
            )
            continue

        # rescrape and log if difference
        _ids_to_rescrape = [x["id"] for x in _rewards_status_list]

        manual_reScrape(
            chain=chain,
            loop_work=reScrape_loopWork_rewards_status,
            find={"id": {"$in": _ids_to_rescrape}},
            sort=[("block", -1)],
            db_collection="rewards_status",
            threaded=True,
            rewrite=True,
        )


# NEW
def repair_all_outlier_prices_from_pricedb(
    chain: Chain | None = None,
    token_addresses: list[str] | None = None,
    limit: int | None = None,
    multiprocess: bool = False,
):
    """_summary_

    Args:
        chain (Chain | None, optional): . Defaults to None.
        token_addresses (list[str] | None, optional): list of token addresses. Defaults to None.
        limit (int | None, optional): Limit the maximum amount of items to process. Defaults to None.
        multiprocess (bool, optional): . Defaults to False.

    Returns:
        _type_: _description_
    """

    # get outliers
    outliers = get_default_globaldb().get_items_from_database(
        collection_name="usd_prices",
        aggregate=query_price_outliers(
            chain=chain, token_addresses=token_addresses, limit=limit
        ),
    )

    logging.getLogger(__name__).info(
        f" Found {len(outliers)} outlier prices in {chain.database_name}"
    )

    if multiprocess:
        with tqdm.tqdm(total=len(outliers)) as progress_bar:
            # prepare arguments
            _fails = 0
            with ProcessPoolExecutor(max_workers=10) as ex:
                for result in ex.map(rescrape_price_from_outlier, outliers):
                    if not result:
                        _fails += 1
                    progress_bar.set_description(
                        f" Rescraping {chain.fantasy_name} price outliers: {_fails} not processed"
                    )
                    progress_bar.update(1)

        # exit multiprocess mode
        return

    # find outliers for each of the token addresses, using monthly chunks of data ( more specifically, using blocks corresponding to 30 days )
    for outlier in tqdm.tqdm(outliers):
        # rescrape price and log if difference
        _price, _source = get_price(
            network=outlier["network"],
            token_address=outlier["address"],
            block=outlier["block"],
        )

        if _price is None:
            logging.getLogger(__name__).warning(
                f" Could not find price for {outlier['network']} {outlier['address']} at block {outlier['block']}"
            )
            continue

        # calculate difference
        # _price_difference = (outlier["average"] - _price) / outlier["average"]
        _price_difference = (outlier["price"] - _price) / outlier["price"]

        # if there is a big difference

        # check if price is far from the average
        if abs(_price_difference) <= 0.1:
            logging.getLogger(__name__).debug(
                f" No significant price difference found for {outlier['network']} {outlier['address']} at block {outlier['block']} was found: {_price_difference:.0%} -> old {outlier['price']:.2f} vs new {_price:.2f}. Skipping."
            )
            continue

        logging.getLogger(__name__).debug(
            f" Saving new price for {outlier['network']} {outlier['address']} at block {outlier['block']}: {_price:.2f}  [old: {outlier['price']:.2f}]"
        )
        # save new price to database
        token_data = {
            "id": outlier["id"],
            "network": outlier["network"],
            "address": outlier["address"],
            "block": outlier["block"],
            "price": _price,
            "source": _source,
        }
        # save price to database
        if db_return := get_default_globaldb().replace_item_to_database(
            collection_name="usd_prices",
            data=token_data,
        ):
            logging.getLogger(__name__).debug(
                f" Successfully replaced price for {outlier['network']} {outlier['address']} at block {outlier['block']}: {db_return.raw_result}"
            )
        else:
            logging.getLogger(__name__).warning(
                f" Could not save price to database for {outlier['network']} {outlier['address']} at block {token_data['block']}. No successfull result received from database"
            )


def rescrape_price_from_outlier(outlier: dict) -> bool:
    # rescrape price and log if difference
    _price, _source = get_price(
        network=outlier["network"],
        token_address=outlier["address"],
        block=outlier["block"],
    )

    if _price is None:
        logging.getLogger(__name__).warning(
            f" Could not find price for {outlier['network']} {outlier['address']} at block {outlier['block']}"
        )
        return False

    # calculate difference
    # _price_difference = (outlier["average"] - _price) / outlier["average"]
    _price_difference = (outlier["price"] - _price) / outlier["price"]

    # if there is a big difference

    # check if price is far from the average
    if abs(_price_difference) <= 0.1:
        logging.getLogger(__name__).debug(
            f" No significant price difference found for {outlier['network']} {outlier['address']} at block {outlier['block']} was found: {_price_difference:.0%} -> old {outlier['price']:.2f} vs new {_price:.2f}. Skipping."
        )
        return False

    logging.getLogger(__name__).debug(
        f" Saving new price for {outlier['network']} {outlier['address']} at block {outlier['block']}: {_price:.2f}  [old: {outlier['price']:.2f}]"
    )
    # save new price to database
    token_data = {
        "id": outlier["id"],
        "network": outlier["network"],
        "address": outlier["address"],
        "block": outlier["block"],
        "price": _price,
        "source": _source,
    }
    # save price to database
    if db_return := get_default_globaldb().replace_item_to_database(
        collection_name="usd_prices",
        data=token_data,
    ):
        logging.getLogger(__name__).debug(
            f" Successfully replaced price for {outlier['network']} {outlier['address']} at block {outlier['block']}: {db_return.raw_result}"
        )
        return True
    else:
        logging.getLogger(__name__).warning(
            f" Could not save price to database for {outlier['network']} {outlier['address']} at block {token_data['block']}. No successfull result received from database"
        )
        return False


def rescrape_price_from_dbItem(priceDBitem: dict) -> bool:
    # rescrape price and log if difference
    _price, _source = get_price(
        network=priceDBitem["network"],
        token_address=priceDBitem["address"],
        block=priceDBitem["block"],
    )

    if _price is None:
        logging.getLogger(__name__).warning(
            f" Could not find price for {priceDBitem['network']} {priceDBitem['address']} at block {priceDBitem['block']}"
        )
        return False

    # calculate difference
    # _price_difference = (priceDBitem["average"] - _price) / priceDBitem["average"]
    _price_difference = (priceDBitem["price"] - _price) / priceDBitem["price"]

    # if there is a big difference

    # check if price is far from the average
    if abs(_price_difference) <= 0.05:
        logging.getLogger(__name__).debug(
            f" No significant price difference found for {priceDBitem['network']} {priceDBitem['address']} at block {priceDBitem['block']} was found: {_price_difference:.0%} -> old {priceDBitem['price']:.2f} vs new {_price:.2f}. Skipping."
        )
        # TODO: save price in cached rescraped prices as no significance found ( so that we don't rescrape it again)
        return False

    logging.getLogger(__name__).debug(
        f" Saving new price for {priceDBitem['network']} {priceDBitem['address']} at block {priceDBitem['block']}: {_price:.2f}  [old: {priceDBitem['price']:.2f}]"
    )
    # save new price to database
    token_data = {
        "id": priceDBitem["id"],
        "network": priceDBitem["network"],
        "address": priceDBitem["address"],
        "block": priceDBitem["block"],
        "price": _price,
        "source": _source,
    }
    # save price to database
    if db_return := get_default_globaldb().replace_item_to_database(
        collection_name="usd_prices",
        data=token_data,
    ):
        logging.getLogger(__name__).debug(
            f" Successfully replaced price for {priceDBitem['network']} {priceDBitem['address']} at block {priceDBitem['block']}: {db_return.raw_result}"
        )
        return True
    else:
        logging.getLogger(__name__).warning(
            f" Could not save price to database for {priceDBitem['network']} {priceDBitem['address']} at block {token_data['block']}. No successfull result received from database"
        )
        return False


def repair_prices_are_arrays():
    """Repair prices that are saved as arrays in the database"""
    items_to_save = []
    for itm in get_default_globaldb().get_items_from_database(
        collection_name="usd_prices",
        find={"price": {"$type": 4}},
        batch_size=100000,
    ):
        itm["price"] = (
            float(itm["price"][0])
            if isinstance(itm["price"][0], float)
            else itm["price"][1] if isinstance(itm["price"][1], float) else 0
        )

        if itm["price"] not in [0, None]:
            items_to_save.append(itm)

    if items_to_save:
        if db_return := get_default_globaldb().replace_items_to_database(
            data=items_to_save,
            collection_name="usd_prices",
        ):
            logging.getLogger(__name__).debug(
                f" PRICES as array REPAIRED ->  del:{db_return.deleted_count} ins:{db_return.inserted_count} match:{db_return.matched_count} mod:{db_return.modified_count} ups:{db_return.upserted_count}"
            )
        else:
            logging.getLogger(__name__).error(
                f" Failed to save repaired prices as array for {len(items_to_save)} items"
            )


def query_price_outliers_original(
    chain: Chain | None = None,
    token_addresses: list[str] | None = None,
    # average_range: list[int] = [-200, "current"],
    price_disctance: float = 1,
    limit: int | None = None,
) -> list[dict]:
    """Query for price outliers+

    Args:
        chain (Chain | None, optional): . Defaults to None.
        token_addresses (list[str] | None, optional): . Defaults to None.
        average_range (list[int], optional): number of documents before and after to create the average price. Defaults to [-5, 5].
        price_disctance (float, optional): How far the price should be from the average to be included as outlier. Defaults to 0.1.
        limit (int | None, optional): limit the number of returned items. Defaults to None.

    """

    # tokens list with problems to avoid
    problematic_tokens: dict[Chain, list] = {
        Chain.ETHEREUM: [
            "0x04f2694c8fcee23e8fd0dfea1d4f5bb8c352111f".lower(),  # sOHM no price
        ]
    }

    price_disctance = abs(price_disctance)

    _match = {}
    if chain:
        _match["network"] = chain.database_name

    if token_addresses:
        _match["address"] = {"$in": token_addresses}

    # Problematic tokens:
    if chain and chain in problematic_tokens and not token_addresses:
        # add problematic tokens to the match
        _match["$and"] = [
            {"address": {"$nin": problematic_tokens[chain]}},
        ]
    if not chain and not token_addresses and problematic_tokens:
        # add problematic tokens to the match
        _match["$and"] = [
            {
                "$and": [
                    {"address": {"$nin": _lst}},
                    {"network": {"$ne": _ch.database_name}},
                ]
            }
            for _ch, _lst in problematic_tokens.items()
        ]

    query = [
        {"$match": _match},
        {
            "$setWindowFields": {
                "partitionBy": "$address",
                "sortBy": {"block": 1},
                "output": {
                    "stdeviation": {
                        "$stdDevSamp": "$price",
                        "window": {"range": ["current", 200]},
                    },
                    "average": {
                        "$avg": "$price",
                        "window": {"range": ["current", 200]},
                    },
                    # average short term
                    "average_m": {"$avg": "$price", "window": {"range": [-5, 5]}},
                },
            }
        },
        {
            "$addFields": {
                "zScore1": {
                    "$cond": [
                        {"$eq": ["$average", 0]},
                        0,
                        {
                            "$abs": {
                                "$divide": [
                                    {"$subtract": ["$price", "$average"]},
                                    "$price",
                                ]
                            }
                        },
                    ]
                },
                "zScore2": {
                    "$cond": [
                        {"$eq": ["$stdeviation", 0]},
                        0,
                        {
                            "$abs": {
                                "$divide": [
                                    {"$subtract": ["$price", "$stdeviation"]},
                                    "$price",
                                ]
                            }
                        },
                    ]
                },
                "zScore3": {
                    "$cond": [
                        {"$eq": ["$average_m", 0]},
                        0,
                        {
                            "$abs": {
                                "$divide": [
                                    {"$subtract": ["$price", "$average_m"]},
                                    "$price",
                                ]
                            }
                        },
                    ]
                },
            }
        },
        {
            "$match": {
                "$or": [
                    {"zScore1": {"$gte": 1.5}},
                    {"zScore2": {"$gte": 1}},
                    {"zScore3": {"$gte": 0.5}},
                ]
            }
        },
        # {"$addFields": {"sortfield": {"$sum": ["$zScore1", "$zScore2", "$zScore3"]}}},
        # sort by the highest zScore, (one randomly selected)
        {"$sort": {f"zScore{random.randrange(1,4)}": -1}},
    ]

    if limit:
        query.append({"$limit": limit})

    return query


def query_price_outliers(
    chain: Chain | None = None,
    token_addresses: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Query for price outliers

    Args:
        chain (Chain | None, optional): . Defaults to None.
        token_addresses (list[str] | None, optional): . Defaults to None.
        limit (int | None, optional): limit the number of returned items. Defaults to None.

    """

    # tokens list with problems to avoid
    problematic_tokens: dict[Chain, list] = {
        Chain.ETHEREUM: [
            "0x04f2694c8fcee23e8fd0dfea1d4f5bb8c352111f".lower(),  # sOHM no price
            "0xf4dc48d260c93ad6a96c5ce563e70ca578987c74".lower(),  # BABEL
        ]
    }

    _match = {}
    if chain:
        _match["network"] = chain.database_name

    if token_addresses:
        _match["address"] = {"$in": token_addresses}

    # Problematic tokens:
    if chain and chain in problematic_tokens and not token_addresses:
        # add problematic tokens to the match
        _match["$and"] = [
            {"address": {"$nin": problematic_tokens[chain]}},
        ]
    if not chain and not token_addresses and problematic_tokens:
        # add problematic tokens to the match
        _match["$and"] = [
            {
                "$and": [
                    {"address": {"$nin": _lst}},
                    {"network": {"$ne": _ch.database_name}},
                ]
            }
            for _ch, _lst in problematic_tokens.items()
        ]

    query = [
        {"$match": _match},
        {
            "$setWindowFields": {
                "partitionBy": "$address",
                "sortBy": {"block": 1},
                "output": {
                    "stdeviation": {
                        "$stdDevSamp": "$price",
                        "window": {"range": ["current", 200]},
                    },
                    "average": {
                        "$avg": "$price",
                        "window": {"range": ["current", 200]},
                    },
                    "average_m": {"$avg": "$price", "window": {"range": [-5, 5]}},
                },
            }
        },
        {
            "$addFields": {
                "zScore1": {
                    "$cond": [
                        {"$eq": ["$average", 0]},
                        0,
                        {
                            "$abs": {
                                "$divide": [
                                    {"$subtract": ["$price", "$average"]},
                                    "$price",
                                ]
                            }
                        },
                    ]
                },
                "zScore2": {
                    "$cond": [
                        {"$eq": ["$stdeviation", 0]},
                        0,
                        {"$abs": {"$divide": ["$stdeviation", "$price"]}},
                    ]
                },
                "zScore3": {
                    "$cond": [
                        {"$eq": ["$average_m", 0]},
                        0,
                        {
                            "$abs": {
                                "$divide": [
                                    {"$subtract": ["$price", "$average_m"]},
                                    "$price",
                                ]
                            }
                        },
                    ]
                },
            }
        },
        {"$match": {"zScore2": {"$gte": 0.1}}},
        {"$sort": {"zScore2": -1}},
    ]

    if limit:
        query.append({"$limit": limit})

    return query
