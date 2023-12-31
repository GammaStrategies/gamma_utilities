import logging

import tqdm
from apps.database_checker import get_price
from apps.database_reScrape import manual_reScrape, reScrape_loopWork_rewards_status
from bins.database.helpers import get_default_globaldb, get_from_localdb
from bins.formulas.general import itentify_valid_and_outliers
from bins.general.enums import Chain
from bins.mixed.price_utilities import price_scraper


def repair_all_outlier_prices_from_pricedb(
    chain: Chain, token_addresses: list[str] | None = None, limit: int = 100
) -> list[dict]:
    # get all unique addrsses from global database
    _query = [
        {"$match": {"network": chain.database_name}},
        {"$group": {"_id": "$address"}},
    ]
    _token_addresses_list = token_addresses or [
        x["_id"]
        for x in get_default_globaldb().get_items_from_database(
            collection_name="usd_prices", aggregate=_query
        )
    ]

    logging.getLogger(__name__).info(
        f" Found {len(_token_addresses_list)} tokens to check outliers for"
    )

    # find outliers for each of the token addresses, using monthly chunks of data ( more specifically, using blocks corresponding to 30 days )
    for _token_address in tqdm.tqdm(_token_addresses_list):
        # get all price std deviations > 1
        _query = [
            {"$match": {"network": chain.database_name, "address": _token_address}},
            {
                "$setWindowFields": {
                    "partitionBy": "$address",
                    "sortBy": {"block": 1},
                    "output": {
                        "stdeviation": {
                            "$stdDevSamp": "$price",
                            "window": {"range": [-35, 10]},
                        },
                        # "average": {"$avg": "$price", "window": {"range": [-35, 10]}},
                    },
                }
            },
            # {
            #     "$addFields": {
            #         "zScore": {
            #             "$cond": [
            #                 {"$eq": ["$stdeviation", 0]},
            #                 0,
            #                 {
            #                     "$divide": [
            #                         {"$subtract": ["$price", "$average"]},
            #                         "$stdeviation",
            #                     ]
            #                 },
            #             ]
            #         }
            #     }
            # },
            {"$sort": {"stdeviation": -1}},
            {"$limit": limit},
        ]

        _token_prices_list = get_default_globaldb().get_items_from_database(
            collection_name="usd_prices", aggregate=_query
        )

        if _token_prices_list:
            logging.getLogger(__name__).debug(
                f" Found {len(_token_prices_list)} price outliers for {_token_address} in {chain.database_name}. Rescraping..."
            )
        else:
            logging.getLogger(__name__).debug(
                f" No price outliers found for {_token_address} in {chain.database_name}. Skipping..."
            )
            continue

        # rescrape price and log if difference
        for token_data in _token_prices_list:
            _price, _source = get_price(
                network=chain.database_name,
                token_address=_token_address,
                block=token_data["block"],
            )

            if _price is None:
                logging.getLogger(__name__).warning(
                    f"Could not find price for {_token_address} at block {token_data['block']}"
                )
                continue

            # calculate diffrerence
            _price_difference = abs(_price - token_data["price"]) / token_data["price"]

            # log and save if difference > 3%
            if abs(_price - token_data["price"]) / token_data["price"] > 0.03:
                logging.getLogger(__name__).debug(
                    f" Significant price difference for {_token_address} at block {token_data['block']} was found: {_price_difference:,.0%} -> old {token_data['price']:,.2f} vs new {_price:,.2f}"
                )

                # save new price to database
                token_data["price"] = _price
                token_data["source"] = _source
                if db_return := get_default_globaldb().replace_item_to_database(
                    collection_name="usd_prices",
                    data=token_data,
                ):
                    logging.getLogger(__name__).debug(
                        f"Successfully replaced price for {_token_address} at block {token_data['block']}: {db_return.raw_result}"
                    )
                else:
                    logging.getLogger(__name__).warning(
                        f"Could not replace price for {_token_address} at block {token_data['block']}. No successfull result received from database"
                    )
            else:
                logging.getLogger(__name__).debug(
                    f" No significant price difference for {_token_address} at block {token_data['block']} was found: {_price_difference:,.0%}"
                )


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
                            "window": {"range": [-35, 10]},
                        },
                        "average": {"$avg": "$apr", "window": {"range": [-35, 10]}},
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
