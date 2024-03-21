from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
import logging
import tqdm
from apps.repair.prices.outliers import rescrape_price_from_dbItem
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)
from bins.general.enums import Chain


def repair_prices_analytic_returns(
    chain: Chain, token_address: str, block_ini: int, block_end: int
):

    # 1) Repair database prices, when wrong ( rescrape)
    _find = {
        "address": token_address,
        "network": chain.database_name,
        "block": {"$gte": block_ini, "$lte": block_end},
    }
    rescrape_prices_for_token(
        chain=chain,
        find=_find,
        sort=[("block", -1)],
        multiprocess=True,
        price_divergence=0.01,
    )

    # 2) Repair hypervisor returns prices
    # find hypervisor addresses containing token_address
    hypervisor_static_list = get_from_localdb(
        network=chain.database_name,
        collection="static",
        find={
            "$or": [
                {"pool.token0.address": token_address},
                {"pool.token1.address": token_address},
            ]
        },
    )
    if not hypervisor_static_list:
        logging.getLogger(__name__).warning(
            f" No hypervisors found for token address {token_address} "
        )
        return
    # convert hype list in easier to use format
    hypervisor_static_list = {x["address"]: x for x in hypervisor_static_list}

    # find all hypervisor returns within the block range ( block is end block)
    hypervisor_returns = get_from_localdb(
        network=chain.database_name,
        collection="hypervisor_returns",
        find={
            "address": {"$in": list(hypervisor_static_list.keys())},
            "timeframe.end.block": {"$gte": block_ini, "$lte": block_end},
        },
    )
    logging.getLogger(__name__).debug(
        f" Found {len(hypervisor_returns)} hypervisor returns matching the token criteria specified"
    )

    # change those proces to the correct ones
    _prices = {
        x["block"]: Decimal(str(x["price"]))
        for x in get_default_globaldb().get_items_from_database(
            collection_name="usd_prices",
            find={
                "address": token_address,
                "network": chain.database_name,
                "block": {"$gte": block_ini, "$lte": block_end},
            },
        )
    }

    items_to_update = []
    for hype_return in hypervisor_returns:

        # define token 0 or 1
        token_0o1 = None
        if (
            hypervisor_static_list[hype_return["address"]]["pool"]["token0"]["address"]
            == token_address
        ):
            token_0o1 = "token0"
        elif (
            hypervisor_static_list[hype_return["address"]]["pool"]["token1"]["address"]
            == token_address
        ):
            token_0o1 = "token1"
        else:
            raise ValueError(
                " Token address not found in hypervisor pool. This should never happen"
            )

        # get the price for the ini block
        _should_update = False
        if price_ini := _prices.get(hype_return["timeframe"]["ini"]["block"]):
            if hype_return["status"]["ini"]["prices"][token_0o1] != price_ini:
                hype_return["status"]["ini"]["prices"][token_0o1] = price_ini
                _should_update = True

        # get the price for the end block
        if price_end := _prices.get(hype_return["timeframe"]["end"]["block"]):
            if hype_return["status"]["end"]["prices"][token_0o1] != price_end:
                hype_return["status"]["end"]["prices"][token_0o1] = price_end
                _should_update = True

        # add to update list
        if _should_update:
            items_to_update.append(
                get_default_globaldb().convert_decimal_to_d128(hype_return)
            )

    # update the hypervisor returns
    if items_to_update:
        logging.getLogger(__name__).info(
            f" Updating {len(items_to_update)} hypervisor returns prices"
        )
        db_return = get_default_localdb(
            network=chain.database_name
        ).replace_items_to_database(
            data=items_to_update, collection_name="hypervisor_returns"
        )
        logging.getLogger(__name__).debug(
            f" {db_return.modified_count} hypervisor returns updated"
        )


# HELPER
def rescrape_prices_for_token(
    chain: Chain,
    find: dict,
    sort=[("block", 1)],
    limit: int | None = None,
    multiprocess: bool = True,
    price_divergence: float = 0.1,
):
    # get prices to rescrape
    price_items_db = get_default_globaldb().get_items_from_database(
        collection_name="usd_prices", find=find, sort=sort, limit=limit
    )

    with tqdm.tqdm(total=len(price_items_db)) as progress_bar:
        # prepare arguments
        _fails = 0

        if multiprocess:
            with ProcessPoolExecutor(max_workers=10) as ex:
                for result in ex.map(rescrape_price_from_dbItem, price_items_db):
                    if not result:
                        _fails += 1
                    progress_bar.set_description(
                        f" Rescraping {chain.fantasy_name} price: {_fails} not processed"
                    )
                    progress_bar.update(1)
        else:
            for i in price_items_db:
                if not rescrape_price_from_dbItem(
                    priceDBitem=i, price_divergence=price_divergence
                ):
                    _fails += 1
                progress_bar.set_description(
                    f" Rescraping {chain.fantasy_name} price: {_fails} not processed"
                )
                progress_bar.update(1)
