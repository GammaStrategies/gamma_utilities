from datetime import datetime, timezone
import logging
from apps.feeds.returns.builds import (
    force_build_period_yield,
    save_hypervisor_returns_to_database,
)
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain, text_to_chain


def repair_hypervisor_returns():
    """Repair hypervisor returns"""
    #
    # override networks if specified in cml
    for chain in [
        text_to_chain(x)
        for x in (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
        )
    ]:

        # repair null values
        repair_null_values(chain=chain, collection="hypervisor_returns", remove=True)
        repair_null_values(
            chain=chain, collection="latest_hypervisor_returns", remove=True
        )
        # remove old latest hypervisor returns
        remove_old_latest_hypervisor_returns(chain=chain)


# HELPERS
def remove_direct_transfers(chain: Chain, hypervisor_address: str, before_block: int):
    """
    Remove all hypervisor return items before a block related to a direct gamma transfer to fix token weights. ( sporadically done at the initial stage of a hypervisor life)
    """
    # get direct transfers
    data_to_remove = get_from_localdb(
        network=chain.database_name,
        collection="hypervisor_returns",
        find={"address": hypervisor_address, "block": {"$lte": before_block}},
        sort={"timeframe.ini.block": 1},
    )
    data_left = get_from_localdb(
        network=chain.database_name,
        collection="hypervisor_returns",
        find={"address": hypervisor_address, "block": {"$gt": before_block}},
        sort={"timeframe.ini.block": 1},
    )

    start_datetime = datetime.fromtimestamp(
        data_to_remove[0]["timeframe"]["ini"]["timestamp"]
    )
    end_datetime = datetime.fromtimestamp(
        data_to_remove[-1]["timeframe"]["end"]["timestamp"]
    )

    days = (
        data_to_remove[-1]["timeframe"]["end"]["timestamp"]
        - data_to_remove[0]["timeframe"]["ini"]["timestamp"]
    ) / (60 * 60 * 24)

    logging.getLogger(__name__).info(
        f" Removing {len(data_to_remove)}[{len(data_to_remove)/(len(data_left)+len(data_to_remove)):,.1%} of total] direct transfers from hypervisor returns {hypervisor_address} corresponding to {days:,.1f} days from {start_datetime} to {end_datetime}"
    )

    # remove items
    db_return = get_default_localdb(network=chain.database_name).delete_items(
        collection_name="hypervisor_returns",
        data=data_to_remove,
    )
    logging.getLogger(__name__).info(f" Removed {db_return.deleted_count} items ")


def repair_null_values(
    chain: Chain, collection: str = "hypervisor_returns", remove: bool = False
):
    """Get all hypervisors with null underlying values or prices and try to repair them by recreating and saving them to the database

    Args:
        chain (Chain):
        collection (str, optional): "hypervisor_returns" or "latest_hypervisor_returns". Defaults to "hypervisor_returns".
        remove (bool, optional): Should remove item when not repaired?. Defaults to False.
    """
    logging.getLogger(__name__).info(
        f" {chain.database_name} repairing null values from {collection} by recreating them"
    )

    wrong_items = get_from_localdb(
        network=chain,
        collection=collection,
        aggregate=[
            {
                "$match": {
                    "$or": [
                        {"status.end.prices.token1": None},
                        {"status.end.underlying.qtty.token1": None},
                        {"status.end.prices.token0": None},
                        {"status.end.underlying.qtty.token0": None},
                        {"status.ini.prices.token1": None},
                        {"status.ini.underlying.qtty.token1": None},
                        {"status.ini.prices.token0": None},
                        {"status.ini.underlying.qtty.token0": None},
                    ]
                }
            }
        ],
    )

    logging.getLogger(__name__).debug(
        f" {chain.database_name} found {len(wrong_items)} items"
    )
    for item in wrong_items:

        # try repairing
        if repaired := force_build_period_yield(
            chain=chain,
            hypervisor_address=item["address"],
            block_ini=item["timeframe"]["ini"]["block"],
            block_end=item["timeframe"]["end"]["block"],
        ):

            _todict = [x.to_dict() for x in repaired]

            # make sure its not equal to the original
            if (
                not repaired[0].status.end.underlying.qtty.token0
                or not repaired[0].status.end.underlying.qtty.token1
                or not repaired[0].status.ini.underlying.qtty.token0
                or not repaired[0].status.ini.underlying.qtty.token1
            ):
                logging.getLogger(__name__).error(
                    f" {chain.database_name} could not repair {item['id']} -> underlying qtty is zero. hype: {item['address']} blocks from {item['timeframe']['ini']['block']} to {item['timeframe']['end']['block']}"
                )
                if remove:
                    if db_return := get_default_localdb(
                        network=chain.database_name
                    ).delete_item(data=item, collection_name=collection):
                        logging.getLogger(__name__).info(
                            f" Removed {db_return.deleted_count} items "
                        )
                    else:
                        logging.getLogger(__name__).error(
                            f" {chain.database_name} could not remove {item['id']} -> underlying qtty is zero. hype: {item['address']} blocks from {item['timeframe']['ini']['block']} to {item['timeframe']['end']['block']}"
                        )
                continue

            elif (
                not repaired[0].status.end.prices.token0
                or not repaired[0].status.end.prices.token1
                or not repaired[0].status.ini.prices.token0
                or not repaired[0].status.ini.prices.token1
            ):
                logging.getLogger(__name__).error(
                    f" {chain.database_name} could not repair {item['id']} -> prices are zero. hype: {item['address']} blocks from {item['timeframe']['ini']['block']} to {item['timeframe']['end']['block']}"
                )
                if remove:
                    if db_return := get_default_localdb(
                        network=chain.database_name
                    ).delete_item(data=item, collection_name=collection):
                        logging.getLogger(__name__).info(
                            f" Removed {db_return.deleted_count} items "
                        )
                    else:
                        logging.getLogger(__name__).error(
                            f" {chain.database_name} could not remove {item['id']} -> prices are zero. hype: {item['address']} blocks from {item['timeframe']['ini']['block']} to {item['timeframe']['end']['block']}"
                        )
                continue

            # save converted to dict results to database
            save_hypervisor_returns_to_database(
                chain=chain,
                period_yield_list=_todict,
            )

        else:
            logging.getLogger(__name__).error(
                f" {chain.database_name} could not repair {item['id']} -> underlying qtty and/or price are zero. hype: {item['address']} blocks from {item['timeframe']['ini']['block']} to {item['timeframe']['end']['block']}"
            )


def remove_old_latest_hypervisor_returns(
    chain: Chain, hypervisor_address: str | None = None, max_period=365
):
    """Remove old latest hypervisor returns from the database

    Args:
        chain (Chain):
        hypervisor_address (str | None, optional): . Defaults to all.
        max_period (int, optional): maximum days from utc now to be kept in database. Defaults to 365.
    """

    min_timestamp = datetime.now(tz=timezone.utc).timestamp() - (
        max_period * 24 * 60 * 60
    )

    _find = {"timeframe.ini.block": {"$lt": min_timestamp}}
    if hypervisor_address:
        _find["address"] = hypervisor_address

    if items := get_from_localdb(
        network=chain.database_name,
        collection="latest_hypervisor_returns",
        find=_find,
        sort={"timeframe.ini.block": -1},
    ):
        logging.getLogger(__name__).info(
            f" Removing {len(items)} items from {chain.database_name} latest_hypervisor_returns older than {max_period} days [ {datetime.fromtimestamp(min_timestamp,tz=timezone.utc)} ]"
        )
        db_return = get_default_localdb(network=chain.database_name).delete_items(
            collection_name="latest_hypervisor_returns",
            data=items,
        )
        logging.getLogger(__name__).debug(f" Removed {db_return.deleted_count} items ")
