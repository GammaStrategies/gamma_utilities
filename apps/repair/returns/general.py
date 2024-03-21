from datetime import datetime
import logging
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain


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
