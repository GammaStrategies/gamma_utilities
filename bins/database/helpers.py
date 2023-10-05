import logging
from apps.feeds.operations import feed_operations_hypervisors
from apps.feeds.utils import get_hypervisors_data_for_apr

from bins.errors.general import ProcessingError
from ..configuration import CONFIGURATION
from ..database.common.db_collections_common import database_global, database_local
from ..general.enums import Chain, error_identity, text_to_chain


def get_default_localdb(network: str) -> database_local:
    return database_local(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
        db_name=f"{network}_gamma",
    )


def get_default_globaldb() -> database_global:
    return database_global(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
    )


def get_from_localdb(network: str, collection: str, **kwargs) -> list:
    """get data from a local database type

    Args:
        network (str):
        collection (str):

    Returns:
        list: result
    """
    return get_default_localdb(network=network).get_items_from_database(
        collection_name=collection, **kwargs
    )


def get_price_from_db(
    network: str,
    block: int,
    token_address: str,
) -> float:
    """
    Get the price of a token at a specific block from database
    May return price of block -1 +1 if not found at block

    Args:
        network (str):
        block (int):
        token_address (str):

    Returns:
        float: usd price of token at block
    """
    # try get the prices from database
    global_db = database_global(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
    )

    if token_price := global_db.get_price_usd(
        network=network, block=block, address=token_address
    ):
        return token_price[0]["price"]

    # if price not found, check if block+1 block-1 has price ( because there is a low probability to high difference)
    if token_price := global_db.get_price_usd(
        network=network, block=block + 1, address=token_address
    ):
        logging.getLogger(__name__).warning(
            f" No price for {token_address} on {network} at block {block} has been found in database. Instead using price from block {block+1}"
        )
        return token_price[0]["price"]

    elif token_price := global_db.get_price_usd(
        network=network, block=block - 1, address=token_address
    ):
        logging.getLogger(__name__).warning(
            f" No price for {token_address} on {network} at block {block} has been found in database. Instead using price from block {block-1}"
        )
        return token_price[0]["price"]

    raise ProcessingError(
        chain=text_to_chain(network),
        item={"address": token_address, "block": block},
        identity=error_identity.PRICE_NOT_FOUND,
        action=f"scrape_price",
        message=f" No price for {token_address} on {network} at blocks {block}, {block+1} and {block-1} in database.",
    )
    # raise ValueError(
    #     f" No price for {token_address} on {network} at blocks {block}, {block+1} and {block-1} in database."
    # )


def get_prices_from_db(
    network: str,
    block: int,
    token_addresses: list[str],
) -> dict:
    # try get the prices from database
    if token_prices := get_default_globaldb().get_items_from_database(
        collection_name="usd_prices",
        find=dict(network=network, address={"$in": token_addresses}, block=block),
    ):
        return {price["address"]: price["price"] for price in token_prices}


def get_latest_price_from_db(network: str, token_address: str) -> float:
    # try get the prices from database
    if token_price := get_default_globaldb().get_items_from_database(
        collection_name="current_usd_prices",
        find=dict(network=network, address=token_address),
        sort=[("block", -1)],
        limit=1,
    ):
        return token_price[0]["price"]


def get_latest_prices_from_db(
    network: str, token_addresses: list[str] | None = None
) -> dict[float]:
    find = (
        dict(network=network, address={"$in": token_addresses})
        if token_addresses
        else dict(network=network)
    )
    # try get the prices from database
    if token_prices := get_default_globaldb().get_items_from_database(
        collection_name="current_usd_prices",
        find=find,
    ):
        return {price["address"]: price["price"] for price in token_prices}


# Handling hypervisor periods ( op.block -> op.block-1 -> op.block ...)
def execute_processes_within_hypervisor_periods(
    chain: Chain,
    hypervisor_address: str,
    ini_process_function: callable,
    end_process_function: callable,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    try_solve_errors: bool = False,
):
    # check if any of block or timestamp has been supplied
    if (not block_ini and not block_end) or (not timestamp_ini and not timestamp_end):
        raise ValueError("No block or timestamp has been supplied")

    # get data from database
    if data_list := get_hypervisors_data_for_apr(
        network=chain.database_name,
        hypervisor_addresses=[hypervisor_address],
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        block_ini=block_ini,
        block_end=block_end,
    ):
        # loop for each hypervisor data found { _id: <hypervisor_address> ,  {status: <hypervisor status data + operations + rewards...>}
        for hype_data in data_list:
            # check data consistency
            if not "status" in hype_data:
                raise ValueError(f"Invalid hypervisor data: {hype_data}")

            # check first item
            hypervisor_periods_first_item_checks(
                chain=chain,
                first_item=hype_data["status"][0],
                timestamp_ini=timestamp_ini,
                block_ini=block_ini,
                try_solve_errors=try_solve_errors,
            )

            ###### START #######
            # define loop working vars
            last_item = None
            last_item_type = None

            # loop thu each hype status data ( hypervisor status found for that particular time period )
            for idx, status_data in enumerate(hype_data["status"]):
                # define local vars for later use
                wanted_topics = ["deposit", "withdraw", "rebalance", "zeroBurn"]
                operation_types = [op["topic"] for op in status_data["operations"]]
                last_operation_types = (
                    [op["topic"] for op in last_item["operations"]] if last_item else []
                )

                # hypervisors without operations are end values
                if not status_data["operations"]:
                    # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<

                    # make sure last item was not an end value
                    if last_item_type != "ini":
                        # check if this is the first idx to discard it if it is
                        if idx == 0:
                            logging.getLogger(__name__).warning(
                                f" {chain.database_name} {status_data['address']} index {idx} is an end value but has no operations, because its a -1 block. Discarding it safely."
                            )
                            continue

                        raise ValueError(
                            f" {chain.database_name} {status_data['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {status_data['block']} last ops: {last_operation_types} current ops: {operation_types}"
                        )

                    # define last item type
                    last_item_type = "end"

                    # check totalSupply has not changed
                    hypervisors_periods_checkTotalSupplyChanged(
                        chain=chain,
                        hypervisor_address=hypervisor_address,
                        data=hype_data,
                        idx=idx,
                        last_item=last_item,
                        current_item=status_data,
                        try_solve_errors=try_solve_errors,
                    )

                    # check fee Growth
                    hypervisors_periods_checkfeeGrowths(
                        chain=chain,
                        hypervisor_address=hypervisor_address,
                        data=hype_data,
                        idx=idx,
                        last_item=last_item,
                        current_item=status_data,
                        try_solve_errors=try_solve_errors,
                    )

                    # execute process function
                    if end_process_function:
                        end_process_function(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                        )

                elif (
                    not any([x in wanted_topics for x in operation_types])
                    and last_item
                    and last_item["totalSupply"] == status_data["totalSupply"]
                ):
                    # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<
                    # because the initial query discards topics outside wanted topics, this is a correct end value.

                    # make sure last item was not an end value
                    if last_item_type != "ini":
                        raise ValueError(
                            f"  {chain.database_name} {status_data['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {status_data['block']} last ops: {last_operation_types} current ops: {operation_types}"
                        )
                    # define last item type
                    last_item_type = "end"

                    # check totalSupply has not changed
                    hypervisors_periods_checkTotalSupplyChanged(
                        chain=chain,
                        hypervisor_address=hypervisor_address,
                        data=hype_data,
                        idx=idx,
                        last_item=last_item,
                        current_item=status_data,
                        try_solve_errors=try_solve_errors,
                    )

                    # check fee Growth
                    hypervisors_periods_checkfeeGrowths(
                        chain=chain,
                        hypervisor_address=hypervisor_address,
                        data=hype_data,
                        idx=idx,
                        last_item=last_item,
                        current_item=status_data,
                        try_solve_errors=try_solve_errors,
                    )

                    # execute process function
                    if end_process_function:
                        end_process_function(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                        )

                else:
                    # it has operations:
                    # this is an initial value
                    if last_item_type and last_item_type != "end":
                        # check if they are consecutive blocks
                        if last_item["block"] + 1 == status_data["block"]:
                            logging.getLogger(__name__).warning(
                                f" {chain.database_name} {status_data['address']} index {idx} is an initial value but last item was considered initial also. Blocks are consecutive (noproblm). Last item block: {last_item['block']} current block: {status_data['block']}  last ops: {last_operation_types} current ops: {operation_types}"
                            )
                        else:
                            raise ValueError(
                                f"  {chain.database_name} {status_data['address']} index {idx} is an initial value but last item was considered initial also. Last item block: {last_item['block']} current block: {status_data['block']}  last ops: {last_operation_types} current ops: {operation_types}"
                            )
                    last_item_type = "ini"

                    # This should not have any topics outside wanted topics
                    if not any([x in wanted_topics for x in operation_types]):
                        raise ValueError(
                            f" {chain.database_name} {status_data['address']} index {idx} has not wanted operations {operation_types}"
                        )

                    # execute process function
                    if ini_process_function:
                        ini_process_function(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                        )

                last_item = status_data

                # if this is the last idx and last_item == ini, then we can decide wether to scrape a new item using the last block/timestamp defined or not
                if idx == len(hype_data["status"]) - 1 and last_item_type == "ini":
                    if block_end and last_item["block"] < block_end:
                        # scrape block_end

                        pass
                    elif timestamp_end and last_item["timestamp"] < timestamp_end:
                        # convert timestamp_end to block_end and scrape block_end

                        pass
                    else:
                        #   no need to scrape because last_item is the last item of the defined period, that happen to be a initial value
                        logging.getLogger(__name__).debug(
                            f" {chain.database_name} {status_data['address']} last index {idx} is an initial value, but also the last period value."
                        )

    else:
        logging.getLogger(__name__).error(
            f"   {chain.database_name} {hypervisor_address} has no data from {block_ini} to {block_end}"
        )


def hypervisor_periods_first_item_checks(
    chain: Chain,
    first_item: dict,
    timestamp_ini: int | None,
    block_ini: int | None,
    try_solve_errors: bool,
):
    # 1) check first item has uncollected fees
    if (
        float(first_item["fees_uncollected"]["qtty_token0"]) > 0
        or float(first_item["fees_uncollected"]["qtty_token1"]) > 0
    ):
        if timestamp_ini != None or block_ini != None:
            # this may happen when timestamp_ini or block_ini are defined and those are not the first temporal points of this hypervisor
            logging.getLogger(__name__).debug(
                f"         {chain.database_name} {first_item['address']} has uncollected fees on the first item but seems not a problem bc hype has data before {first_item['block']} block ."
            )
        else:
            if try_solve_errors:
                # uncollected fees on the first item cannot occur if all time hypervisor data is defined.
                # there are missing operations between blocks
                try:
                    # scrape operations between blocks for this hypervisor: ini_block = static hype block   end_block = first block
                    ini_block = get_from_localdb(
                        network=chain.database_name,
                        collection="static",
                        find={"address": first_item["address"]},
                        limit=1,
                    )[0]["block"]
                    end_block = first_item["block"]

                    feed_operations_hypervisors(
                        network=chain.database_name,
                        hypervisor_addresses=[first_item["address"]],
                        block_ini=ini_block,
                        block_end=end_block,
                    )
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f"  Error while feeding operations of {first_item['address']} ->  {e}"
                    )
            else:
                raise ValueError(
                    f" {chain.database_name} {first_item['address']} has uncollected fees on the first item. There are missing operations before block. {first_item['block']}"
                )


def hypervisors_periods_checkTotalSupplyChanged(
    chain: Chain,
    hypervisor_address: str,
    data: dict,
    idx: int,
    last_item: dict,
    current_item: dict,
    try_solve_errors: bool = False,
):
    if last_item["totalSupply"] != current_item["totalSupply"]:
        # blocks should not be consecutive
        if last_item["block"] + 1 != current_item["block"]:
            if not try_solve_errors:
                raise ValueError(
                    f"     FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
                )

            logging.getLogger(__name__).warning(
                f" Trying solve: index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
            )

            # # make sure data is exact
            # if diff := test_inequality_hypervisor(
            #     network=chain.database_name, hypervisors=[last_item, current_item]
            # ):
            #     # check if totalSupply is solved
            #     if diff[0]["block"] == last_item["block"]:
            #         if diff[0]["totalSupply"] != last_item["totalSupply"]:
            #             # solved
            #             logging.getLogger(__name__).debug(
            #                 f"  index {idx} changed last_item totalSupply to {diff[0]['totalSupply']}"
            #             )
            #         if diff[1]["block"] == current_item["block"]:
            #             if diff[1]["totalSupply"] != current_item["totalSupply"]:
            #                 # solved
            #                 logging.getLogger(__name__).debug(
            #                     f"  index {idx} changed current_item totalSupply to {diff[1]['totalSupply']}"
            #                 )
            #     else:
            #         if diff[0]["block"] == current_item["block"]:
            #             if diff[0]["totalSupply"] != current_item["totalSupply"]:
            #                 # solved
            #                 logging.getLogger(__name__).debug(
            #                     f"  index {idx} changed current_item totalSupply to {diff[0]['totalSupply']}"
            #                 )
            #         if diff[1]["block"] == last_item["block"]:
            #             if diff[1]["totalSupply"] != last_item["totalSupply"]:
            #                 # solved
            #                 logging.getLogger(__name__).debug(
            #                     f"  index {idx} changed last_item totalSupply to {diff[1]['totalSupply']}"
            #                 )

            #     # save diff to db
            #     if db_return := get_default_localdb(
            #         network=chain.database_name
            #     ).update_items_to_database(data=diff, collection_name="status"):
            #         logging.getLogger(__name__).debug(
            #             f"  db return {db_return.modified_count} items modified"
            #         )
            #     else:
            #         logging.getLogger(__name__).error(f"  db return nothing")

            # scrape operations between blocks for this hypervisor
            feed_operations_hypervisors(
                network=chain.database_name,
                hypervisor_addresses=[hypervisor_address],
                block_ini=last_item["block"],
                block_end=current_item["block"],
            )
        else:
            raise ValueError(
                f"      FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  consecutive blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
            )


def hypervisors_periods_checkfeeGrowths(
    chain: Chain,
    hypervisor_address: str,
    data: dict,
    idx: int,
    last_item: dict,
    current_item: dict,
    try_solve_errors: bool = False,
):
    """Check if feegrowth always grows"""

    # check if feeGrowthGlobal is always growing
    if int(last_item["pool"]["feeGrowthGlobal0X128"]) > int(
        current_item["pool"]["feeGrowthGlobal0X128"]
    ):
        raise ValueError(
            f"     FAIL index {idx} feeGrowthGlobal0X128 is decreasing from {last_item['feeGrowthGlobal0X128']} to {current_item['feeGrowthGlobal0X128']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
        )

    if int(last_item["pool"]["feeGrowthGlobal1X128"]) > int(
        current_item["pool"]["feeGrowthGlobal1X128"]
    ):
        raise ValueError(
            f"     FAIL index {idx} feeGrowthGlobal1X128 is decreasing from {last_item['feeGrowthGlobal1X128']} to {current_item['feeGrowthGlobal1X128']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
        )

    # check if feeGrowthInside last is always growing
