from decimal import Decimal
import logging
import time
from apps.feeds.operations import feed_operations
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
)
from bins.errors.actions import process_error
from bins.errors.general import ProcessingError
from bins.general.enums import Chain, Protocol, error_identity
from bins.w3.builders import build_db_hypervisor

from .objects import period_yield_data


def create_hypervisor_returns(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    convert_to_dict: bool = True,
    convert_to_d128: bool = True,
) -> list[period_yield_data] | list[dict]:
    #
    result = []

    # create control vars
    last_item = None

    # get a list of custom ordered hype status
    if ordered_hype_status_list := get_returns_source_data(
        chain=chain,
        hypervisor_address=hypervisor_address,
        timestamp_ini=int(timestamp_ini),
        timestamp_end=int(timestamp_end),
        block_ini=block_ini,
        block_end=block_end,
    ):
        # get all prices related to this hypervisor for the specified period
        token_addresses = [
            ordered_hype_status_list[0]["pool"]["token0"]["address"],
            ordered_hype_status_list[0]["pool"]["token1"]["address"],
        ]
        # get the max and min blocks from the ordered hype status list
        min_block = min([x["block"] for x in ordered_hype_status_list])
        max_block = max([x["block"] for x in ordered_hype_status_list])
        # get prices
        token_prices = {
            f"{x['address']}_{x['block']}": x["price"]
            for x in get_default_globaldb().get_items_from_database(
                collection_name="usd_prices",
                find={
                    "network": chain.database_name,
                    "address": {"$in": token_addresses},
                    "block": {"$gte": min_block, "$lte": max_block},
                },
            )
        }

        if len(ordered_hype_status_list):
            # check if we need another hype status with current latest data
            if last_hype_status := get_last_returns_source_data(
                chain=chain,
                hipervisor_status=ordered_hype_status_list[-1],
                timestamp_end=timestamp_end,
            ):
                logging.getLogger(__name__).debug(
                    f" Added a last hype status at block {last_hype_status['block']}"
                )
                ordered_hype_status_list.append(last_hype_status)

        # NOT USEFUL:
        # calculate the number of seconds from defined ini_timestamp/block to first item found
        # (hypervisor may not be created at the same time as the first item)
        #
        last_item_seconds_to_end = 0
        if timestamp_end:
            last_item_seconds_to_end = (
                timestamp_end - ordered_hype_status_list[-1]["timestamp"]
            )
        elif block_end:
            last_item_seconds_to_end = block_end - ordered_hype_status_list[-1]["block"]
        # # # # # # # # # #
        for idx, data in enumerate(ordered_hype_status_list):
            # zero and par indexes refer to initial values
            if idx == 0 or idx % 2 == 0:
                # this is an initial value
                pass
            else:
                # this is an end value

                # create yield data and fill from hype status
                current_period = period_yield_data()
                try:
                    # check if last_item is not 1 block away from current item
                    if last_item and last_item["block"] + 1 == data["block"]:
                        # there are times blocks are consecutive more than 2 items in a row... that's ok. check if next or previous block is consecutive
                        try:
                            if (
                                ordered_hype_status_list[idx + 1]["block"]
                                == data["block"] + 1
                                or ordered_hype_status_list[idx - 1]["block"]
                                == data["block"] - 1
                            ):
                                pass
                            else:
                                raise ValueError(
                                    f" Blocks are consecutive. Last block: {last_item['block']} Current block: {data['block']}"
                                )
                        except Exception as e:
                            raise ValueError(
                                f" Blocks are consecutive. Last block: {last_item['block']} Current block: {data['block']}"
                            )

                    # fill usd price
                    try:
                        current_period.set_prices(
                            token0_price=Decimal(
                                str(
                                    token_prices[
                                        f"{data['pool']['token0']['address']}_{last_item['block']}"
                                    ]
                                )
                            ),
                            token1_price=Decimal(
                                str(
                                    token_prices[
                                        f"{data['pool']['token1']['address']}_{last_item['block']}"
                                    ]
                                )
                            ),
                            position="ini",
                        )
                        current_period.set_prices(
                            token0_price=Decimal(
                                str(
                                    token_prices[
                                        f"{data['pool']['token0']['address']}_{data['block']}"
                                    ]
                                )
                            ),
                            token1_price=Decimal(
                                str(
                                    token_prices[
                                        f"{data['pool']['token1']['address']}_{data['block']}"
                                    ]
                                )
                            ),
                            position="end",
                        )

                    except Exception as e:
                        # it will be filled later
                        pass

                    # fill from hype status
                    try:
                        current_period.fill_from_hypervisors_data(
                            ini_hype=last_item,
                            end_hype=data,
                            network=chain.database_name,
                        )
                    except ProcessingError as e:
                        logging.getLogger(__name__).error(
                            f" Error while creating hype returns. {e.message}"
                        )
                        # process error
                        process_error(e)

                    # fill rewards
                    try:
                        current_period.fill_from_rewards_data(
                            ini_rewards=last_item["rewards_status"],
                            end_rewards=data["rewards_status"],
                        )
                    except ProcessingError as e:
                        logging.getLogger(__name__).error(
                            f" Error while creating hype returns. {e.message}"
                        )
                        # process error
                        process_error(e)

                    # convert to dict if needed
                    if convert_to_dict:
                        current_period = current_period.to_dict()

                        # convert to bson 128
                        if convert_to_d128:
                            current_period = get_default_localdb(
                                network=chain.database_name
                            ).convert_decimal_to_d128(current_period)

                    # append to result
                    result.append(current_period)

                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Error while creating hype returns.  {e}"
                    )

            # log for errors: periods must be consecutive and not overlaped
            if len(result) > 1:
                for i in range(len(result) - 1, 0, -1):
                    item0 = result[i - 1]
                    item1 = result[i]

                    if convert_to_dict:
                        item0_end_block = item0["end_period"]["block"]
                        item0_ini_block = item0["ini_period"]["block"]
                        item1_ini_block = item1["ini_period"]["block"]
                        item1_end_block = item1["end_period"]["block"]
                    else:
                        item0_end_block = item0.timeframe.end.block
                        item0_ini_block = item0.timeframe.ini.block
                        item1_ini_block = item1.timeframe.ini.block
                        item1_end_block = item1.timeframe.end.block

                    if (
                        item0_end_block > item1_ini_block
                        or item0_ini_block > item1_ini_block
                        or item1_ini_block > item1_end_block
                    ):
                        raise ProcessingError(
                            chain=chain,
                            item={
                                "item0": item0,
                                "item1": item1,
                                "description": " check database query and subsequent processing of data bc period data items overlap.",
                            },
                            identity=error_identity.OVERLAPEDED_PERIODS,
                            action="check_manually",
                            message=f" Overlaped periods found between {item0_end_block} and {item1_ini_block}. Check manually. ",
                        )

            # set lastitem
            last_item = data

    # check for inconsistencies in the impermanent loss figures to identify token price errors
    # if list_impermament := [
    #     (
    #         item.period_impermanent_usd / Decimal(str(item.period_days)),
    #         (item.period_impermanent_usd / item.ini_underlying_usd)
    #         / Decimal(str(item.period_days)),
    #         item.period_days,
    #     )
    #     for item in result
    #     if item.period_days and item.ini_underlying_usd
    # ]:
    #     maximum_il = max(list_impermament, key=lambda x: x[1])
    #     if (
    #         abs(maximum_il[1]) > Decimal("0.01")
    #         and abs(maximum_il[0]) > Decimal("100")
    #         and maximum_il[2] > 1
    #     ):
    #         po = ""
    #         # get tokens involved

    # logging.getLogger(__name__).info(" -- ")
    # logging.getLogger(__name__).info(
    #     f"  maximum IL/ini/DAY: {maximum_il[0]:,.2f} -> {maximum_il[1]:,.4%} -> {maximum_il[2]} days"
    # )

    return result


def feed_hypervisor_returns(
    chain: Chain, hypervisor_addresses: list[str] | None = None
):
    """Feed hypervisor returns from the specified chain and hypervisor addresses

    Args:
        chain (Chain):
        hypervisor_addresses (list[str]): list of hype addresses

    """

    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name} returns information"
    )

    batch_size = 50000

    # get the last end block found in hypervisor returns for each hype in the specified list
    query = []
    if (
        _match := {"$match": {"address": {"$in": hypervisor_addresses}}}
        if hypervisor_addresses
        else {}
    ):
        query.append(_match)
    # the last end block found in hypervisor returns
    query.append(
        {"$group": {"_id": "$address", "end_block": {"$max": "$timeframe.end.block"}}}
    )

    if hype_block_data := get_from_localdb(
        network=chain.database_name,
        collection="hypervisor_returns",
        aggregate=query,
        batch_size=batch_size,
    ):
        # work on each hype found block
        for item in hype_block_data:
            hype_address = item["_id"]
            hype_ini_block = item["end_block"] + 1

            # create and save hype returns to database
            save_hypervisor_returns_to_database(
                chain=chain, hypervisor_address=hype_address, block_ini=hype_ini_block
            )
    else:
        logging.getLogger(__name__).info(
            f" No hypervisor returns found in database for {chain.database_name}. Staring from scratch."
        )
        if hypervisor_addresses:
            find = {"address": {"$in": hypervisor_addresses}}
        else:
            find = {}
        # get a list of hypes to feed
        hypervisors_static = get_from_localdb(
            network=chain.database_name,
            collection="static",
            find=find,
            projection={"address": 1, "timestamp": 1, "_id": 0},
        )

        # create chunks of timeframes to feed data so that we don't overload the database
        #
        # get the lowest timestamp from static data
        min_timestamp = min([hype["timestamp"] for hype in hypervisors_static])

        # define highest timestamp
        max_timestamp = int(time.time())

        # define chunk size
        chunk_size = 86400 * 7 * 4  # 4 week

        # create chunks
        chunks = [
            (i, i + chunk_size) for i in range(min_timestamp, max_timestamp, chunk_size)
        ]

        logging.getLogger(__name__).info(
            f" {len(chunks)} chunks created to feed each hypervisor returns data so that the database does not overload"
        )

        # get hypervisor returns for each chunk
        for chunk in chunks:
            for item in hypervisors_static:
                logging.getLogger(__name__).info(
                    f" Feeding chunk {chunk[0]} to {chunk[1]} for {chain.database_name}'s {item['address']} hypervisor"
                )
                save_hypervisor_returns_to_database(
                    chain=chain,
                    hypervisor_address=item["address"],
                    timestamp_ini=chunk[0],
                    timestamp_end=chunk[1],
                )


def save_hypervisor_returns_to_database(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):
    # create hypervisor returns list
    if data := create_hypervisor_returns(
        chain=chain,
        hypervisor_address=hypervisor_address,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        block_ini=block_ini,
        block_end=block_end,
    ):
        # save all at once
        if db_return := get_default_localdb(
            network=chain.database_name
        ).set_hypervisor_return_bulk(data=data):
            logging.getLogger(__name__).debug(
                f"     db return-> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
            )
        else:
            logging.getLogger(__name__).error(
                f"  database did not return anything while saving {hypervisor_address}s returns at { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" No hypervisor {hypervisor_address} data from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end} to construct returns "
        )


def create_chunks(min: int, max: int, chunk_size: int) -> list[tuple[int, int]]:
    """build chunks of data

    Args:
        min (int): minimum value
        max (int): maximum value
        chunk_size (int): size of each chunk

    Returns:
        list: list of tuples with chunk (start:int, end:int)
    """

    # create chunks
    return [(i, i + chunk_size) for i in range(min, max, chunk_size)]


def get_returns_source_data(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
) -> list[dict]:
    """Get hypervisor data to create returns from database using chunks to avoid 16Mb errors

    Args:
        chain (Chain): _description_
        hypervisor_address (str): _description_
        timestamp_ini (int | None, optional): _description_. Defaults to None.
        timestamp_end (int | None, optional): _description_. Defaults to None.
        block_ini (int | None, optional): _description_. Defaults to None.
        block_end (int | None, optional): _description_. Defaults to None.

    Returns:
        list[dict]: list of hypervisor status
    """

    batch_size = 50000

    # try getting data directly from database
    try:
        # build query (ease debuging)
        query = get_default_localdb(
            network=chain.database_name
        ).query_locs_apr_hypervisor_data_calculation(
            hypervisor_address=hypervisor_address,
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
        )
        # get a list of custom ordered hype status
        ordered_hype_status_list = get_from_localdb(
            network=chain.database_name,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        )
        if ordered_hype_status_list:
            return ordered_hype_status_list[0]["status"]
        else:
            return []
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Error getting {hypervisor_address} hype data to construct returns from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}. Trying to slice it in chunks."
        )

    result = {}
    #
    # create chunks of timeframes to process so that we don't receive 16Mb errors
    # when quering mongodb
    # try to avoid 16Mb errors by slicing the query in small chunks
    chunks = [(None, None, None, None)]
    timestamp_chunk_size = 60 * 60 * 24 * 30 * 1  # 1 month
    block_chunk_size = 1000000
    # create chunks of timeframes to process so that we don't receive 16Mb errors
    # when quering mongodb
    if timestamp_ini and timestamp_end:
        if timestamp_end - timestamp_ini > timestamp_chunk_size:
            # create chunks
            chunks = [
                (_ini, _end, None, None)
                for _ini, _end, in create_chunks(
                    min=int(timestamp_ini),
                    max=int(timestamp_end),
                    chunk_size=timestamp_chunk_size,
                )
            ]
        else:
            chunks = [(timestamp_ini, timestamp_end, None, None)]
        logging.getLogger(__name__).debug(
            f" {len(chunks)} chunks of timestamps created to build returns data. Defined chunk size: {timestamp_chunk_size}"
        )

    elif block_ini and block_end:
        if block_end - block_ini > block_chunk_size:
            # create chunks
            chunks = [
                (None, None, _ini, _end)
                for _ini, _end in create_chunks(
                    min=block_ini, max=block_end, chunk_size=block_chunk_size
                )
            ]
        else:
            chunks = [(None, None, block_ini, block_end)]

        logging.getLogger(__name__).debug(
            f" {len(chunks)} chunks of blocks created to build returns data. Defined chunk size: {block_chunk_size}"
        )
    _start_time = time.time()
    for t_ini, t_end, b_ini, b_end in chunks:
        # build query (ease debuging)
        query = get_default_localdb(
            network=chain.database_name
        ).query_locs_apr_hypervisor_data_calculation(
            hypervisor_address=hypervisor_address,
            timestamp_ini=t_ini,
            timestamp_end=t_end,
            block_ini=b_ini,
            block_end=b_end,
        )
        # get a list of custom ordered hype status
        if ordered_hype_status_list := get_from_localdb(
            network=chain.database_name,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        ):
            # add to result if id does not exist
            for hype_status in ordered_hype_status_list[0]["status"]:
                if not hype_status["id"] in result:
                    result[hype_status["id"]] = hype_status

    logging.getLogger(__name__).debug(
        f"  chunk process->  {len(result)} items found in {time.time() - _start_time} seconds"
    )
    # convert result to list and return
    return list(result.values())


def get_last_returns_source_data(
    chain: Chain,
    hipervisor_status: dict,
    timestamp_end: int | None,
):
    # check if timestamp end is close to current time ( last 24h)
    if timestamp_end and timestamp_end > int(time.time()) - 60 * 60 * 24:
        # build the latest hypervisor status
        if result := build_db_hypervisor(
            address=hipervisor_status["address"],
            network=chain.database_name,
            block=0,
            dex=hipervisor_status["dex"],
        ):
            # return only if supply is equal to hypervisor status
            if result["totalSupply"] == hipervisor_status["totalSupply"]:
                return result
            else:
                logging.getLogger(__name__).debug(
                    f"  last hypervisor supplies do not match by {int(result['totalSupply'])-int(hipervisor_status['totalSupply'])} ->  last:{result['totalSupply']}  database: {hipervisor_status['totalSupply']}"
                )
    # return empty
    return {}
