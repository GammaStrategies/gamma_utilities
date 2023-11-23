import logging
import time
from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
    get_price_from_db,
)
from bins.errors.general import ProcessingError
from bins.formulas.apr import calculate_rewards_apr
from bins.general.general_utilities import create_chunks


def get_reward_pool_prices(
    network: str, block: int, reward_token: str, token0: str, token1: str
) -> tuple[float, float, float]:
    """Get reward token, token0 and token1 prices

    Args:
        network (str):
        block (int):
        reward_token (str): reward token address
        token0 (str): pool token0 address
        token1 (str): pool token1 address

    Returns:
        tuple[float, float, float]:  rewardToken_price, hype_token0_price, hype_token1_price
    """
    rewardToken_price = get_price_from_db(
        network=network,
        block=block,
        token_address=reward_token,
    )
    hype_token0_price = get_price_from_db(
        network=network,
        block=block,
        token_address=token0,
    )
    hype_token1_price = get_price_from_db(
        network=network,
        block=block,
        token_address=token1,
    )

    return rewardToken_price, hype_token0_price, hype_token1_price


def get_hypervisor_price_per_share_from_status(
    network: str, hypervisor_status: dict
) -> float:
    token0_price = get_price_from_db(
        network=network,
        block=hypervisor_status["block"],
        token_address=hypervisor_status["pool"]["token0"]["address"],
    )
    token1_price = get_price_from_db(
        network=network,
        block=hypervisor_status["block"],
        token_address=hypervisor_status["pool"]["token1"]["address"],
    )
    return get_hypervisor_price_per_share(
        hypervisor_status=hypervisor_status,
        token0_price=token0_price,
        token1_price=token1_price,
    )


def get_hypervisor_price_per_share(
    hypervisor_status: dict, token0_price: float, token1_price: float
) -> float:
    # do not calculate when no supply
    if int(hypervisor_status["totalSupply"]) == 0:
        return 0.0

    # get the hypervisor token amounts claimable by LPs ( parked + deployed + owed)
    hype_total0 = (
        int(hypervisor_status["tvl"]["parked_token0"])
        + int(hypervisor_status["tvl"]["deployed_token0"])
        + int(hypervisor_status["tvl"]["fees_owed_token0"])
    ) / (10 ** hypervisor_status["pool"]["token0"]["decimals"])
    hype_total1 = (
        int(hypervisor_status["tvl"]["parked_token1"])
        + int(hypervisor_status["tvl"]["deployed_token1"])
        + int(hypervisor_status["tvl"]["fees_owed_token1"])
    ) / (10 ** hypervisor_status["pool"]["token1"]["decimals"])

    # get uncollected fees for the pool at this block
    # try to get LPs fees only, and not gamma portion (  when available: database refresh neededwhen not )
    if "lps_qtty_token0" in hypervisor_status["fees_uncollected"]:
        # LPs fees only
        uncollected_fees0 = float(
            hypervisor_status["fees_uncollected"]["lps_qtty_token0"]
        ) / (10 ** hypervisor_status["pool"]["token0"]["decimals"])
        uncollected_fees1 = float(
            hypervisor_status["fees_uncollected"]["lps_qtty_token1"]
        ) / (10 ** hypervisor_status["pool"]["token1"]["decimals"])
    else:
        # all fees ( gamma portion included )
        logging.getLogger(__name__).warning(
            f" Share price calculation is using all uncollected fees for {hypervisor_status['address']} at block {hypervisor_status['block']}. A database reScrape is needed."
        )
        uncollected_fees0 = float(
            hypervisor_status["fees_uncollected"]["qtty_token0"]
        ) / (10 ** hypervisor_status["pool"]["token0"]["decimals"])
        uncollected_fees1 = float(
            hypervisor_status["fees_uncollected"]["qtty_token1"]
        ) / (10 ** hypervisor_status["pool"]["token1"]["decimals"])

    # create an easy to debug totals
    total_token0 = hype_total0 + uncollected_fees0
    total_token1 = hype_total1 + uncollected_fees1

    # calculate price per share
    price_per_share = (token0_price * total_token0 + token1_price * total_token1) / (
        int(hypervisor_status["totalSupply"]) / (10 ** hypervisor_status["decimals"])
    )

    # if price_per_share > 10**14:
    #     raise ValueError(
    #         f" Price per share is too high. {price_per_share}  for hype {hypervisor_status['address']} at block {hypervisor_status['block']}. ¿¿ Uncollected fees ?? -> u0:{uncollected_fees0} u1:{uncollected_fees1}"
    #     )

    return price_per_share


def reward_add_status_fields(
    reward_data: dict,
    hypervisor_status: dict,
    apr: float,
    rewardToken_price: float,
    token0_price: float,
    token1_price: float,
    hype_price_per_share: float | None = None,
) -> dict:
    # add status fields ( APR )
    reward_data["hypervisor_symbol"] = hypervisor_status["symbol"]
    reward_data["dex"] = hypervisor_status["dex"]
    reward_data["apr"] = apr
    reward_data["rewardToken_price_usd"] = rewardToken_price
    reward_data["token0_price_usd"] = token0_price
    reward_data["token1_price_usd"] = token1_price
    reward_data[
        "hypervisor_share_price_usd"
    ] = hype_price_per_share or get_hypervisor_price_per_share(
        hypervisor_status=hypervisor_status,
        token0_price=token0_price,
        token1_price=token1_price,
    )

    return reward_data


def add_apr_process01(
    network: str,
    hypervisor_status: dict,
    reward_data: dict,
    rewardToken_price: float | None = None,
    hype_token0_price: float | None = None,
    hype_token1_price: float | None = None,
    hype_price_per_share: float | None = None,
) -> dict:
    # token_prices
    if not rewardToken_price or not hype_token0_price or not hype_token1_price:
        (
            rewardToken_price,
            hype_token0_price,
            hype_token1_price,
        ) = get_reward_pool_prices(
            network=network,
            block=hypervisor_status["block"],
            reward_token=reward_data["rewardToken"],
            token0=hypervisor_status["pool"]["token0"]["address"],
            token1=hypervisor_status["pool"]["token1"]["address"],
        )

    # hypervisor price per share
    if not hype_price_per_share:
        hype_price_per_share = get_hypervisor_price_per_share(
            hypervisor_status=hypervisor_status,
            token0_price=hype_token0_price,
            token1_price=hype_token1_price,
        )

    if int(reward_data["total_hypervisorToken_qtty"]) and hype_price_per_share:
        # if there is hype qtty staked and price per share
        apr = calculate_rewards_apr(
            token_price=rewardToken_price,
            token_reward_rate=int(reward_data["rewards_perSecond"])
            / (10 ** reward_data["rewardToken_decimals"]),
            total_lp_locked=int(reward_data["total_hypervisorToken_qtty"])
            / (10 ** hypervisor_status["decimals"]),
            lp_token_price=hype_price_per_share,
        )
    else:
        # no apr if no hype qtty staked or no price per share
        apr = 0

    reward_data = reward_add_status_fields(
        reward_data=reward_data,
        hypervisor_status=hypervisor_status,
        apr=apr,
        rewardToken_price=rewardToken_price,
        token0_price=hype_token0_price,
        token1_price=hype_token1_price,
        hype_price_per_share=hype_price_per_share,
    )

    return reward_data


def get_hypervisor_data_for_apr(
    network: str,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
) -> list[dict]:
    """Create a sorted by time list of hypervisor status, rewards and static data for apr calculation.

    Args:
        network (str):
        hypervisor_address (str):
        timestamp_ini (int): timestamp to start getting data from.
        timestamp_end (int): timestamp to end getting data from.

    Returns:
        list:
    """

    # Or timestamp or block should be provided
    if not timestamp_ini and not block_ini or not timestamp_end and not block_end:
        raise ValueError("timestamps or blocks must be provided")

    # build query (ease debuging)
    query = get_default_localdb(
        network=network
    ).query_locs_apr_hypervisor_data_calculation(
        hypervisor_address=hypervisor_address,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
    )
    batch_size = 100000

    # try to get data from db. If fails, try to slice query in chunks
    try:
        # get hype data from db so that apr can be constructed.
        return get_from_localdb(
            network=network,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        )
    except ProcessingError as e:
        logging.getLogger(__name__).debug(
            f" Could not get {hypervisor_address} hype data to construct hypervisor_data_for_apr from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}. Trying to slice it in chunks"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Error getting {hypervisor_address} hype data to construct hypervisor_data_for_apr from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}. Trying to slice it in chunks."
        )

    # try to avoid mongodb's 16Mb errors by slicing the query in small chunks
    result = [{"_id": hypervisor_address, "status": []}]
    #
    chunks = [(None, None, None, None)]
    timestamp_chunk_size = 60 * 60 * 24 * 30 * 1  # 1 month
    block_chunk_size = 1000000
    # create chunks of timeframes to process
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
    # control vars
    _start_time = time.time()
    _processed_ids = set()
    for t_ini, t_end, b_ini, b_end in chunks:
        # build query (ease debuging)
        query = get_default_localdb(
            network=network
        ).query_locs_apr_hypervisor_data_calculation(
            hypervisor_address=hypervisor_address,
            timestamp_ini=t_ini,
            timestamp_end=t_end,
            block_ini=b_ini,
            block_end=b_end,
        )
        # get a list of custom ordered hype status
        if ordered_hype_status_list := get_from_localdb(
            network=network,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        ):
            # add to result if id does not exist
            for hype_status in ordered_hype_status_list[0]["status"]:
                if not hype_status["_id"] in _processed_ids:
                    # add to result
                    result[0]["status"].append(hype_status)
                    # add to processed ids
                    _processed_ids.add(hype_status["_id"])

    logging.getLogger(__name__).debug(
        f"  chunk process->  {len(result[0]['status'])} items found in {time.time() - _start_time} seconds"
    )

    # return
    return result


def filter_hypervisor_data_for_apr(
    data: list[dict], min_period_seconds: int = 40
) -> list[dict]:
    """Discard time periods of less than 1 minute -> initial and final periods timestamp difference must be greater than 1 minute.

    Args:
        data (list[dict]): as returned by query_locs_apr_hypervisor_data_calculation query
        min_period_seconds (int, optional): Defaults to one minute.
    Returns:
        list[dict]: filtered data
    """
    for _ordered_hype_status_db in data:
        items_to_keep = []

        for i in range(0, len(_ordered_hype_status_db["status"]), 2):
            # make sure there is a next item
            if i + 1 < len(_ordered_hype_status_db["status"]):
                # calculate seconds between 2 items
                _seconds_period = (
                    _ordered_hype_status_db["status"][i + 1]["timestamp"]
                    - _ordered_hype_status_db["status"][i]["timestamp"]
                )

                if _seconds_period >= min_period_seconds:
                    # keep both items
                    items_to_keep.append(_ordered_hype_status_db["status"][i])
                    items_to_keep.append(_ordered_hype_status_db["status"][i + 1])
                else:
                    logging.getLogger(__name__).debug(
                        f" Discarding period {_seconds_period} seconds long for hype {_ordered_hype_status_db['status'][i]['address']}"
                    )

        # replace status with filtered items
        _ordered_hype_status_db["status"] = items_to_keep

    # return filtered data
    return data


def get_hypervisor_data_for_apr_custom_test(
    network: str,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
) -> list[dict]:
    """Create a sorted by time list of hypervisor status, rewards and static data for apr calculation.

    Args:
        network (str):
        hypervisor_address (str):
        timestamp_ini (int): timestamp to start getting data from.
        timestamp_end (int): timestamp to end getting data from.

    Returns:
        list:
    """

    # Or timestamp or block should be provided
    if not timestamp_ini and not block_ini or not timestamp_end and not block_end:
        raise ValueError("timestamps or blocks must be provided")

    # build query (ease debuging)
    query = get_default_localdb(network=network).query_hypervisor_periods(
        hypervisor_addresses=[hypervisor_address],
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
    )
    batch_size = 10000

    # try to get data from db. If fails, try to slice query in chunks
    try:
        # get hype data from db so that apr can be constructed.
        return get_from_localdb(
            network=network,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        )
    except ProcessingError as e:
        logging.getLogger(__name__).debug(
            f" Could not get {hypervisor_address} hype data to construct hypervisor_data_for_apr from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}. Trying to slice it in chunks"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Error getting {hypervisor_address} hype data to construct hypervisor_data_for_apr from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}. Trying to slice it in chunks."
        )

    # try to avoid mongodb's 16Mb errors by slicing the query in small chunks
    result = [{"_id": hypervisor_address, "status": []}]
    #
    chunks = [(None, None, None, None)]
    timestamp_chunk_size = 60 * 60 * 24 * 10  # 10 days
    block_chunk_size = 1000000
    # create chunks of timeframes to process
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
    # control vars
    _start_time = time.time()
    _processed_ids = set()
    for t_ini, t_end, b_ini, b_end in chunks:
        # build safest query ( avoid 16Mb errors as much as possible)
        query = get_default_localdb(network=network).query_hypervisor_periods_flatten(
            hypervisor_addresses=[hypervisor_address],
            timestamp_ini=t_ini,
            timestamp_end=t_end,
            block_ini=b_ini,
            block_end=b_end,
        )
        # get a list of custom ordered hype status
        if ordered_hype_status_list := get_from_localdb(
            network=network,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        ):
            # group by hypervisor address: [ { '_id':<hype_address>, 'status': [ <hype_status> ] } ]
            ordered_hype_status_list = group_by_hypervisors(ordered_hype_status_list)

            # add to result if id does not exist
            for hype_status in ordered_hype_status_list[0]["status"]:
                if not hype_status["_id"] in _processed_ids:
                    # add to result
                    result[0]["status"].append(hype_status)
                    # add to processed ids
                    _processed_ids.add(hype_status["_id"])

    logging.getLogger(__name__).debug(
        f"  chunk process->  {len(result[0]['status'])} items found in {time.time() - _start_time} seconds"
    )

    # return
    return result


# This is the good one
def get_hypervisors_data_for_apr(
    network: str,
    hypervisor_addresses: list[str],
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    only_return_last_items: int | None = None,
) -> list[dict]:
    """Create a sorted by time list of hypervisor status, rewards and static data for any hypervisor calculation ( apr mainly).

    Args:
        network (str):
        hypervisor_address (str):
        timestamp_ini (int): timestamp to start getting data from.
        timestamp_end (int): timestamp to end getting data from.
        ...
        only_return_last_items (int): if provided, only the last items of the list will be returned

    Returns:
        list: {

        }
    """

    # Or timestamp or block should be provided
    if not timestamp_ini and not block_ini or not timestamp_end and not block_end:
        raise ValueError("timestamps or blocks must be provided")

    # build query (ease debuging)
    query = get_default_localdb(network=network).query_hypervisor_periods(
        hypervisor_addresses=hypervisor_addresses,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        block_ini=block_ini,
        block_end=block_end,
    )
    batch_size = 10000

    # try to get data from db. If fails, try to slice query in chunks
    try:
        # get hype data from db so that apr can be constructed.
        result = get_from_localdb(
            network=network,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        )

        # filter data, if defined
        if only_return_last_items:
            for _ordered_hype_status_db in result:
                _ordered_hype_status_db["status"] = _ordered_hype_status_db["status"][
                    -only_return_last_items:
                ]

        return result

    except ProcessingError as e:
        logging.getLogger(__name__).debug(
            f" Could not get {hypervisor_addresses} hype data to construct hypervisor_data_for_apr from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}. Trying to slice it in chunks"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Error getting {hypervisor_addresses} hype data to construct hypervisor_data_for_apr from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if (block_ini and block_end) else timestamp_end}. Trying to slice it in chunks."
        )

    # try to avoid mongodb's 16Mb errors by slicing the query in small chunks
    result = [
        {"_id": hypervisor_address, "status": []}
        for hypervisor_address in hypervisor_addresses
    ]
    #
    chunks = [(None, None, None, None)]
    timestamp_chunk_size = 60 * 60 * 24 * 10  # 10 days
    block_chunk_size = 1000000
    # create chunks of timeframes to process
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
    # control vars
    _start_time = time.time()
    _processed_ids = set()
    for t_ini, t_end, b_ini, b_end in chunks:
        # build safest query ( avoid 16Mb errors as much as possible)
        query = get_default_localdb(network=network).query_hypervisor_periods_flatten(
            hypervisor_addresses=hypervisor_addresses,
            timestamp_ini=t_ini,
            timestamp_end=t_end,
            block_ini=b_ini,
            block_end=b_end,
        )
        # get a list of custom ordered hype status
        if ordered_hype_status_list := get_from_localdb(
            network=network,
            collection="operations",
            aggregate=query,
            batch_size=batch_size,
        ):
            # group by hypervisor address: [ { '_id':<hype_address>, 'status': [ <hype_status> ] } ]
            ordered_hype_status_list = group_by_hypervisors(ordered_hype_status_list)

            # add to result if id does not exist
            for hype_status in ordered_hype_status_list[0]["status"]:
                if not hype_status["_id"] in _processed_ids:
                    # add to result
                    result[0]["status"].append(hype_status)
                    # add to processed ids
                    _processed_ids.add(hype_status["id"])

    logging.getLogger(__name__).debug(
        f"  chunk process->  {len(result[0]['status'])} items found in {time.time() - _start_time} seconds"
    )

    # filter data, if defined
    if only_return_last_items:
        for _ordered_hype_status_db in result:
            _ordered_hype_status_db["status"] = _ordered_hype_status_db["status"][
                -only_return_last_items:
            ]

    # return
    return result


def group_by_hypervisors(db_data_list) -> list[dict]:
    """When using 'query_hypervisor_periods_flatten' query, this function will group the results by hypervisor address
        emulating the results of 'query_hypervisor_periods' query, but saving mongo's 16Mb max. potential errors

    Args:
        db_data_list (_type_): use 'query_hypervisor_periods_flatten' query to get this data

    Returns:
        list[dict]:
    """
    # group by hypervisor address: [ { '_id':<hype_address>, 'status': [ <hype_status> ] } ]
    result = {}
    for idx, item in enumerate(db_data_list):
        # ease var access
        hype_address = item["hypervisor_address"]
        # add hype address if not in result
        if not hype_address in result:
            result[hype_address] = {"_id": hype_address, "status": []}
        # add status
        result[hype_address]["status"].append(item["status"])

    # return
    return list(result.values())
