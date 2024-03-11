# latest hypervisor returns

from decimal import Decimal
import logging
from multiprocessing.pool import Pool
import time

import tqdm
from apps.feeds.returns.builds import get_last_return_data_from_db
from apps.feeds.returns.objects import period_yield_data
from apps.repair.prices.helpers import get_price
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.general.enums import Chain
from bins.w3.builders import (
    build_db_hypervisor_multicall,
    build_erc20_helper,
    get_latest_block,
)


def feed_latest_hypervisor_returns(
    chain: Chain,
    hypervisor_addresses: list[str] | None = None,
    multiprocess: bool = True,
):
    """Feed latest hypervisor returns from the specified chain and hypervisor addresses

    Args:
        chain (Chain):
        hypervisor_addresses (list[str]): list of hype addresses
        multiprocess (bool, optional): use multiprocessing. Defaults to True.

    """

    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name} latest hypervisor returns information {f'[multiprocessing]' if multiprocess else ''}"
    )
    # get filters
    filters = CONFIGURATION["script"]["protocols"].get("gamma", {}).get("filters", {})
    # get all hype addresses non discarded
    hypervisor_addresses = hypervisor_addresses or [
        x["address"]
        for x in get_from_localdb(
            network=chain.database_name, collection="static", find={}
        )
        if x["address"]
        not in filters.get("hypervisors_not_included", {}).get(chain.database_name, [])
    ]
    if not hypervisor_addresses:
        logging.getLogger(__name__).info(
            f" No hypervisors found in database {chain.database_name}"
        )
        return

    # get chain last block and timestamp
    helper = build_erc20_helper(chain=chain)
    latest_block = helper.block
    latest_timestamp = helper._timestamp
    blocks_per_second = helper.average_blockTime()

    # build arguments for multiprocessing
    _args = [
        (
            chain,
            hypervisor_address,
            latest_block,
            latest_timestamp,
            blocks_per_second,
        )
        for hypervisor_address in hypervisor_addresses
    ]

    with tqdm.tqdm(total=len(_args)) as progress_bar:

        if multiprocess:
            # use multiprocessing
            with Pool() as pool:
                for result in pool.starmap(feed_latest_hypervisor_returns_work, _args):
                    progress_bar.update(1)
        else:
            # get addresses and blocks to feed
            for arg in _args:
                feed_latest_hypervisor_returns_work(**arg)
                progress_bar.update(1)


def feed_latest_hypervisor_returns_work(
    chain: Chain,
    hypervisor_address: str,
    latest_block: int,
    latest_timestamp: int,
    blocks_per_second: float,
    save_to_database: bool = True,
):
    _startime_total = time.time()

    # get the last hypervisor returns
    if hypervisor_returns := get_from_localdb(
        network=chain.database_name,
        collection="hypervisor_returns",
        find={"address": hypervisor_address},
        sort=[("timeframe.end.block", -1)],
        limit=1,
    ):
        hypervisor_returns = hypervisor_returns[0]

    # get the last latest hypervisor returns
    if latest_hypervisor_returns := get_from_localdb(
        network=chain.database_name,
        collection="latest_hypervisor_returns",
        find={"address": hypervisor_address},
        sort=[("timeframe.end.block", -1)],
        limit=1,
    ):
        latest_hypervisor_returns = latest_hypervisor_returns[0]

    # get the last hype status
    if hypervisor_status := get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={"address": hypervisor_address},
        sort=[("block", -1)],
        limit=1,
    ):
        hypervisor_status = hypervisor_status[0]

    # decide wether to start from 1)hypervisor_returns or 2)latest_hypervisor_returns block
    # control var
    block_ini = None

    # check if last hypervisor status block is lower than latest_hypervisor_returns end block
    if (
        latest_hypervisor_returns
        and (
            latest_hypervisor_returns["timeframe"]["ini"]["block"]
            >= hypervisor_status["block"]
        )
        and (
            latest_hypervisor_returns["timeframe"]["end"]["block"]
            > hypervisor_returns["timeframe"]["end"]["block"]
        )
    ):
        # start from 2)latest_hypervisor_returns
        # first snapshot should be the next block after latest_hypervisor_returns end_block
        block_ini = latest_hypervisor_returns["timeframe"]["end"]["block"] + 1

    if not block_ini and hypervisor_returns:
        # start from 1)hypervisor_returns
        # first snapshot should be the next block after hypervisor_returns end_block
        block_ini = hypervisor_returns["timeframe"]["end"]["block"] + 1

        # and should be the same as the last hypervisor status block
        if block_ini != hypervisor_status["block"]:
            # status may be a transfer ( not affecting the hype supply)
            logging.getLogger(__name__).debug(
                f" Hypervisor returns end block is different from last hypervisor status block for {chain.database_name} {hypervisor_address}. May be because status its a transfer"
            )

    if not block_ini:
        logging.getLogger(__name__).error(
            f" Can't find a block_ini for {chain.database_name} {hypervisor_address} to process latest hypervisor returns. Skipping"
        )
        return

    # create period yields from snapshots
    blocks_to_process: list[tuple[int, int]] = create_blocks_to_process_list(
        block_ini=block_ini,
        latest_block=latest_block,
        blocks_per_second=blocks_per_second,
    )

    if not blocks_to_process:
        logging.getLogger(__name__).info(
            f" Latest hypervisor returns are up to date for {chain.database_name} {hypervisor_address}"
        )
        return

    period_yield_list = []
    for b_ini, b_end in blocks_to_process:
        try:
            # get snapshots
            ini_hype_status = build_db_hypervisor_multicall(
                address=hypervisor_address,
                network=chain.database_name,
                block=b_ini,
                dex=hypervisor_status["dex"],
                pool_address=hypervisor_status["pool"]["address"],
                token0_address=hypervisor_status["pool"]["token0"]["address"],
                token1_address=hypervisor_status["pool"]["token1"]["address"],
            )
            end_hype_status = build_db_hypervisor_multicall(
                address=hypervisor_address,
                network=chain.database_name,
                block=b_end,
                dex=hypervisor_status["dex"],
                pool_address=hypervisor_status["pool"]["address"],
                token0_address=hypervisor_status["pool"]["token0"]["address"],
                token1_address=hypervisor_status["pool"]["token1"]["address"],
            )

            # both should have the same totalSupply
            if ini_hype_status["totalSupply"] != end_hype_status["totalSupply"]:
                logging.getLogger(__name__).error(
                    f" {chain.database_name} {hypervisor_address} totalSupply is different between snapshots"
                )
                continue

            # add operations empty list to the db objects, to emulate the same structure as xpected
            ini_hype_status["operations"] = []
            end_hype_status["operations"] = []

            # create period yield data
            if current_period := create_period_yield_data(
                chain=chain, status_ini=ini_hype_status, status_end=end_hype_status
            ):
                period_yield_list.append(current_period)
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unexpected error processing blocks for the latest hype returns {chain.database_name} {hypervisor_address} {e}"
            )

    logging.getLogger(__name__).info(
        f" {chain.database_name} {hypervisor_address} {len(period_yield_list)} period yields created in {(time.time()-_startime_total)/60:,.1f} minutes"
    )

    # convert to dict and save
    if save_to_database and period_yield_list:
        try:
            # save converted to dict results to database
            save_latest_hypervisor_returns_to_database(
                chain=chain,
                period_yield_list=[x.to_dict() for x in period_yield_list],
            )
        except AttributeError as e:
            # AttributeError: 'NoneType' object has no attribute 'to_dict'
            logging.getLogger(__name__).error(
                f" Could not convert latest yield result to dictionary, so not saved. Probably because of a previous hopefully solved error -> {e}"
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Could not convert latest yield result to dictionary, so not saved -> {e}"
            )

    return period_yield_list


def create_blocks_to_process_list(
    block_ini: int,
    latest_block: int,
    blocks_per_second: float,
    period_lenght: int = 25920,
) -> list[tuple[int, int]]:

    # if the latest_block-block ini converted in seconds is less than a day, skip
    if (latest_block - block_ini) * blocks_per_second < (86400 / 2):
        logging.getLogger(__name__).debug(
            f" No latest hype returns can be created because {block_ini} to {latest_block} is less than half a day ( when converted)"
        )
        return []

    # define period lenghts
    # days:seconds
    days_left_vs_period = {
        365: 18576000,
        180: 7776000,
        90: 6048000,
        60: 3456000,
        30: 1728000,
        15: 604800,
        7: 259200,
        1: period_lenght,
    }
    # convert period lenghts to blocks
    days_left_vs_period = {
        k: int(v / blocks_per_second) for k, v in days_left_vs_period.items()
    }

    blocks_to_process = []
    _block = block_ini
    while _block < latest_block:
        # get current period lentgh as days left
        days_left = ((latest_block - _block) * blocks_per_second) / 86400
        # get the period lenght
        closest_key = min(
            list(days_left_vs_period.keys()), key=lambda x: abs(x - days_left)
        )
        blocks_period_lenght = days_left_vs_period[closest_key]

        if _block + blocks_period_lenght > latest_block:
            break

        blocks_to_process.append((_block, _block + blocks_period_lenght))

        # move to next block
        _block += blocks_period_lenght + 1

    return blocks_to_process


def create_period_yield_data(
    chain: Chain, status_ini: dict, status_end: dict
) -> period_yield_data:
    current_period = period_yield_data()

    # get prices
    token0_price_ini, source = get_price(
        network=chain.database_name,
        token_address=status_ini["pool"]["token0"]["address"],
        block=status_ini["block"],
    )
    token1_price_ini, source = get_price(
        network=chain.database_name,
        token_address=status_ini["pool"]["token1"]["address"],
        block=status_ini["block"],
    )
    token0_price_end, source = get_price(
        network=chain.database_name,
        token_address=status_end["pool"]["token0"]["address"],
        block=status_end["block"],
    )
    token1_price_end, source = get_price(
        network=chain.database_name,
        token_address=status_end["pool"]["token1"]["address"],
        block=status_end["block"],
    )

    # fill usd price
    current_period.set_prices(
        token0_price=Decimal(str(token0_price_ini)),
        token1_price=Decimal(str(token1_price_ini)),
        position="ini",
    )
    current_period.set_prices(
        token0_price=Decimal(str(token0_price_end)),
        token1_price=Decimal(str(token1_price_end)),
        position="end",
    )

    # fill from hype status
    current_period.fill_from_hypervisors_data(
        ini_hype=status_ini,
        end_hype=status_end,
        network=chain.database_name,
    )

    # # fill rewards
    # try:
    #     current_period.fill_from_rewards_data(
    #         ini_rewards=last_item["rewards_status"],
    #         end_rewards=current_item["rewards_status"],
    #     )
    # except ProcessingError as e:
    #     logging.getLogger(__name__).error(
    #         f" Error while creating hype returns rewards. {e.message}"
    #     )

    return current_period


def save_latest_hypervisor_returns_to_database(
    chain: Chain,
    period_yield_list: list[dict],
):
    # save all at once
    if db_return := get_default_localdb(
        network=chain.database_name
    ).replace_items_to_database(
        data=[database_local.convert_decimal_to_d128(x) for x in period_yield_list],
        collection_name="latest_hypervisor_returns",
    ):
        logging.getLogger(__name__).debug(
            f"     {chain.database_name} saved latest returns -> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
        )
    else:
        logging.getLogger(__name__).error(
            f"  database did not return anything while trying to save latest hypervisor returns to database for {chain.database_name}"
        )
