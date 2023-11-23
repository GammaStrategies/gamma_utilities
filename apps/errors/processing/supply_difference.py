import logging
from apps.feeds.operations import feed_operations
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.errors.general import ProcessingError
from bins.general.enums import text_to_protocol
from bins.w3.builders import build_db_hypervisor_multicall


def actions_on_supply_difference(error: ProcessingError):
    # 1) Snapshots are not correct?
    logging.getLogger(__name__).info(
        f" Rescraping snapshots for {error.chain.database_name} of blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: supply difference error"
    )
    # use multicall to ger the whole snapshot in 2 calls
    # to use multicall, we need pool and token addresses: get from database static
    if _hypervisor_static := get_from_localdb(
        network=error.chain.database_name,
        collection="static",
        find={"address": error.item["hypervisor_address"]},
    ):
        _hypervisor_static = _hypervisor_static[0]

        ini_hype_db = build_db_hypervisor_multicall(
            address=error.item["hypervisor_address"],
            network=error.chain.database_name,
            block=error.item["ini_block"],
            dex=text_to_protocol(error.item["dex"]),
            pool_address=_hypervisor_static["pool"]["address"],
            token0_address=_hypervisor_static["pool"]["token0"]["address"],
            token1_address=_hypervisor_static["pool"]["token1"]["address"],
            convert_bint=True,
        )

        end_hype_db = build_db_hypervisor_multicall(
            address=error.item["hypervisor_address"],
            network=error.chain.database_name,
            block=error.item["end_block"],
            dex=text_to_protocol(error.item["dex"]),
            pool_address=_hypervisor_static["pool"]["address"],
            token0_address=_hypervisor_static["pool"]["token0"]["address"],
            token1_address=_hypervisor_static["pool"]["token1"]["address"],
            convert_bint=True,
        )

        if supply_diff := int(end_hype_db["totalSupply"]) - int(
            ini_hype_db["totalSupply"]
        ):
            # log the difference and continue
            logging.getLogger(__name__).debug(
                f"  Database snapshots are correct because totalSupply has a difference of {supply_diff}  for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}"
            )
        else:
            # save snapshots to database and log
            # TODO: bulk write
            db_return_ini = get_default_localdb(
                network=error.chain.database_name
            ).set_status(ini_hype_db)
            db_return_end = get_default_localdb(
                network=error.chain.database_name
            ).set_status(end_hype_db)
            logging.getLogger(__name__).debug(
                f" Hypervisor {error.item.get('hypervisor_name')} had incorrect totalSupply between ini/end period. Now solved. new data to db. Ini: {db_return_ini.modified_count}. End: {db_return_end.modified_count}"
            )
            # end of the process
            return

    # 2) Missing operations ?  rescrape operations for this chain between defined blocks
    rescrape_block_ini = error.item["ini_block"]
    rescrape_block_end = error.item["end_block"]
    logging.getLogger(__name__).info(
        f" Rescraping operations for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: supply difference error"
    )
    feed_operations(
        protocol="gamma",
        network=error.chain.database_name,
        block_ini=rescrape_block_ini,
        block_end=rescrape_block_end,
    )

    # use multicall to ger the whole snapshot in 2 calls
    # to use multicall, we need pool and token addresses: get from database static
    if _hypervisor_static := get_from_localdb(
        network=error.chain.database_name,
        collection="static",
        find={"address": error.item["hypervisor_address"]},
    ):
        _hypervisor_static = _hypervisor_static[0]

        ini_hype_db = build_db_hypervisor_multicall(
            address=error.item["hypervisor_address"],
            network=error.chain.database_name,
            block=error.item["ini_block"],
            dex=text_to_protocol(error.item["dex"]),
            pool_address=_hypervisor_static["pool"]["address"],
            token0_address=_hypervisor_static["pool"]["token0"]["address"],
            token1_address=_hypervisor_static["pool"]["token1"]["address"],
            convert_bint=True,
        )

        end_hype_db = build_db_hypervisor_multicall(
            address=error.item["hypervisor_address"],
            network=error.chain.database_name,
            block=error.item["end_block"],
            dex=text_to_protocol(error.item["dex"]),
            pool_address=_hypervisor_static["pool"]["address"],
            token0_address=_hypervisor_static["pool"]["token0"]["address"],
            token1_address=_hypervisor_static["pool"]["token1"]["address"],
            convert_bint=True,
        )

        ini_uncollected_0 = int(ini_hype_db["fees_uncollected"]["qtty_token0"])
        ini_uncollected_1 = int(ini_hype_db["fees_uncollected"]["qtty_token1"])
        end_uncollected_0 = int(end_hype_db["fees_uncollected"]["qtty_token0"])
        end_uncollected_1 = int(end_hype_db["fees_uncollected"]["qtty_token1"])
        fees_uncollected_0 = end_uncollected_0 - ini_uncollected_0
        fees_uncollected_1 = end_uncollected_1 - ini_uncollected_1

        if (
            fees_uncollected_0 != error.item["fees_token0"]
            or fees_uncollected_1 != error.item["fees_token1"]
        ):
            # save new hypes to db
            # TODO: bulk write
            db_return_ini = get_default_localdb(
                network=error.chain.database_name
            ).set_status(ini_hype_db)
            db_return_end = get_default_localdb(
                network=error.chain.database_name
            ).set_status(end_hype_db)
            logging.getLogger(__name__).debug(
                f" Hypervisor {error.item.get('hypervisor_name')} had incorrect uncollected fees between ini/end period. Now solved. new data to db. Ini: {db_return_ini.modified_count}. End: {db_return_end.modified_count}"
            )
            # end of the process
            return
    else:
        logging.getLogger(__name__).error(
            f"Error getting static data for hypervisor {error.item.get('hypervisor_name')}. Cant check if uncollected fees are correct"
        )

    # 3) Scrape operations between blocks: maybe there are some operation missing
    rescrape_block_ini = error.item["ini_block"]
    rescrape_block_end = error.item["end_block"]

    # rescrape operations for this chain between defined blocks
    logging.getLogger(__name__).debug(
        f" Rescraping operations for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: negative fees error"
    )
    feed_operations(
        protocol="gamma",
        network=error.chain.database_name,
        block_ini=rescrape_block_ini,
        block_end=rescrape_block_end,
    )
