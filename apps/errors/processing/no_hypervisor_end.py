import logging
from apps.feeds.operations import feed_operations
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.errors.general import ProcessingError
from bins.general.enums import text_to_protocol
from bins.w3.builders import build_db_hypervisor_multicall


def actions_on_no_hypervisor_period_end(error: ProcessingError):
    rescrape_block_ini = error.item["ini_block"]
    rescrape_block_end = error.item["end_block"]

    # Missing operations ?  rescrape operations for this chain between defined blocks

    logging.getLogger(__name__).info(
        f" Rescraping operations for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: no hypervisor period end error"
    )
    feed_operations(
        protocol="gamma",
        network=error.chain.database_name,
        block_ini=rescrape_block_ini,
        block_end=rescrape_block_end,
    )
