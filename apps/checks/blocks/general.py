import logging
from apps.checks.base_objects import analysis_item, base_analyzer_object
from bins.database.common.db_collections_common import database_global, database_local
from bins.database.helpers import get_default_globaldb, get_from_localdb
from bins.general.enums import Chain


def check_localdb_blocks(local_db_manager: database_local):
    """check if blocks are typed correctly

    Args:
        local_db_manager (database_local):
    """

    if blocks_operatons := local_db_manager.get_items_from_database(
        collection_name="operations",
        find={"blockNumber": {"$not": {"$type": "int"}}},
    ):
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_operatons)} operations with the block field not being int"
        )

    if blocks_status := local_db_manager.get_items_from_database(
        collection_name="status", find={"block": {"$not": {"$type": "int"}}}
    ):
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_status)} hypervisor status with the block field not being int"
        )


def check_globaldb_blocks(global_db_manager: database_global):
    """check that blocks have the correct type

    Args:
        global_db_manager (database_global):
    """

    if blocks_usd_prices := global_db_manager.get_items_from_database(
        collection_name="usd_prices", find={"block": {"$not": {"$type": "int"}}}
    ):
        logging.getLogger(__name__).warning(
            f" Found {len(blocks_usd_prices)} usd prices with the block field not being int: database '{global_db_manager._db_name}' collection 'usd_prices'   ids-> {[x['_id'] for x in blocks_usd_prices]}"
        )
        # try replacing those found non int block prices to int
        # replace_blocks_to_int()


class blocks_analyzer(base_analyzer_object):
    def __init__(self):
        super().__init__()

    def check_localdb_blocks(self, chain: Chain):
        """check if blocks are typed correctly

        Args:
            local_db_manager (database_local):
        """

        if blocks_operatons := get_from_localdb(
            network=chain.database_name,
            collection="operations",
            find={"blockNumber": {"$not": {"$type": "int"}}},
        ):

            # create item
            self.items.append(
                analysis_item(
                    name="blocks",
                    data=blocks_operatons,
                    log_message=f" Found {len(blocks_operatons)} operations with the block field not being int in {chain.database_name}",
                    telegram_message=f" Found {len(blocks_operatons)} operations with the block field not being int in {chain.database_name}",
                )
            )

        if blocks_status := get_from_localdb(
            network=chain.database_name,
            collection="status",
            find={"block": {"$not": {"$type": "int"}}},
        ):
            self.items.append(
                analysis_item(
                    name="blocks",
                    data=blocks_operatons,
                    log_message=f" Found {len(blocks_status)} hypervisor status with the block field not being int in {chain.database_name}",
                    telegram_message=f" Found {len(blocks_status)} hypervisor status with the block field not being int in {chain.database_name}",
                )
            )

    def check_globaldb_blocks(self):
        """check that blocks have the correct type"""

        if blocks_usd_prices := get_default_globaldb().get_items_from_database(
            collection_name="usd_prices", find={"block": {"$not": {"$type": "int"}}}
        ):
            self.items.append(
                analysis_item(
                    name="blocks",
                    data=blocks_usd_prices,
                    log_message=f" Found {len(blocks_usd_prices)} usd prices  with the block field not being int ",
                    telegram_message=f" Found {len(blocks_usd_prices)}usd prices with the block field not being int ",
                )
            )

    # TODO: implement a chekc_all_chains method with parallel processing
