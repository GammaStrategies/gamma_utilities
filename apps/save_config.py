import logging
from bins.config.loader.file import load_configuration_file
from bins.database.helpers import get_default_globaldb


def save_configuration_to_database(cfg_name: str):
    """Save current file configuration to database"""

    logging.getLogger(__name__).info(
        f"   Saving {cfg_name} configuration file to the database"
    )

    cfg = load_configuration_file(cfg_name=cfg_name)

    if db_return := get_default_globaldb().save_item_to_database(
        data=cfg.dict(), collection_name="configuration"
    ):
        logging.getLogger(__name__).info(
            f"Configuration saved to database: {db_return.raw_result}"
        )


def main(cfg_name: str):
    save_configuration_to_database(cfg_name=cfg_name)
