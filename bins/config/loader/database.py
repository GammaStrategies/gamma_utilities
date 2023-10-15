import sys
from ...database.common.db_collections_common import database_global
from ...errors.general import ConfigurationError
from ..objects.general import config


def load_configuration_db(mongodb_url: str) -> config | None:
    """Modify configuration object using database

    Args:
        cfg (config): configuration object

    Returns:
        config:  modified configuration object
    """
    cfg: config = None
    if db_configuration := database_global(
        mongo_url=mongodb_url
    ).get_items_from_database(
        collection_name="configuration", find={"id": "client_configuration"}
    ):
        # try:
        # add db_configuration into loaded config
        cfg = config(**db_configuration[0])
        # except ConfigurationError as e:
        #     if e.action == "exit":
        #         print(f" Error loading database configuration: {e.message}")
        #         sys.exit(1)
        #     print(f"Error loading database configuration: {e}")

        # except Exception as e:
        #     print(f"Error while loading database configuration: {e}")
    else:
        # no configuration found in database
        pass

    #
    return cfg
