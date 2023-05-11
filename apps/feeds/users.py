import logging

from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.database.db_user_status import user_status_hypervisor_builder


### user Status #######################


def feed_user_status(network: str, protocol: str):
    # get hypervisor addresses from database
    hypervisor_addresses = get_hypervisor_addresses_from_database(
        network=network, protocol=protocol
    )

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} user status information for {len(hypervisor_addresses)} hypervisors"
    )

    for idx, address in enumerate(hypervisor_addresses):
        logging.getLogger(__name__).info(
            f"   [{idx} of {len(hypervisor_addresses)}] Building {network}'s {address} user status"
        )

        hype_new = user_status_hypervisor_builder(
            hypervisor_address=address, network=network, protocol=protocol
        )

        try:
            hype_new._process_operations()
        except Exception:
            logging.getLogger(__name__).exception(
                f" Unexpected error while feeding user status of {network}'s  {address}"
            )


# helpers
def get_hypervisor_addresses_from_database(
    network: str, protocol: str, filtered: bool = True
) -> list[str]:
    result = []
    # get database configuration
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    # get blacklisted hypervisors
    if filtered:
        blacklisted = (
            CONFIGURATION.get("script", {})
            .get("protocols", {})
            .get(protocol, {})
            .get("filters", {})
            .get("hypervisors_not_included", {})
            .get(network, [])
        )
    # check n clean
    if blacklisted is None:
        blacklisted = []

    # retrieve all addresses from database
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    result = local_db_manager.get_distinct_items_from_database(
        collection_name="static",
        field="address",
        condition={"address": {"$nin": blacklisted}},
    )

    return result
