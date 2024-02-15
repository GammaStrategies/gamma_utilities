# Specific processing functions
import logging

from apps.feeds.queue.push import (
    build_and_save_queue_from_hypervisor_static,
    build_and_save_queue_from_hypervisor_status,
)
from apps.feeds.queue.queue_item import QueueItem
from apps.feeds.static import _create_hypervisor_static_dbObject
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.w3.builders import build_db_hypervisor_multicall


def pull_from_queue_hypervisor_static(network: str, queue_item: QueueItem) -> bool:
    if hype_static := _create_hypervisor_static_dbObject(
        address=queue_item.address,
        network=network,
        dex=queue_item.data["protocol"],
        enforce_contract_creation=CONFIGURATION["_custom_"][
            "cml_parameters"
        ].enforce_contract_creation,
    ):
        # add hypervisor static data to database
        if db_return := get_default_localdb(network=network).set_static(
            data=hype_static
        ):
            logging.getLogger(__name__).debug(
                f" {network}'s hypervisor {queue_item.address} static dbResult-> mod:{db_return.modified_count} ups:{db_return.upserted_id} match: {db_return.matched_count}"
            )

            # create a reward static queue item for this hypervisor, if needed
            if queue_item.data.get("create_reward_static", False):
                build_and_save_queue_from_hypervisor_static(
                    hypervisor_static=hype_static, network=network
                )

            return True

        logging.getLogger(__name__).debug(
            f" {network}'s hypervisor {queue_item.address} static could not be saved"
        )

    else:
        logging.getLogger(__name__).debug(
            f" {network}'s hypervisor {queue_item.address} static dictionary object could not be created"
        )
    return False


def pull_from_queue_hypervisor_status(network: str, queue_item: QueueItem) -> bool:

    try:
        # get hypervisor static information
        if hypervisor_static := get_from_localdb(
            network=network, collection="static", find={"address": queue_item.address}
        ):
            hypervisor_static = hypervisor_static[0]

            if hypervisor := build_db_hypervisor_multicall(
                address=queue_item.address,
                network=network,
                block=queue_item.block,
                dex=hypervisor_static["dex"],
                pool_address=hypervisor_static["pool"]["address"],
                token0_address=hypervisor_static["pool"]["token0"]["address"],
                token1_address=hypervisor_static["pool"]["token1"]["address"],
                force_rpcType="private",
            ):
                # save hype
                if db_return := get_default_localdb(network).set_status(
                    data=hypervisor
                ):
                    # evaluate if price has been saved
                    if (
                        db_return.upserted_id
                        or db_return.modified_count
                        or db_return.matched_count
                    ):
                        logging.getLogger(__name__).debug(
                            f" {network} queue item {queue_item.id} hypervisor status saved to database"
                        )
                        # set queue from hype status operation
                        build_and_save_queue_from_hypervisor_status(
                            hypervisor_status=hypervisor, network=network
                        )
                        # set result
                        return True
                    else:
                        logging.getLogger(__name__).error(
                            f" {network} queue item {queue_item.id} reward status not saved to database. database returned: {db_return.raw_result}"
                        )
                else:
                    logging.getLogger(__name__).error(
                        f" No database return received while trying to save results for {network} queue item {queue_item.id}"
                    )

            else:
                logging.getLogger(__name__).error(
                    f"Error building {network}'s hypervisor status for {queue_item.address}. Can't continue queue item {queue_item.id}"
                )
        else:
            logging.getLogger(__name__).error(
                f" {network} No hypervisor static found for {queue_item.address}. Can't continue queue item {queue_item.id}"
            )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s hypervisor status queue item: {e}"
        )

    # return result
    return False
