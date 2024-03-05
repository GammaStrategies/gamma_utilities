### Static ######################
import contextlib
import logging
import concurrent.futures
from multiprocessing.pool import Pool
from requests import HTTPError
import tqdm
from apps.feeds.queue.push import build_and_save_queue_items_from_hypervisor_addresses

from bins.configuration import (
    CONFIGURATION,
    STATIC_REGISTRY_ADDRESSES,
)
from bins.database.common.database_ids import create_id_rewards_static
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.errors.general import ProcessingError
from bins.general.enums import (
    Chain,
    Protocol,
    error_identity,
    rewarderType,
    text_to_chain,
    text_to_protocol,
)
from bins.w3.builders import (
    build_db_hypervisor,
    build_db_hypervisor_multicall,
    build_erc20_helper,
    build_hypervisor,
    convert_dex_protocol,
)
from bins.w3.protocols.camelot.rewarder import (
    camelot_rewards_nft_pool,
    camelot_rewards_nft_pool_master,
    camelot_rewards_nitro_pool_factory,
)

from bins.w3.protocols.general import erc20, bep20

from bins.w3.protocols.gamma.registry import gamma_hypervisor_registry
from bins.w3.protocols.gamma.rewarder import (
    gamma_masterchef_rewarder,
    gamma_rewarder,
    gamma_masterchef_registry,
    gamma_masterchef_v1,
)
from bins.w3.protocols.lynex.rewarder import lynex_voter_v5
from bins.w3.protocols.synthswap.rewarder import synthswap_masterchef_v1
from bins.w3.protocols.thena.rewarder import thena_voter_v3
from bins.w3.protocols.zyberswap.rewarder import zyberswap_masterchef_v1
from bins.w3.protocols.beamswap.rewarder import beamswap_masterchef_v2
from bins.w3.protocols.angle.rewarder import angle_merkle_distributor_creator
from bins.w3.protocols.ramses.hypervisor import gamma_hypervisor as ramses_hypervisor
from bins.w3.protocols.pharaoh.hypervisor import gamma_hypervisor as pharaoh_hypervisor
from bins.w3.protocols.cleopatra.hypervisor import (
    gamma_hypervisor as cleopatra_hypervisor,
)

from bins.apis.etherscan_utilities import etherscan_helper


# hypervisors static data
def feed_hypervisor_static(
    protocol: str, network: str, dex: str, rewrite: bool = False, threaded: bool = True
):
    """Save hypervisor static data using web3 calls from a hypervisor's registry

    Args:
        protocol (str):
        network (str):
        dex (str):
        rewrite (bool): Force rewrite all hypervisors found
        threaded (bool):
    """

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} {dex} hypervisors static information"
    )

    # hypervisors to process
    hypervisors_to_process = _get_static_hypervisors_to_process(
        network=network, dex=dex, rewrite=rewrite
    )

    # set log list of hypervisors with errors
    _errors = 0
    with tqdm.tqdm(total=len(hypervisors_to_process), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    hypervisor,
                    network,
                    dex,
                    CONFIGURATION["_custom_"][
                        "cml_parameters"
                    ].donot_enforce_contract_creation,
                )
                for hypervisor in hypervisors_to_process
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(
                    lambda p: _create_hypervisor_static_databaseObject(*p), args
                ):
                    if result:
                        # progress
                        progress_bar.set_description(
                            f' 0x..{result["address"][-4:]} processed '
                        )
                        progress_bar.refresh()

                        # add hypervisor status to database
                        if db_return := get_default_localdb(network=network).set_static(
                            data=result
                        ):
                            logging.getLogger(__name__).debug(
                                f"    hype static db_return  mod:{db_return.modified_count} ups_id:{db_return.upserted_id}"
                            )
                        # update progress
                        progress_bar.update(1)
                    else:
                        # error found
                        _errors += 1
        else:
            # get operations from database
            for hypervisor in hypervisors_to_process:
                progress_bar.set_description(
                    f" 0x..{hypervisor['address'][-4:]} to be processed"
                )
                progress_bar.refresh()
                if result := _create_hypervisor_static_databaseObject(
                    hypervisor=hypervisor,
                    network=network,
                    dex=dex,
                    donot_enforce_contract_creation=CONFIGURATION["_custom_"][
                        "cml_parameters"
                    ].donot_enforce_contract_creation,
                ):
                    # add hypervisor static data to database
                    if db_return := get_default_localdb(network=network).set_static(
                        data=result
                    ):
                        logging.getLogger(__name__).debug(
                            f"    db_return  mod:{db_return.modified_count} ups_id:{db_return.upserted_id}"
                        )
                else:
                    # error found
                    _errors += 1

                # update progress
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisors could not be scraped due to errors".format(
                    _errors,
                    len(hypervisors_to_process),
                    _errors / len(hypervisors_to_process),
                )
            )


def feed_hypervisor_static_deprecated(
    protocol: str, network: str, dex: str, rewrite: bool = False, threaded: bool = True
):
    """Save hypervisor static data using web3 calls from a hypervisor's registry

    Args:
        protocol (str):
        network (str):
        dex (str):
        rewrite (bool): Force rewrite all hypervisors found
        threaded (bool):
    """

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} {dex} hypervisors static information"
    )

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_{protocol}")

    # hypervisor addresses to process
    (
        hypervisor_addresses_to_process,
        hypervisor_addresses_disabled,
    ) = _get_static_hypervisor_addresses_to_process(
        network=network, dex=dex, rewrite=rewrite
    )

    # set log list of hypervisors with errors
    _errors = 0
    with tqdm.tqdm(
        total=len(hypervisor_addresses_to_process), leave=False
    ) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    address,
                    network,
                    dex,
                    CONFIGURATION["_custom_"][
                        "cml_parameters"
                    ].donot_enforce_contract_creation,
                )
                for address in hypervisor_addresses_to_process
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(
                    lambda p: _create_hypervisor_static_dbObject(*p), args
                ):
                    if result:
                        # progress
                        progress_bar.set_description(
                            f' 0x..{result["address"][-4:]} processed '
                        )
                        progress_bar.refresh()

                        # add hypervisor status to database
                        local_db.set_static(data=result)
                        # update progress
                        progress_bar.update(1)
                    else:
                        # error found
                        _errors += 1
        else:
            # get operations from database
            for address in hypervisor_addresses_to_process:
                progress_bar.set_description(f" 0x..{address[-4:]} to be processed")
                progress_bar.refresh()
                if result := _create_hypervisor_static_dbObject(
                    address=address,
                    network=network,
                    dex=dex,
                    donot_enforce_contract_creation=CONFIGURATION["_custom_"][
                        "cml_parameters"
                    ].donot_enforce_contract_creation,
                ):
                    # add hypervisor static data to database
                    local_db.set_static(data=result)
                else:
                    # error found
                    _errors += 1

                # update progress
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        if _errors > 0:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) hypervisors could not be scraped due to errors".format(
                    _errors,
                    len(hypervisor_addresses_to_process),
                    _errors / len(hypervisor_addresses_to_process),
                )
            )


def feed_queue_with_hypervisor_static(
    chain: Chain, protocol: Protocol, rewrite: bool = False
):
    """Create and add queue items to database using hypervisors static data

    Args:
        network (str):
        dex (str):
        rewrite (bool, optional): . Defaults to False.
    """
    logging.getLogger(__name__).info(
        f">Feeding queue with {chain.fantasy_name}'s {protocol.fantasy_name} hypervisors static queue items"
    )

    # create hypervisor addresses to process
    (
        hypervisor_addresses_to_process,
        hypervisor_addresses_disabled,
    ) = _get_static_hypervisor_addresses_to_process(
        network=chain.database_name, dex=protocol.database_name, rewrite=rewrite
    )

    # build and save queue items to database
    build_and_save_queue_items_from_hypervisor_addresses(
        hypervisor_addresses=hypervisor_addresses_to_process,
        chain=chain,
        protocol=protocol,
    )


# from address
def _create_hypervisor_static_dbObject(
    address: str,
    network: str,
    dex: str,
    donot_enforce_contract_creation: bool | None = None,
) -> dict:
    """Create a hypervisor object with static data:
         block = creation block
         timestamp = creation timestamp

    Args:
        address (str): hypervisor address
        network (str):
        block (int):
        dex (str):

    Returns:
        dict: hypervisor object ready to be saved in database
    """

    try:
        # create hypervisor object
        hypervisor = build_hypervisor(
            network=network,
            protocol=convert_dex_protocol(dex),
            block=0,
            hypervisor_address=address,
            check=True,
        )

        # convert hypervisor to dictionary static mode on
        hypervisor_data = hypervisor.as_dict(convert_bint=True, static_mode=True)

    except HTTPError as e:
        logging.getLogger(__name__).error(
            f" Cant convert {network}'s hypervisor {address.lower()} to dictionary because of a network error. -> err: {e}"
        )
        return None
    except Exception as e:
        # could be that this is not a hypervisor?
        logging.getLogger(__name__).error(
            f" Cant convert {network}'s hypervisor {address.lower()} to dictionary because it seems this address is not a hypervisor. -> err: {e}"
        )
        return None

    # add creation  block and timestamp for hypervisor
    if creation_data_hype := _get_contract_creation_block(
        network=network, contract_address=address.lower()
    ):
        logging.getLogger(__name__).debug(
            f"     setting creation block and timestamp for {network}'s hypervisor {address.lower()}"
        )
        hypervisor_data["block"] = creation_data_hype["block"]
        hypervisor_data["timestamp"] = creation_data_hype["timestamp"]
    elif not donot_enforce_contract_creation:
        # cannot process hype static info correctly. Log it and return None
        logging.getLogger(__name__).error(
            f"Could not get creation block and timestamp for {network}'s hypervisor {address.lower()}. This hype static info will not be saved."
        )
        return None
    else:
        logging.getLogger(__name__).error(
            f"    Could not get creation block and timestamp for {network}'s hypervisor {address.lower()}. Keeping block and timestamp as they are."
        )
        # raise ProcessingError(
        #     chain=text_to_chain(network),
        #     item = hypervisor_data,
        #     identity= error_identity.RETURN_NONE,
        #     action="rescrape",
        #     message=f" Could not get creation block and timestamp for {network}'s hypervisor {address.lower()}"
        # )

    # add creation  block and timestamp for pool
    if creation_data_pool := _get_contract_creation_block(
        network=network, contract_address=hypervisor_data["pool"]["address"]
    ):
        logging.getLogger(__name__).debug(
            f"     setting creation block and timestamp for {network}'s pool {hypervisor_data['pool']['address'].lower()}"
        )
        hypervisor_data["pool"]["block"] = creation_data_pool["block"]
        hypervisor_data["pool"]["timestamp"] = creation_data_pool["timestamp"]
    else:
        logging.getLogger(__name__).error(
            f"     could not get creation block and timestamp for {network}'s pool {hypervisor_data['pool']['address'].lower()}. Setting to hypervisor's creation block and timestamp"
        )
        hypervisor_data["pool"]["block"] = hypervisor_data["block"]

    return hypervisor_data


# from dict ( accepts only address)
def _create_hypervisor_static_databaseObject(
    hypervisor: dict,
    network: str,
    dex: str,
    donot_enforce_contract_creation: bool | None = None,
) -> dict:
    """Create a hypervisor object with static data:
         block = creation block
         timestamp = creation timestamp

    Args:
        hypervisor (dict): either a hypervisor dict or a dict with only the address field
        network (str):
        block (int):
        dex (str):

    Returns:
        dict: hypervisor object ready to be saved in database
    """

    try:
        hypervisor_database = (
            build_db_hypervisor_multicall(
                address=hypervisor["address"],
                network=network,
                block=0,
                dex=dex,
                pool_address=hypervisor["pool"]["address"],
                token0_address=hypervisor["pool"]["token0"]["address"],
                token1_address=hypervisor["pool"]["token1"]["address"],
                static_mode=True,
                convert_bint=True,
            )
            if "pool" in hypervisor
            else build_db_hypervisor(
                address=hypervisor["address"],
                network=network,
                block=0,
                dex=dex,
                static_mode=True,
            )
        )

    except HTTPError as e:
        logging.getLogger(__name__).error(
            f" Cant convert {network}'s hypervisor {hypervisor['address']} to dictionary because of a network error. -> err: {e}"
        )
        return None
    except Exception as e:
        # could be that this is not a hypervisor?
        logging.getLogger(__name__).error(
            f" Cant convert {network}'s hypervisor {hypervisor['address']} to dictionary because it seems this address is not a hypervisor. -> err: {e}"
        )
        return None

    if hypervisor_database is None:
        logging.getLogger(__name__).error(
            f" Cant convert {network}'s hypervisor {hypervisor['address']} to dictionary."
        )
        return None

    # add creation  block and timestamp for hypervisor
    if creation_data_hype := _get_contract_creation_block(
        network=network, contract_address=hypervisor["address"]
    ):
        logging.getLogger(__name__).debug(
            f"     setting creation block and timestamp for {network}'s hypervisor {hypervisor['address']}"
        )
        hypervisor_database["block"] = creation_data_hype["block"]
        hypervisor_database["timestamp"] = creation_data_hype["timestamp"]
    elif not donot_enforce_contract_creation:
        # cannot process hype static info correctly. Log it and return None
        logging.getLogger(__name__).error(
            f"Could not get creation block and timestamp for {network}'s hypervisor {hypervisor['address']}. This hype static info will not be saved."
        )
        return None

    elif (
        "block" in hypervisor
        and "timestamp" in hypervisor
        and hypervisor["block"] > 0
        and hypervisor["timestamp"] > 0
    ):
        # use the block and timestamp from the hypervisor passed as argument, when available
        hypervisor_database["block"] = hypervisor["block"]
        hypervisor_database["timestamp"] = hypervisor["timestamp"]

        logging.getLogger(__name__).error(
            f"    Using last saved creation block and timestamp for {network}'s hypervisor {hypervisor['address']} bc could not get em from external source."
        )
    else:
        logging.getLogger(__name__).error(
            f"    Could not get creation block and timestamp for {network}'s hypervisor {hypervisor['address']}. Keeping block and timestamp as they are."
        )

    # add creation  block and timestamp for pool
    if creation_data_pool := _get_contract_creation_block(
        network=network, contract_address=hypervisor_database["pool"]["address"]
    ):
        logging.getLogger(__name__).debug(
            f"     setting creation block and timestamp for {network}'s pool {hypervisor_database['pool']['address'].lower()}"
        )
        hypervisor_database["pool"]["block"] = creation_data_pool["block"]
        hypervisor_database["pool"]["timestamp"] = creation_data_pool["timestamp"]
    elif (
        "pool" in hypervisor
        and "block" in hypervisor["pool"]
        and "timestamp" in hypervisor["pool"]
        and hypervisor["pool"]["block"] > 0
        and hypervisor["pool"]["timestamp"] > 0
    ):
        # use the block and timestamp from the hypervisor's pool passed as argument, when available
        hypervisor_database["block"] = hypervisor["pool"]["block"]
        hypervisor_database["timestamp"] = hypervisor["pool"]["timestamp"]

        logging.getLogger(__name__).error(
            f"    Using last saved creation block and timestamp for {network}'s pool {hypervisor_database['pool']['address']} bc could not get em from external source."
        )
    else:
        logging.getLogger(__name__).error(
            f"     could not get creation block and timestamp for {network}'s pool {hypervisor_database['pool']['address'].lower()}. Setting to hypervisor's creation block and timestamp"
        )
        hypervisor_database["pool"]["block"] = hypervisor_database["block"]
        # TODO: log to telegram

    return hypervisor_database


def remove_disabled_hypervisors(chain: Chain, hypervisor_addresses: list[str]):
    """If any of the addresses are present in the static database,
        remove any trace of em from queue, status and static ( in that order )

    Args:
        chain (Chain):
        hypervisor_addresses (list[str]): list of addresses to remove
    """
    local_db = database_local(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
        db_name=f"{chain.database_name}_gamma",
    )

    # convert addresses to lower case
    hypervisor_addresses = [x.lower() for x in hypervisor_addresses]

    # only execute when found
    if local_db.get_items_from_database(
        collection_name="static", find={"id": {"$in": hypervisor_addresses}}
    ):
        logging.getLogger(__name__).debug(
            f" {len(result)} {chain.database_name} hypervisors where disabled from the hypervisor registry contract. Trying to remove their traces from database"
        )
        # remove from queue
        collection_name = "queue"
        try:
            # create bulk data object
            bulk_data = [{"filter": {"address": item}} for item in hypervisor_addresses]

            with local_db.db_manager as _db_manager:
                # add to mongodb
                if result := _db_manager.del_items_in_bulk(
                    coll_name=collection_name, data=bulk_data
                ):
                    # log removed items
                    logging.getLogger(__name__).debug(
                        f"      {result.deleted_count} of {len(result)} {chain.database_name} queued items were removed from {collection_name} database collection"
                    )
                else:
                    logging.getLogger(__name__).error(
                        f" Unable to remove multiple items from mongo's {collection_name} collection.  Items qtty: {len(hypervisor_addresses)}  mongodb returned-> {result.bulk_api_result}"
                    )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to remove multiple items from mongo's {collection_name} collection.  Items qtty: {len(hypervisor_addresses)}    error-> {e}"
            )

        # remove from status collection
        collection_name = "status"
        try:
            # create bulk data object
            bulk_data = [{"filter": {"address": item}} for item in hypervisor_addresses]

            with local_db.db_manager as _db_manager:
                # remove from db
                if result := _db_manager.del_items_in_bulk(
                    coll_name=collection_name, data=bulk_data
                ):
                    # log removed items
                    logging.getLogger(__name__).debug(
                        f"      {result.deleted_count} of {len(result)} {chain.database_name} hypervisors were removed from {collection_name} database collection"
                    )
                else:
                    logging.getLogger(__name__).error(
                        f" Unable to remove multiple items from mongo's {collection_name} collection.  Items qtty: {len(hypervisor_addresses)}  mongodb returned-> {result.bulk_api_result}"
                    )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to remove multiple items from mongo's {collection_name} collection.  Items qtty: {len(hypervisor_addresses)}    error-> {e}"
            )

        # remove from static collection
        collection_name = "static"
        try:
            # create bulk data object
            bulk_data = [{"filter": {"id": item}} for item in hypervisor_addresses]

            with local_db.db_manager as _db_manager:
                # add to mongodb
                if result := _db_manager.del_items_in_bulk(
                    coll_name=collection_name, data=bulk_data
                ):
                    # log removed items
                    logging.getLogger(__name__).debug(
                        f"      {result.deleted_count} of {len(result)} {chain.database_name} hypervisors were removed from {collection_name} database collection"
                    )
                else:
                    logging.getLogger(__name__).error(
                        f" Unable to remove multiple items from mongo's {collection_name} collection.  Items qtty: {len(hypervisor_addresses)}  mongodb returned-> {result.bulk_api_result}"
                    )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to remove multiple items from mongo's {collection_name} collection.  Items qtty: {len(hypervisor_addresses)}    error-> {e}"
            )


# update static hypervisor feeRecipients
def update_static_feeRecipients(chain: Chain, dex: Protocol, multiprocess: bool = True):
    """Update feeRecipients in hypervisor static"""

    static_hypervisors = get_from_localdb(
        network=chain.database_name,
        collection="static",
        find={"dex": dex.database_name},
    )

    logging.getLogger(__name__).info(
        f" Trying to update {len(static_hypervisors)} static hypervisors feeRecipient for {chain.database_name} {dex.database_name} "
    )

    if multiprocess:
        # prepare arguments
        args = [(chain, dex, hypervisor) for hypervisor in static_hypervisors]
        with Pool() as pool:
            for result in pool.starmap(update_static_feeRecipient, args):
                pass

    # exit multiprocess mode
    else:
        for hypervisor in static_hypervisors:
            update_static_feeRecipient(chain, dex, hypervisor)


def update_static_feeRecipient(chain: Chain, dex: Protocol, hype_static: dict):

    try:

        hypervisor_static_db = build_db_hypervisor_multicall(
            address=hype_static["address"],
            network=chain.database_name,
            block=0,
            dex=hype_static["dex"],
            pool_address=hype_static["pool"]["address"],
            token0_address=hype_static["pool"]["token0"]["address"],
            token1_address=hype_static["pool"]["token1"]["address"],
            static_mode=True,
        )

        if hypervisor_static_db == None:

            logging.getLogger(__name__).error(
                f"     Could not build {chain} hypervisor {hype_static['address']} static data with a single multicall. Falling to multiple single calls."
            )
            # build it again but without mlticall
            hypervisor_static_db = build_db_hypervisor(
                address=hype_static["address"],
                network=chain.database_name,
                block=0,
                dex=hype_static["dex"],
                static_mode=True,
            )
            if hypervisor_static_db == None:
                logging.getLogger(__name__).error(
                    f"     Could not build {chain} hypervisor {hype_static['address']} static data with multiple single calls. Cant continue."
                )
                return False

        if not "feeRecipient" in hypervisor_static_db:
            logging.getLogger(__name__).error(
                f"     No feeRecipient field found for {chain} hypervisor {hype_static['address']}"
            )
            return False

        if hypervisor_static_db["feeRecipient"] != hype_static["feeRecipient"]:
            logging.getLogger(__name__).info(
                f"     Changing feeRecipient of {chain} hypervisor {hype_static['address']} from {hype_static['feeRecipient']} to {hypervisor_static_db['feeRecipient']}"
            )

            hype_static["feeRecipient"] = hypervisor_static_db["feeRecipient"]

            if db_return := get_default_localdb(
                network=chain.database_name
            ).replace_item_to_database(data=hype_static, collection_name="static"):
                logging.getLogger(__name__).debug(
                    f"     feeRecipient update database result: mod: {db_return.modified_count} upsert: {db_return.upserted_id} "
                )
                return True
            else:
                logging.getLogger(__name__).error(
                    f"     feeRecipient update database did not return anything while updating {chain} hypervisor {hype_static['address']} feeRecipient"
                )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"     Could not update {chain} hypervisor {hype_static['address']} feeRecipient. Error: {e}"
        )

    return False


# rewards static data


def feed_rewards_static(
    network: str,
    dex: str,
    protocol: str = "gamma",
    rewrite: bool = False,
):
    batch_size = 100000

    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} {dex} rewards static information"
    )

    local_db = database_local(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
        db_name=f"{network}_{protocol}",
    )

    # set already processed static rewards
    try:
        already_processed = (
            [x["id"] for x in local_db.get_rewards_static()] if not rewrite else []
        )
    except Exception as e:
        logging.getLogger(__name__).warning(f" Could not get rewards static data : {e}")
        already_processed = []

    # get hypervisors process
    hypervisors = local_db.get_items_from_database(
        collection_name="static", find={"dex": dex}, batch_size=batch_size
    )

    # process
    if rewards_status_list := create_rewards_static(
        network=network,
        dex=dex,
        hypervisors=hypervisors,
        already_processed=already_processed,
        rewrite=rewrite,
    ):
        add_rewards_static_to_database(
            network=network, rewards_static_lst=rewards_status_list
        )


def create_rewards_static(
    network: str,
    dex: str,
    hypervisors: list[dict],
    already_processed: list,
    rewrite: bool,
    block: int = 0,
) -> list[dict]:
    """Chooses the right function to create and add rewards static data to database

    Args:
        network (str):
        dex (str):
        hypervisors (list[dict]): hypervisors as dict
        already_processed (list): already processed rewards static
        rewrite (bool):
    """

    # get hypervisors addresses to process
    hypervisor_addresses = [x["address"] for x in hypervisors]

    rewards_static_lst = []

    # One DEX may have multiple rewarder types

    # select reward type to process

    # UNIQUE REWARDERS
    # ZYBERSWAP
    if dex == Protocol.ZYBERSWAP.database_name:
        rewards_static_lst += create_rewards_static_zyberswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # THENA
    if dex == Protocol.THENA.database_name:
        # thena gauges
        rewards_static_lst += create_rewards_static_thena(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # BEAMSWAP
    if dex == Protocol.BEAMSWAP.database_name:
        rewards_static_lst += create_rewards_static_beamswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # CAMELOT
    if dex == Protocol.CAMELOT.database_name:
        rewards_static_lst += create_rewards_static_camelot(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
            convert_bint=True,
        )

    # RAMSES
    if dex == Protocol.RAMSES.database_name:
        rewards_static_lst += create_rewards_static_ramses(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # PHARAOH
    if dex == Protocol.PHARAOH.database_name:
        rewards_static_lst += create_rewards_static_pharaoh(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )
    # CLEOPATRA
    if dex == Protocol.CLEOPATRA.database_name:
        rewards_static_lst += create_rewards_static_cleopatra(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # SYNTHSWAP
    if dex == Protocol.SYNTHSWAP.database_name:
        rewards_static_lst += create_rewards_static_synthswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # MERKL REWARDS
    if dex in [
        Protocol.SUSHI.database_name,
        Protocol.RETRO.database_name,
        Protocol.UNISWAPv3.database_name,
        Protocol.CAMELOT.database_name,
        Protocol.QUICKSWAP.database_name,
    ]:
        rewards_static_lst += create_rewards_static_merkl(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # LYNEX
    if dex == Protocol.LYNEX.database_name:
        rewards_static_lst += create_rewards_static_lynex(
            chain=text_to_chain(network),
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    # GAMMA
    if dex in list(
        STATIC_REGISTRY_ADDRESSES.get(network, {})
        .get("MasterChefV2Registry", {})
        .keys()
    ):
        rewards_static_lst += create_rewards_static_gamma(
            chain=text_to_chain(network),
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            dexes=[text_to_protocol(dex)],
            rewrite=rewrite,
            block=block,
        )

    return rewards_static_lst


def add_rewards_static_to_database(network: str, rewards_static_lst: list[dict]):
    # build ids
    for data in rewards_static_lst:
        data["id"] = create_id_rewards_static(
            hypervisor_address=data["hypervisor_address"],
            rewarder_address=data["rewarder_address"],
            rewardToken_address=data["rewardToken"],
        )

    # save all items to the database at once
    if rewards_static_lst:
        if db_result := get_default_localdb(network=network).replace_items_to_database(
            data=rewards_static_lst, collection_name="rewards_static"
        ):
            logging.getLogger(__name__).debug(
                f"   database result-> ins: {db_result.inserted_count} mod: {db_result.modified_count} ups: {db_result.upserted_count} del: {db_result.deleted_count}"
            )
        else:
            logging.getLogger(__name__).exception(
                f" Could not save {len(rewards_static_lst)} rewards static items to database. No database response. data-> {rewards_static_lst}"
            )


def create_rewards_static_zyberswap(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []
    # TODO: zyberswap masterchef duo -> 0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba  creation_block: 80073186
    to_process_contract_addresses = {
        "0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower(): {
            "creation_block": 54769965,
            "type": rewarderType.ZYBERSWAP_masterchef_v1,
        }
    }
    ephemeral_cache = {
        "creation_block": {},
    }
    for masterchef_address, contract_data in to_process_contract_addresses.items():
        if contract_data["type"] == rewarderType.ZYBERSWAP_masterchef_v1:
            # create masterchef object
            zyberswap_masterchef = zyberswap_masterchef_v1(
                address=masterchef_address, network=network, block=block
            )
            rewards_data = zyberswap_masterchef.get_rewards(
                hypervisor_addresses=hypervisor_addresses, convert_bint=True
            )

            for reward_data in rewards_data:
                # add block creation data to cache
                if (
                    not reward_data["rewarder_address"]
                    in ephemeral_cache["creation_block"]
                ):
                    # add block creation data
                    if creation_data := _get_contract_creation_block(
                        network=network,
                        contract_address=reward_data["rewarder_address"],
                    ):
                        ephemeral_cache["creation_block"][
                            reward_data["rewarder_address"]
                        ] = creation_data["block"]
                    else:
                        # modify block number manually -> block num. is later used to update rewards_status from
                        ephemeral_cache["creation_block"][
                            reward_data["rewarder_address"]
                        ] = contract_data["creation_block"]

                # set creation block
                reward_data["block"] = ephemeral_cache["creation_block"][
                    reward_data["rewarder_address"]
                ]
                if (
                    rewrite
                    or create_id_rewards_static(
                        hypervisor_address=reward_data["hypervisor_address"],
                        rewarder_address=reward_data["rewarder_address"],
                        rewardToken_address=reward_data["rewardToken"],
                    )
                    not in already_processed
                ):
                    # save to database
                    result.append(reward_data)

    return result


def create_rewards_static_beamswap(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []

    to_process_contract_addresses = {
        "0x9d48141B234BB9528090E915085E0e6Af5Aad42c".lower(): {
            "creation_block": 3665586,
            "type": rewarderType.BEAMSWAP_masterchef_v2,
        }
    }
    ephemeral_cache = {
        "creation_block": {},
    }
    for masterchef_address, contract_data in to_process_contract_addresses.items():
        if contract_data["type"] == rewarderType.BEAMSWAP_masterchef_v2:
            # create masterchef object
            masterchef = beamswap_masterchef_v2(
                address=masterchef_address, network=network, block=block
            )

            for reward_data in masterchef.get_rewards(
                hypervisor_addresses=hypervisor_addresses, convert_bint=True
            ):
                # add block creation data to cache
                if (
                    not reward_data["rewarder_address"]
                    in ephemeral_cache["creation_block"]
                ):
                    # add block creation data
                    if creation_data := _get_contract_creation_block(
                        network=network,
                        contract_address=reward_data["rewarder_address"],
                    ):
                        ephemeral_cache["creation_block"][
                            reward_data["rewarder_address"]
                        ] = creation_data["block"]
                    else:
                        # modify block number manually -> block num. is later used to update rewards_status from
                        ephemeral_cache["creation_block"][
                            reward_data["rewarder_address"]
                        ] = contract_data["creation_block"]

                # set creation block
                reward_data["block"] = ephemeral_cache["creation_block"][
                    reward_data["rewarder_address"]
                ]
                if (
                    rewrite
                    or create_id_rewards_static(
                        hypervisor_address=reward_data["hypervisor_address"],
                        rewarder_address=reward_data["rewarder_address"],
                        rewardToken_address=reward_data["rewardToken"],
                    )
                    not in already_processed
                ):
                    # save to database
                    result.append(reward_data)

    return result


def create_rewards_static_thena(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:

    if network != Chain.BSC.database_name:
        raise ValueError(
            f" THENA voter address only for BINANCE chain... Cant continue.**************"
        )

    result = []
    to_process_contract_addresses = {
        "0x3a1d0952809f4948d15ebce8d345962a282c4fcb".lower(): {
            "creation_block": 27114632,
            "type": rewarderType.THENA_voter_v3,
        }
    }
    ephemeral_cache = {
        "creation_block": {},
    }

    for thenaVoter_address, contract_data in to_process_contract_addresses.items():
        # create thena voter object
        thena_voter = thena_voter_v3(
            address=thenaVoter_address, network=network, block=block
        )
        rewards_data = thena_voter.get_rewards(
            hypervisor_addresses=hypervisor_addresses, convert_bint=True
        )
        for reward_data in rewards_data:
            # add block creation data to cache
            if not reward_data["rewarder_address"] in ephemeral_cache["creation_block"]:
                # add block creation data
                if creation_data := _get_contract_creation_block(
                    network=network, contract_address=reward_data["rewarder_address"]
                ):
                    ephemeral_cache["creation_block"][
                        reward_data["rewarder_address"]
                    ] = creation_data["block"]
                else:
                    # modify block number manually -> block num. is later used to update rewards_status from
                    ephemeral_cache["creation_block"][
                        reward_data["rewarder_address"]
                    ] = contract_data["creation_block"]

            # set creation block
            reward_data["block"] = ephemeral_cache["creation_block"][
                reward_data["rewarder_address"]
            ]
            if (
                rewrite
                or create_id_rewards_static(
                    hypervisor_address=reward_data["hypervisor_address"],
                    rewarder_address=reward_data["rewarder_address"],
                    rewardToken_address=reward_data["rewardToken"],
                )
                not in already_processed
            ):
                result.append(reward_data)

    return result


def create_rewards_static_merkl(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []
    if (
        distributor_creator_address := STATIC_REGISTRY_ADDRESSES.get(
            chain.database_name, {}
        )
        .get("angle_merkl", {})
        .get("distributionCreator", None)
    ):
        # create merkl helper
        distributor_creator = angle_merkle_distributor_creator(
            address=distributor_creator_address,
            network=chain.database_name,
            block=block,
        )

        # create list of hypervisor pools: ( multiple hypes can have the same pool address )
        hype_pools = {}
        for x in hypervisors:
            if not x["pool"]["address"] in hype_pools:
                hype_pools[x["pool"]["address"]] = []
            hype_pools[x["pool"]["address"]].append(x["address"])

        # create ephemeral cache
        ephemeral_cache = {
            "tokens": {},
            "creation_block": {},
            "rewarder": {
                "block": distributor_creator.block,
                "timestamp": distributor_creator._timestamp,
                "address": distributor_creator_address.lower(),
            },
        }

        # get all campaigns from campaign list that match configured hype addresses
        for index, campaign in enumerate(distributor_creator.get_all_campaigns()):
            # check reward token validity
            if not distributor_creator.isValid_reward_token(
                reward_address=campaign["rewardToken"].lower()
            ):
                # reward token not valid
                logging.getLogger(__name__).debug(
                    f" Reward token {campaign['rewardToken']} is not valid. Merkl reward index {index} will not be processed."
                )
                continue

            if campaign["campaignData"]["pool"] in hype_pools:
                # check token in ephemeral cache
                if not campaign["rewardToken"].lower() in ephemeral_cache["tokens"]:
                    logging.getLogger(__name__).debug(
                        f" adding token {campaign['rewardToken']} in ephemeral cache"
                    )
                    # bc there is no token info in allDistributions, we need to get it from chain
                    tokenHelper = build_erc20_helper(
                        chain=chain,
                        address=campaign["rewardToken"].lower(),
                        cached=True,
                    )
                    token_symbol = tokenHelper.symbol
                    token_decimals = tokenHelper.decimals
                    ephemeral_cache["tokens"][campaign["rewardToken"].lower()] = {
                        "symbol": token_symbol,
                        "decimals": token_decimals,
                    }

                # add rewards for each hype
                for hype_address in hype_pools[campaign["campaignData"]["pool"]]:
                    # build static reward data object
                    reward_data = {
                        "block": distributor_creator.block,
                        "timestamp": distributor_creator._timestamp,
                        "hypervisor_address": hype_address.lower(),
                        "rewarder_address": campaign["campaignId"].lower(),
                        "rewarder_type": rewarderType.ANGLE_MERKLE,
                        "rewarder_refIds": [index],
                        "rewarder_registry": distributor_creator_address.lower(),
                        "rewardToken": campaign["rewardToken"].lower(),
                        "rewardToken_symbol": ephemeral_cache["tokens"][
                            campaign["rewardToken"].lower()
                        ]["symbol"],
                        "rewardToken_decimals": ephemeral_cache["tokens"][
                            campaign["rewardToken"].lower()
                        ]["decimals"],
                        "rewards_perSecond": 0,  # TODO: remove this field from all static rewards
                        "total_hypervisorToken_qtty": 0,  # TODO: remove this field from all static rewards
                        "start_rewards_timestamp": campaign["startTimestamp"],
                        "end_rewards_timestamp": campaign["startTimestamp"]
                        + campaign["duration"],
                        # "raw_data": distribution,  # CAREFUL has >8bit int in total_amount
                    }

                    # save later to database
                    if (
                        rewrite
                        or create_id_rewards_static(
                            hypervisor_address=reward_data["hypervisor_address"],
                            rewarder_address=reward_data["rewarder_address"],
                            rewardToken_address=reward_data["rewardToken"],
                        )
                        not in already_processed
                    ):
                        # add block creation data to cache
                        if (
                            not reward_data["rewarder_registry"]
                            in ephemeral_cache["creation_block"]
                        ):
                            # add block creation data
                            if creation_block := _get_contract_creation_block(
                                network=chain.database_name,
                                contract_address=reward_data["rewarder_registry"],
                            ):
                                logging.getLogger(__name__).debug(
                                    f" Found contract creation data for {reward_data['rewarder_registry']} at block {creation_block['block']}."
                                )
                                ephemeral_cache["creation_block"][
                                    reward_data["rewarder_registry"]
                                ] = creation_block["block"]
                            else:
                                # modify block number manually -> block num. is later used to update rewards_status from
                                logging.getLogger(__name__).debug(
                                    f" No contract creation data found for {reward_data['rewarder_registry']}."
                                )
                                ephemeral_cache["creation_block"][
                                    reward_data["rewarder_registry"]
                                ] = distributor_creator.block

                        reward_data["block"] = ephemeral_cache["creation_block"][
                            reward_data["rewarder_registry"]
                        ]

                        # save to database
                        result.append(reward_data)

    # return list of rewards
    return result


def create_rewards_static_ramses(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []

    # rewarder_address = gauge
    # rewarder_registry = receiver address

    ephemeral_cache = {
        "tokens": {},
        "creation_block": {},
        "receiver_address": {},
    }
    for hype_static in hypervisors:
        if rewrite or hype_static["address"].lower() not in already_processed:
            # create ramses hypervisor
            hype_status = ramses_hypervisor(
                address=hype_static["address"], network=chain.database_name, block=block
            )

            # check if gauge is set ( not 0x0000...)
            if (
                hype_status.gauge.address.lower()
                == "0x0000000000000000000000000000000000000000"
            ):
                logging.getLogger(__name__).debug(
                    f" Gauge is not set for ramses hype:{hype_static['address']}. Skipping static rewards processing"
                )
                continue

            if hype_rewards := hype_status.gauge.get_rewards(convert_bint=True):
                logging.getLogger(__name__).debug(
                    f" Found {len(hype_rewards)} static rewards for the hypervisor {hype_static['address']}"
                )

                # add block creation data to cache
                if (
                    not hype_rewards[0]["rewarder_address"]
                    in ephemeral_cache["creation_block"]
                ):
                    if creation_data := _get_contract_creation_block(
                        network=chain.database_name,
                        contract_address=hype_rewards[0]["rewarder_address"],
                    ):
                        ephemeral_cache["creation_block"][
                            hype_rewards[0]["rewarder_address"]
                        ] = creation_data["block"]
                    else:
                        logging.getLogger(__name__).debug(
                            f"  No contract creation date found for ramses reward static {hype_rewards[0]['rewarder_address']}. Using Hypervisor's {hype_static['address']} block {creation_block} "
                        )
                        ephemeral_cache["creation_block"][
                            hype_rewards[0]["rewarder_address"]
                        ] = hype_static["block"]
                # set contract creation block
                creation_block = ephemeral_cache["creation_block"][
                    hype_rewards[0]["rewarder_address"]
                ]

                for reward_data in hype_rewards:
                    # token ephemeral cache
                    if (
                        not reward_data["rewardToken"].lower()
                        in ephemeral_cache["tokens"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" adding token {reward_data['rewardToken']} in ephemeral cache"
                        )
                        # build erc20 helper
                        erc20_helper = build_erc20_helper(
                            chain=chain, address=reward_data["rewardToken"], cached=True
                        )
                        ephemeral_cache["tokens"][
                            reward_data["rewardToken"].lower()
                        ] = {
                            "symbol": erc20_helper.symbol,
                            "decimals": erc20_helper.decimals,
                        }
                    # receiver address ephemeral cache
                    if (
                        not hype_status.address.lower()
                        in ephemeral_cache["receiver_address"]
                    ):
                        ephemeral_cache["receiver_address"][
                            hype_status.address.lower()
                        ] = hype_status.receiver.address.lower()

                    reward_data["hypervisor_address"] = hype_status.address.lower()
                    reward_data["rewardToken_symbol"] = ephemeral_cache["tokens"][
                        reward_data["rewardToken"].lower()
                    ]["symbol"]
                    reward_data["rewardToken_decimals"] = ephemeral_cache["tokens"][
                        reward_data["rewardToken"].lower()
                    ]["decimals"]
                    reward_data["total_hypervisorToken_qtty"] = str(
                        hype_status.totalSupply
                    )

                    # HACK: set rewarder_registry as the 'receiver' -> MultiFeeDistribution contract address
                    reward_data["rewarder_registry"] = ephemeral_cache[
                        "receiver_address"
                    ][hype_status.address.lower()]

                    # add block creation data
                    reward_data["block"] = creation_block
                    logging.getLogger(__name__).debug(
                        f"  Processed ramses {chain.database_name}'s {reward_data['rewarder_address']} {reward_data['rewardToken_symbol']} static rewarder at {reward_data['block']}"
                    )
                    # add to result
                    result.append(reward_data)
    return result


def create_rewards_static_pharaoh(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    """Same as Ramses, but with different rewarder type

    Args:
        chain (Chain):
        hypervisors (list[dict]):
        already_processed (list[str]):
        rewrite (bool, optional): . Defaults to False.
        block (int, optional): . Defaults to 0.

    """
    result = []

    # rewarder_address = gauge
    # rewarder_registry = receiver address

    ephemeral_cache = {
        "tokens": {},
        "creation_block": {},
        "receiver_address": {},
    }
    for hype_static in hypervisors:
        if rewrite or hype_static["address"].lower() not in already_processed:
            # create hypervisor
            hype_status = pharaoh_hypervisor(
                address=hype_static["address"], network=chain.database_name, block=block
            )

            # check if gauge is set ( not 0x0000...)
            if (
                hype_status.gauge.address.lower()
                == "0x0000000000000000000000000000000000000000"
            ):
                logging.getLogger(__name__).debug(
                    f" Gauge is not set for pharaoh hype:{hype_static['address']}. Skipping static rewards processing"
                )
                continue

            if hype_rewards := hype_status.gauge.get_rewards(convert_bint=True):
                logging.getLogger(__name__).debug(
                    f" Found {len(hype_rewards)} static rewards for the hypervisor {hype_static['address']}"
                )

                # add block creation data to cache
                if (
                    not hype_rewards[0]["rewarder_address"]
                    in ephemeral_cache["creation_block"]
                ):
                    if creation_data := _get_contract_creation_block(
                        network=chain.database_name,
                        contract_address=hype_rewards[0]["rewarder_address"],
                    ):
                        ephemeral_cache["creation_block"][
                            hype_rewards[0]["rewarder_address"]
                        ] = creation_data["block"]
                    else:
                        logging.getLogger(__name__).debug(
                            f"  No contract creation date found for pharaoh reward static {hype_rewards[0]['rewarder_address']}. Using Hypervisor's {hype_static['address']} block {creation_block} "
                        )
                        ephemeral_cache["creation_block"][
                            hype_rewards[0]["rewarder_address"]
                        ] = hype_static["block"]
                # set contract creation block
                creation_block = ephemeral_cache["creation_block"][
                    hype_rewards[0]["rewarder_address"]
                ]

                for reward_data in hype_rewards:
                    # token ephemeral cache
                    if (
                        not reward_data["rewardToken"].lower()
                        in ephemeral_cache["tokens"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" adding token {reward_data['rewardToken']} in ephemeral cache"
                        )
                        # build erc20 helper
                        erc20_helper = build_erc20_helper(
                            chain=chain, address=reward_data["rewardToken"], cached=True
                        )
                        ephemeral_cache["tokens"][
                            reward_data["rewardToken"].lower()
                        ] = {
                            "symbol": erc20_helper.symbol,
                            "decimals": erc20_helper.decimals,
                        }
                    # receiver address ephemeral cache
                    if (
                        not hype_status.address.lower()
                        in ephemeral_cache["receiver_address"]
                    ):
                        ephemeral_cache["receiver_address"][
                            hype_status.address.lower()
                        ] = hype_status.receiver.address.lower()

                    reward_data["hypervisor_address"] = hype_status.address.lower()
                    reward_data["rewardToken_symbol"] = ephemeral_cache["tokens"][
                        reward_data["rewardToken"].lower()
                    ]["symbol"]
                    reward_data["rewardToken_decimals"] = ephemeral_cache["tokens"][
                        reward_data["rewardToken"].lower()
                    ]["decimals"]
                    reward_data["total_hypervisorToken_qtty"] = str(
                        hype_status.totalSupply
                    )

                    # HACK: set rewarder_registry as the 'receiver' -> MultiFeeDistribution contract address
                    reward_data["rewarder_registry"] = ephemeral_cache[
                        "receiver_address"
                    ][hype_status.address.lower()]

                    # add block creation data
                    reward_data["block"] = creation_block
                    logging.getLogger(__name__).debug(
                        f"  Processed pharaoh {chain.database_name}'s {reward_data['rewarder_address']} {reward_data['rewardToken_symbol']} static rewarder at {reward_data['block']}"
                    )
                    # add to result
                    result.append(reward_data)
    return result


def create_rewards_static_cleopatra(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    """Same as Ramses, but with different rewarder type

    Args:
        chain (Chain):
        hypervisors (list[dict]):
        already_processed (list[str]):
        rewrite (bool, optional): . Defaults to False.
        block (int, optional): . Defaults to 0.

    """
    result = []

    # rewarder_address = gauge
    # rewarder_registry = receiver address

    ephemeral_cache = {
        "tokens": {},
        "creation_block": {},
        "receiver_address": {},
    }
    for hype_static in hypervisors:
        if rewrite or hype_static["address"].lower() not in already_processed:
            # create hypervisor
            hype_status = cleopatra_hypervisor(
                address=hype_static["address"], network=chain.database_name, block=block
            )

            # check if gauge is set ( not 0x0000...)
            if (
                hype_status.gauge.address.lower()
                == "0x0000000000000000000000000000000000000000"
            ):
                logging.getLogger(__name__).debug(
                    f" Gauge is not set for cleopatra hype:{hype_static['address']}. Skipping static rewards processing"
                )
                continue

            if hype_rewards := hype_status.gauge.get_rewards(convert_bint=True):
                logging.getLogger(__name__).debug(
                    f" Found {len(hype_rewards)} static rewards for the hypervisor {hype_static['address']}"
                )

                # add block creation data to cache
                if (
                    not hype_rewards[0]["rewarder_address"]
                    in ephemeral_cache["creation_block"]
                ):
                    if creation_data := _get_contract_creation_block(
                        network=chain.database_name,
                        contract_address=hype_rewards[0]["rewarder_address"],
                    ):
                        ephemeral_cache["creation_block"][
                            hype_rewards[0]["rewarder_address"]
                        ] = creation_data["block"]
                    else:
                        logging.getLogger(__name__).debug(
                            f"  No contract creation date found for cleopatra reward static {hype_rewards[0]['rewarder_address']}. Using Hypervisor's {hype_static['address']} block {creation_block} "
                        )
                        ephemeral_cache["creation_block"][
                            hype_rewards[0]["rewarder_address"]
                        ] = hype_static["block"]
                # set contract creation block
                creation_block = ephemeral_cache["creation_block"][
                    hype_rewards[0]["rewarder_address"]
                ]

                for reward_data in hype_rewards:
                    # token ephemeral cache
                    if (
                        not reward_data["rewardToken"].lower()
                        in ephemeral_cache["tokens"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" adding token {reward_data['rewardToken']} in ephemeral cache"
                        )
                        # build erc20 helper
                        erc20_helper = build_erc20_helper(
                            chain=chain, address=reward_data["rewardToken"], cached=True
                        )
                        ephemeral_cache["tokens"][
                            reward_data["rewardToken"].lower()
                        ] = {
                            "symbol": erc20_helper.symbol,
                            "decimals": erc20_helper.decimals,
                        }
                    # receiver address ephemeral cache
                    if (
                        not hype_status.address.lower()
                        in ephemeral_cache["receiver_address"]
                    ):
                        ephemeral_cache["receiver_address"][
                            hype_status.address.lower()
                        ] = hype_status.receiver.address.lower()

                    reward_data["hypervisor_address"] = hype_status.address.lower()
                    reward_data["rewardToken_symbol"] = ephemeral_cache["tokens"][
                        reward_data["rewardToken"].lower()
                    ]["symbol"]
                    reward_data["rewardToken_decimals"] = ephemeral_cache["tokens"][
                        reward_data["rewardToken"].lower()
                    ]["decimals"]
                    reward_data["total_hypervisorToken_qtty"] = str(
                        hype_status.totalSupply
                    )

                    # HACK: set rewarder_registry as the 'receiver' -> MultiFeeDistribution contract address
                    reward_data["rewarder_registry"] = ephemeral_cache[
                        "receiver_address"
                    ][hype_status.address.lower()]

                    # add block creation data
                    reward_data["block"] = creation_block
                    logging.getLogger(__name__).debug(
                        f"  Processed cleopatra {chain.database_name}'s {reward_data['rewarder_address']} {reward_data['rewardToken_symbol']} static rewarder at {reward_data['block']}"
                    )
                    # add to result
                    result.append(reward_data)
    return result


def create_rewards_static_synthswap(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []

    to_process_contract_addresses = {
        "0xef153cb7bfc04c657cb7f582c7411556320098b9".lower(): {
            "creation_block": 2015084,
            "type": rewarderType.SYNTHSWAP_masterchef_v1,
        }
    }
    ephemeral_cache = {
        "creation_block": {},
    }
    for masterchef_address, contract_data in to_process_contract_addresses.items():
        if contract_data["type"] == rewarderType.SYNTHSWAP_masterchef_v1:
            # create masterchef object
            _masterchef = synthswap_masterchef_v1(
                address=masterchef_address, network=network, block=block
            )
            rewards_data = _masterchef.get_rewards(
                hypervisor_addresses=hypervisor_addresses, convert_bint=True
            )

            for reward_data in rewards_data:
                # add block creation data to cache
                if (
                    not reward_data["rewarder_address"]
                    in ephemeral_cache["creation_block"]
                ):
                    # add block creation data
                    if creation_data := _get_contract_creation_block(
                        network=network,
                        contract_address=reward_data["rewarder_address"],
                    ):
                        ephemeral_cache["creation_block"][
                            reward_data["rewarder_address"]
                        ] = creation_data["block"]
                    else:
                        # modify block number manually -> block num. is later used to update rewards_status from
                        ephemeral_cache["creation_block"][
                            reward_data["rewarder_address"]
                        ] = contract_data["creation_block"]

                # set creation block
                reward_data["block"] = ephemeral_cache["creation_block"][
                    reward_data["rewarder_address"]
                ]
                if (
                    rewrite
                    or create_id_rewards_static(
                        hypervisor_address=reward_data["hypervisor_address"],
                        rewarder_address=reward_data["rewarder_address"],
                        rewardToken_address=reward_data["rewardToken"],
                    )
                    not in already_processed
                ):
                    # save to database
                    result.append(reward_data)

    return result


def create_rewards_static_camelot(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
    convert_bint: bool = False,
) -> list[dict]:
    result = create_rewards_static_camelot_spNFT(
        chain=chain,
        hypervisors=hypervisors,
        already_processed=already_processed,
        rewrite=rewrite,
        block=block,
        convert_bint=convert_bint,
    )
    result += create_rewards_static_camelot_nitro(
        chain=chain,
        hypervisors=hypervisors,
        already_processed=already_processed,
        rewrite=rewrite,
        block=block,
        convert_bint=convert_bint,
    )

    return result


def create_rewards_static_camelot_spNFT(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
    convert_bint: bool = False,
):
    result = []
    # GET Active pools from camelot nft pool master
    # using multicall

    # create a camelot nft pool master object
    nft_pool_master = camelot_rewards_nft_pool_master(
        address=STATIC_REGISTRY_ADDRESSES[chain.database_name]["camelot_nft"]["master"],
        network=chain.database_name,
        block=block,
    )
    timestamp = nft_pool_master._timestamp
    # dicr <nft_pool>:< lptoken...>
    all_reward_pools_info = nft_pool_master.get_all_rewards_info(chain=chain)

    # ease access vars : create a address:hypervisor dict
    ordered_hypervisors = {x["address"]: x for x in hypervisors}
    # analyze LPtoken addresses to match any of camelots hypervisor addresses
    for nft_pool_address, nft_pool_data in all_reward_pools_info.items():
        # 1. check if lptToken is a hypervisor address
        if nft_pool_data["lpToken"] not in ordered_hypervisors:
            continue
        # 2. check if rewarder is active ( allocPoints > 0 )
        if nft_pool_data["allocPoint"] == 0:  #
            # if this rewarder is in the database, set it to inactive ->> "end_rewards_timestamp": current timestamp,
            if db_return := get_default_localdb(
                network=chain.database_name
            ).find_one_and_update(
                collection_name="rewards_static",
                find={
                    "id": create_id_rewards_static(
                        hypervisor_address=nft_pool_data["lpToken"].lower(),
                        rewarder_address=nft_pool_address.lower(),
                        rewardToken_address=nft_pool_data["grailToken"].lower(),
                    )
                },
                update={"$set": {"end_rewards_timestamp": timestamp}},
            ):
                # log updated end timestamp
                logging.getLogger(__name__).debug(
                    f" Updated {chain.database_name} {nft_pool_address} end timestamp to {timestamp}"
                )
            else:
                # no rewarder found in database
                logging.getLogger(__name__).debug(
                    f" Cant find reward static to update end timestamp for {chain.database_name} {nft_pool_address}. Should exist and doesnt. Check!"
                )
            # skip this rewarder
            continue

        # skip if rewrite is False and it is already processed
        if (
            not rewrite
            and create_id_rewards_static(
                hypervisor_address=nft_pool_data["lpToken"].lower(),
                rewarder_address=nft_pool_address.lower(),
                rewardToken_address=nft_pool_data["grailToken"].lower(),
            )
            in already_processed
        ):
            # skip this rewarder
            continue

        # 3. get data and save to database
        # get contract creation block
        if creation_data := _get_contract_creation_block(
            network=chain.database_name,
            contract_address=nft_pool_address,
        ):
            reward_static_block = creation_data["block"]
            creation_timestamp = creation_data["timestamp"]
        else:
            # modify block number manually -> block num. is later used to update rewards_status from
            reward_static_block = block
            creation_timestamp = timestamp

        # get tokens specs
        # build erc20 helper
        grailTokenHelper = build_erc20_helper(
            chain=chain, address=nft_pool_data["grailToken"], cached=True
        )
        xGrailTokenHelper = build_erc20_helper(
            chain=chain, address=nft_pool_data["xGrailToken"], cached=True
        )

        # Will create one staking reward per token (grail and xgrail), but they will share the same rewarder, so they should be updated at the same time
        # TODO: scraping Camelot spNFT status, save both token rewards, not one, so there is no need to create 2 queue items for the same rewarder
        result.append(
            {
                "block": reward_static_block,
                "timestamp": timestamp,
                "hypervisor_address": nft_pool_data["lpToken"],
                "rewarder_address": nft_pool_address,
                "rewarder_type": rewarderType.CAMELOT_spNFT,
                # TODO: id at the master pool level ?
                "rewarder_refIds": [],
                "rewarder_registry": STATIC_REGISTRY_ADDRESSES[chain.database_name][
                    "camelot_nft"
                ]["master"],
                "rewardToken": nft_pool_data["grailToken"],
                "rewardToken_symbol": grailTokenHelper.symbol,
                "rewardToken_decimals": grailTokenHelper.decimals,
                "rewards_perSecond": (
                    str(nft_pool_data["poolEmissionRate"])
                    if convert_bint
                    else nft_pool_data["poolEmissionRate"]
                ),
                "total_hypervisorToken_qtty": (
                    str(nft_pool_data["lpSupplyWithMultiplier"])
                    if convert_bint
                    else nft_pool_data["lpSupplyWithMultiplier"]
                ),
                "start_rewards_timestamp": creation_timestamp,
                "end_rewards_timestamp": 0,
            }
        )
        # append xGrail also
        result.append(
            {
                "block": reward_static_block,
                "timestamp": timestamp,
                "hypervisor_address": nft_pool_data["lpToken"],
                "rewarder_address": nft_pool_address,
                "rewarder_type": rewarderType.CAMELOT_spNFT,
                # TODO: id at the master pool level ?
                "rewarder_refIds": [],
                "rewarder_registry": STATIC_REGISTRY_ADDRESSES[chain.database_name][
                    "camelot_nft"
                ]["master"],
                "rewardToken": nft_pool_data["xGrailToken"],
                "rewardToken_symbol": xGrailTokenHelper.symbol,
                "rewardToken_decimals": xGrailTokenHelper.decimals,
                "rewards_perSecond": (
                    str(nft_pool_data["poolEmissionRate"])
                    if convert_bint
                    else nft_pool_data["poolEmissionRate"]
                ),
                "total_hypervisorToken_qtty": (
                    str(nft_pool_data["lpSupplyWithMultiplier"])
                    if convert_bint
                    else nft_pool_data["lpSupplyWithMultiplier"]
                ),
                "start_rewards_timestamp": creation_timestamp,
                "end_rewards_timestamp": 0,
            }
        )

    return result


def create_rewards_static_camelot_nitro(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
    convert_bint: bool = False,
):
    result = []

    # ease access vars : create a address:hypervisor dict
    ordered_hypervisors = {x["address"]: x for x in hypervisors}

    # GET Active pools from camelot nft pool master
    # using multicall

    # create nitro factory
    nitro_factory = camelot_rewards_nitro_pool_factory(
        address=STATIC_REGISTRY_ADDRESSES[chain.database_name]["camelot_nft"][
            "nitroPoolFactory"
        ],
        network=chain.database_name,
        block=block,
    )
    timestamp = nitro_factory._timestamp

    # get a distinct list of nftPool's ( unique addresses ) from database
    unique_nftPools = {
        item["rewarder_address"]: item
        for item in get_from_localdb(
            network=chain.database_name,
            collection="rewards_static",
            find={
                "rewarder_type": rewarderType.CAMELOT_spNFT,
                "hypervisor_address": {"$in": list(ordered_hypervisors.keys())},
            },
        )
    }

    # dicr <nitro pool address>:< data ( including spNFT pool address)...>
    all_reward_pools_info = nitro_factory.get_all_rewards_info(
        chain=chain, nft_pools=list(unique_nftPools.keys())
    )

    # process each nitro pool
    for nitro_pool_address, _pool_data in all_reward_pools_info.items():
        # 1. check if nftPool is in unique_nftPools
        if _pool_data["nftPool"] not in unique_nftPools:
            continue

        # ease var access
        _hypervisor_address = unique_nftPools[_pool_data["nftPool"]][
            "hypervisor_address"
        ]

        # # skip if rewrite is False and it is already processed
        if (
            rewrite == False
            and (
                create_id_rewards_static(
                    hypervisor_address=_hypervisor_address,
                    rewarder_address=nitro_pool_address,
                    rewardToken_address=_pool_data["rewardsToken1"]["token"].lower(),
                )
                in already_processed
                or _pool_data["rewardsToken1"]["token"].lower()
                == "0x0000000000000000000000000000000000000000"
            )
            and (
                create_id_rewards_static(
                    hypervisor_address=_hypervisor_address,
                    rewarder_address=nitro_pool_address,
                    rewardToken_address=_pool_data["rewardsToken2"]["token"].lower(),
                )
                in already_processed
                or _pool_data["rewardsToken2"]["token"].lower()
                == "0x0000000000000000000000000000000000000000"
            )
        ):
            # skip this rewarder
            continue

        # 3. get data and save to database
        # get contract creation block
        if creation_data := _get_contract_creation_block(
            network=chain.database_name,
            contract_address=nitro_pool_address,
        ):
            reward_static_block = creation_data["block"]
            creation_timestamp = creation_data["timestamp"]
        else:
            # modify block number manually -> block num. is later used to update rewards_status from
            reward_static_block = block
            creation_timestamp = timestamp

        # get tokens specs
        # build erc20 helper
        token0 = build_erc20_helper(
            chain=chain,
            address=_pool_data["rewardsToken1"]["token"].lower(),
            cached=True,
        )
        token1 = build_erc20_helper(
            chain=chain,
            address=_pool_data["rewardsToken2"]["token"].lower(),
            cached=True,
        )

        # process token1
        if (
            _pool_data["rewardsToken1"]["token"].lower()
            != "0x0000000000000000000000000000000000000000"
        ):
            result.append(
                {
                    "block": reward_static_block,
                    "timestamp": timestamp,
                    "hypervisor_address": _hypervisor_address,
                    "rewarder_address": nitro_pool_address,
                    "rewarder_type": rewarderType.CAMELOT_nitro,
                    "rewarder_refIds": [],
                    "rewarder_registry": STATIC_REGISTRY_ADDRESSES[chain.database_name][
                        "camelot_nft"
                    ]["nitroPoolFactory"],
                    "rewardToken": _pool_data["rewardsToken1"]["token"].lower(),
                    "rewardToken_symbol": token0.symbol,
                    "rewardToken_decimals": token0.decimals,
                    "rewards_perSecond": (
                        str(_pool_data["rewardsToken1PerSecond"])
                        if convert_bint
                        else _pool_data["rewardsToken1PerSecond"]
                    ),
                    "total_hypervisorToken_qtty": (
                        str(_pool_data["totalDepositAmount"])
                        if convert_bint
                        else _pool_data["totalDepositAmount"]
                    ),
                    "start_rewards_timestamp": _pool_data["settings"]["startTime"],
                    "end_rewards_timestamp": _pool_data["settings"]["endTime"],
                }
            )
        # process token2
        if (
            _pool_data["rewardsToken2"]["token"].lower()
            != "0x0000000000000000000000000000000000000000"
        ):
            result.append(
                {
                    "block": reward_static_block,
                    "timestamp": timestamp,
                    "hypervisor_address": _hypervisor_address,
                    "rewarder_address": nitro_pool_address,
                    "rewarder_type": rewarderType.CAMELOT_nitro,
                    "rewarder_refIds": [],
                    "rewarder_registry": STATIC_REGISTRY_ADDRESSES[chain.database_name][
                        "camelot_nft"
                    ]["nitroPoolFactory"],
                    "rewardToken": _pool_data["rewardsToken2"]["token"].lower(),
                    "rewardToken_symbol": token1.symbol,
                    "rewardToken_decimals": token1.decimals,
                    "rewards_perSecond": (
                        str(_pool_data["rewardsToken2PerSecond"])
                        if convert_bint
                        else _pool_data["rewardsToken2PerSecond"]
                    ),
                    "total_hypervisorToken_qtty": (
                        str(_pool_data["totalDepositAmount"])
                        if convert_bint
                        else _pool_data["totalDepositAmount"]
                    ),
                    "start_rewards_timestamp": _pool_data["settings"]["startTime"],
                    "end_rewards_timestamp": _pool_data["settings"]["endTime"],
                }
            )

    return result


def create_rewards_static_lynex(
    chain: Chain,
    hypervisor_addresses: list[str],
    already_processed: list[str],
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []

    if chain != Chain.LINEA:
        raise ValueError(
            f" LYNEX voter address only for LINEA chain... Cant continue.**************"
        )

    to_process_contract_addresses = {
        "0x0B2c83B6e39E32f694a86633B4d1Fe69d13b63c5".lower(): {
            "creation_block": 2207763,
            "type": rewarderType.LYNEX_voter_v5,
        }
    }
    ephemeral_cache = {
        "creation_block": {},
    }

    for thenaVoter_address, contract_data in to_process_contract_addresses.items():
        # create thena voter object
        _voter = lynex_voter_v5(
            address=thenaVoter_address, network=chain.database_name, block=block
        )
        rewards_data = _voter.get_rewards(
            hypervisor_addresses=hypervisor_addresses, convert_bint=True
        )
        for reward_data in rewards_data:
            # add block creation data to cache
            if not reward_data["rewarder_address"] in ephemeral_cache["creation_block"]:
                # add block creation data
                if creation_data := _get_contract_creation_block(
                    network=chain.database_name,
                    contract_address=reward_data["rewarder_address"],
                ):
                    ephemeral_cache["creation_block"][
                        reward_data["rewarder_address"]
                    ] = creation_data["block"]
                else:
                    # modify block number manually -> block num. is later used to update rewards_status from
                    ephemeral_cache["creation_block"][
                        reward_data["rewarder_address"]
                    ] = contract_data["creation_block"]

            # set creation block
            reward_data["block"] = ephemeral_cache["creation_block"][
                reward_data["rewarder_address"]
            ]
            if (
                rewrite
                or create_id_rewards_static(
                    hypervisor_address=reward_data["hypervisor_address"],
                    rewarder_address=reward_data["rewarder_address"],
                    rewardToken_address=reward_data["rewardToken"],
                )
                not in already_processed
            ):
                result.append(reward_data)

    return result


# gamma rewards
def create_rewards_static_gamma(
    chain: Chain,
    hypervisor_addresses: list[str],
    already_processed: list,
    dexes: list[Protocol] = None,
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []

    # create hype addresses

    for dex in dexes or (
        STATIC_REGISTRY_ADDRESSES.get(chain.database_name, {})
        .get("MasterChefV2Registry", {})
        .keys()
    ):
        logging.getLogger(__name__).debug(
            f"   creating static {chain.database_name} GAMMA rewards for {dex}"
        )

        # create masterchef v2 registry helper
        masterchef_registry = gamma_masterchef_registry(
            address=STATIC_REGISTRY_ADDRESSES[chain.database_name][
                "MasterChefV2Registry"
            ][dex],
            network=chain.database_name,
            block=block,
        )

        # get masterchef addresses from masterchef registry
        for masterchef_address in masterchef_registry.get_masterchef_addresses():
            # create masterchef helper
            masterchef_helper = gamma_masterchef_v1(
                address=masterchef_address, network=chain.database_name, block=block
            )
            # get rewarders from masterchef
            all_rewarders = masterchef_helper.get_rewarders(rid=0)
            if not all_rewarders:
                logging.getLogger(__name__).debug(
                    f"           No rewarders found for {chain} {masterchef_address}. Skipping."
                )
                continue
            logging.getLogger(__name__).debug(
                f"  {chain.database_name} - found {len(all_rewarders)} rewarders in masterchef {masterchef_address}"
            )
            # scrape masterchef creation block to be user as start block for rewarders
            if creation_data := _get_contract_creation_block(
                network=chain.database_name,
                contract_address=masterchef_address,
            ):
                reward_static_block = creation_data["block"]
                creation_timestamp = creation_data["timestamp"]
            else:
                # modify block number manually -> block num. is later used to update rewards_status from
                reward_static_block = masterchef_helper.block
                creation_timestamp = masterchef_helper._timestamp

            # process each rewarder &+ to result
            for rewarder_address, hypervisors_and_pids in all_rewarders.items():
                # filter hypervisor addresses
                hypervisors_and_pids = {
                    k: v
                    for k, v in hypervisors_and_pids.items()
                    if k.lower() in hypervisor_addresses
                }
                if not hypervisors_and_pids:
                    logging.getLogger(__name__).debug(
                        f"           No hypervisors found to be processed for {chain} {rewarder_address}"
                    )
                    continue

                # build rewarder & get info
                gamma_rewarder = gamma_masterchef_rewarder(
                    address=rewarder_address,
                    network=chain.database_name,
                    block=block,
                )
                rewards = gamma_rewarder.get_rewards(
                    hypervisors_and_pids=hypervisors_and_pids, filter=False
                )
                if not rewards:
                    logging.getLogger(__name__).debug(
                        f"           No active rewards found for {chain} {rewarder_address}"
                    )
                    continue

                # complete each reward info and append to result
                for reward in rewards:
                    # check if we should include this into result
                    if rewrite == False and (
                        create_id_rewards_static(
                            hypervisor_address=reward["hypervisor_address"],
                            rewarder_address=reward["rewarder_address"],
                            rewardToken_address=reward["rewardToken"].lower(),
                        )
                        in already_processed
                        or reward["rewardToken"].lower()
                        == "0x0000000000000000000000000000000000000000"
                    ):
                        # skip this rewarder
                        # logging.getLogger(__name__).debug(f"          Skipping {chain} {rewarder_address} pid {reward['rewarder_refIds']} as it is already processed")
                        continue

                    # set reward token symbol and decimals
                    ercHelper = build_erc20_helper(
                        chain=chain,
                        address=reward["rewardToken"],
                        block=reward["block"],
                    )
                    reward["rewardToken_symbol"] = ercHelper.symbol
                    reward["rewardToken_decimals"] = ercHelper.decimals
                    # set total lpToken qtty staked ( string to avoid mongo uint 8bit overflow)
                    ercHelper = build_erc20_helper(
                        chain=chain,
                        address=reward["hypervisor_address"],
                        block=reward["block"],
                    )
                    if totalLP := ercHelper.balanceOf(reward["rewarder_registry"]):
                        reward["total_hypervisorToken_qtty"] = str(totalLP)
                    else:
                        logging.getLogger(__name__).debug(
                            f"           No total LP found for {chain} hype {reward['hypervisor_address']} at rewarder {rewarder_address} pid {reward['rewarder_refIds']}"
                        )

                    # append to result
                    result.append(
                        {
                            "block": reward_static_block or reward["block"],
                            "timestamp": reward["timestamp"],
                            "hypervisor_address": reward["hypervisor_address"].lower(),
                            "rewarder_address": reward["rewarder_address"].lower(),
                            "rewarder_type": rewarderType.GAMMA_masterchef_rewarder,
                            "rewarder_refIds": reward["rewarder_refIds"],
                            "rewarder_registry": reward["rewarder_registry"].lower(),
                            "rewardToken": reward["rewardToken"].lower(),
                            "rewardToken_symbol": reward["rewardToken_symbol"],
                            "rewardToken_decimals": reward["rewardToken_decimals"],
                            "rewards_perSecond": str(reward["rewards_perSecond"]),
                            "total_hypervisorToken_qtty": str(
                                reward["total_hypervisorToken_qtty"]
                            ),
                            "start_rewards_timestamp": creation_timestamp,
                            "end_rewards_timestamp": 0,
                        }
                    )

                logging.getLogger(__name__).debug(
                    f"           {len(rewards)} rewards found for {chain} {rewarder_address} pid {reward['rewarder_refIds']}"
                )

    return result


# helpers
def _get_static_hypervisor_addresses_to_process(
    network: str,
    dex: str,
    rewrite: bool = False,
) -> tuple[list[str], list[str]]:
    """Create a list of hypervisor addresses to be scraped to feed static database collection

    Args:
        local_db (database_local):
        rewrite (bool, optional): . Defaults to False.

    Returns:
       tuple[ list[str], list[str] ]:  hypervisor addresses, disabled list of hypervisor addresses
    """
    # get hyp addresses from database
    logging.getLogger(__name__).debug(
        f"   Retrieving {network} hypervisors addresses from database"
    )
    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_gamma")

    hypervisor_addresses_database = []
    if not rewrite:
        hypervisor_addresses_database = local_db.get_distinct_items_from_database(
            collection_name="static", field="id"
        )
    else:
        logging.getLogger(__name__).info(
            f"   Rewriting all hypervisors static information of {network}'s {dex} "
        )

    # return filtered hypervisor addresses from registry
    return _get_filtered_hypervisor_addresses_from_registry(
        network=network,
        dex=dex,
        exclude_addresses=hypervisor_addresses_database,
    )


def _get_static_hypervisors_to_process(
    network: str, dex: str, rewrite: bool = False
) -> list[dict]:
    """Get a list of hypervisors to process, using the database dict format so that:
          {"address":<hypervisor address>} is returned in case the hype does not exist in the db
          {"address":<hypervisor address>, "pool":<pool dict> ... etc... } is returned in case the hype exists in the db

    Args:
        network (str):
        dex (str):
        rewrite (bool, optional): Rewrite database content. Defaults to False.

    Returns:
        list[dict]: list of hypervisors to process
    """
    result = []

    # get a list of all hypervisors already in the database
    hypervisor_static_list = {
        x["address"]: x
        for x in get_from_localdb(
            network=network, collection="static", find={"dex": dex}, batch_size=50000
        )
    }
    # create a list of addresses to filter already processed hypes
    hypervisor_addresses_toExclude = []
    if not rewrite:
        hypervisor_addresses_toExclude = list(hypervisor_static_list.keys())
    else:
        logging.getLogger(__name__).info(
            f"   Creating a list of all hypervisors static information of {network}'s {dex} ( rewrite enabled )"
        )

    # get a list of hypervisor addresses from registry
    (
        hypervisor_addresses_to_process,
        hypervisor_addresses_disabled,
    ) = _get_filtered_hypervisor_addresses_from_registry(
        network=network,
        dex=dex,
        exclude_addresses=hypervisor_addresses_toExclude,
    )
    # log disabled hypes
    if hypervisor_addresses_disabled:
        logging.getLogger(__name__).debug(
            f"  {network}'s {dex} list of disabled hypervisor addresses at registry level (not to be processed): {hypervisor_addresses_disabled}"
        )

    # create a list of hypervisors (as dict) to process
    for address in hypervisor_addresses_to_process:
        # check if already in static list
        if address in hypervisor_static_list:
            # add to result
            result.append(hypervisor_static_list[address])
        else:
            # add to result as a dict
            result.append({"address": address.lower()})

    return result


def _get_filtered_hypervisor_addresses_from_registry(
    network: str,
    dex: str,
    protocol: str = "gamma",
    block: int = 0,
    exclude_addresses: list = [],
) -> tuple[list[str], list[str]]:
    """get filtered hypervisor addresses from registry

    Args:
        network (str): _description_
        dex (str): _description_
        protocol (str, optional): _description_. Defaults to "gamma".
        block (int, optional): _description_. Defaults to 0.
        exclude_addresses (list, optional): _description_. Defaults to [].

    Returns:
        tuple[list[str],list[str]]:  list of addresses to be processed, list of addresses disabled by contract

    """
    try:
        # create registry
        registry_address = (
            STATIC_REGISTRY_ADDRESSES.get(network, {})
            .get("hypervisors", {})
            .get(dex, None)
        )
        gamma_registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network,
            block=block,
        )

        # get filters
        filters: dict = (
            CONFIGURATION["script"]["protocols"].get(protocol, {}).get("filters", {})
        )
        exclude_addresses += [
            x.lower()
            for x in filters.get("hypervisors_not_included", {}).get(network, [])
        ]
        logging.getLogger(__name__).debug(
            f"   excluding {len(exclude_addresses)} hypervisors: {exclude_addresses}"
        )
        # apply filters
        gamma_registry.apply_blacklist(blacklist=exclude_addresses)

        # get hypes & return
        return gamma_registry.get_hypervisors_addresses()

    except ValueError as e:
        logging.getLogger(__name__).error(
            f" Error while fetching hypes from {gamma_registry._network} registry   .error: {e}"
        )

    # return an empty hype address list
    return []


def _get_contract_creation_block(network: str, contract_address: str) -> dict:
    """Get the block number where a contract was created
        None will be returned if the contract was not found

    Args:
        network (str):
        contract_address (str):

    Returns:
        dict: "block":
              "timestamp":
    """
    # create an etherescan helper
    cg_helper = etherscan_helper(api_keys=CONFIGURATION["sources"]["api_keys"])

    if contract_creation_data := cg_helper.get_contract_creation(
        network=network, contract_addresses=[contract_address]
    ):
        for item in contract_creation_data:
            try:
                if (
                    "contractAddress" in item
                    and item["contractAddress"].lower() == contract_address.lower()
                ):
                    # this is the contract creation transaction

                    # create dummy web3 object
                    dummyw3 = (
                        bep20(
                            address="0x0000000000000000000000000000000000000000",
                            network=network,
                            block=0,
                        )
                        if network == "binance"
                        else erc20(
                            address="0x0000000000000000000000000000000000000000",
                            network=network,
                            block=0,
                        )
                    )
                    # retrieve block data
                    if creation_tx := dummyw3._getTransactionReceipt(
                        txHash=item["txHash"]
                    ):
                        block_data = dummyw3._getBlockData(
                            block=creation_tx.blockNumber
                        )
                        return {
                            "block": block_data.number,
                            "timestamp": block_data.timestamp,
                        }
                    else:
                        logging.getLogger(__name__).error(
                            f" Can't get the tx receipt for {item['txHash']}"
                        )
            except Exception as e:
                logging.getLogger(__name__).error(
                    f" Error while fetching contract creation data from etherscan. error: {e}"
                )

    return None
