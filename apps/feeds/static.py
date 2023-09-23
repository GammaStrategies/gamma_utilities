### Static ######################
import contextlib
import logging
import concurrent.futures
import tqdm

from bins.configuration import (
    CONFIGURATION,
    STATIC_REGISTRY_ADDRESSES,
)
from bins.database.common.database_ids import create_id_rewards_static
from bins.database.common.db_collections_common import database_local
from bins.errors.general import ProcessingError
from bins.general.enums import (
    Chain,
    Protocol,
    error_identity,
    rewarderType,
    text_to_chain,
)
from bins.w3.builders import build_erc20_helper, build_hypervisor, convert_dex_protocol

from bins.w3.protocols.general import erc20, bep20

from bins.w3.protocols.gamma.registry import gamma_hypervisor_registry
from bins.w3.protocols.gamma.rewarder import (
    gamma_rewarder,
    gamma_masterchef_registry,
    gamma_masterchef_v1,
)
from bins.w3.protocols.synthswap.rewarder import synthswap_masterchef_v1
from bins.w3.protocols.thena.rewarder import thena_voter_v3
from bins.w3.protocols.zyberswap.rewarder import zyberswap_masterchef_v1
from bins.w3.protocols.beamswap.rewarder import beamswap_masterchef_v2
from bins.w3.protocols.angle.rewarder import angle_merkle_distributor_creator
from bins.w3.protocols.ramses.hypervisor import gamma_hypervisor as ramses_hypervisor

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

    # debug variables
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # set local database name and create manager
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_{protocol}")

    # hypervisor addresses to process
    (
        hypervisor_addresses_to_process,
        hypervisor_addresses_disabled,
    ) = _get_static_hypervisor_addresses_to_process(
        protocol=protocol, network=network, dex=dex, rewrite=rewrite
    )

    # set log list of hypervisors with errors
    _errors = 0
    with tqdm.tqdm(
        total=len(hypervisor_addresses_to_process), leave=False
    ) as progress_bar:
        if threaded:
            # threaded
            args = (
                (address, network, dex) for address in hypervisor_addresses_to_process
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


def _create_hypervisor_static_dbObject(
    address: str,
    network: str,
    dex: str,
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
            cached=False,
            check=True,
        )

        # convert hypervisor to dictionary static mode on
        hypervisor_data = hypervisor.as_dict(convert_bint=True, static_mode=True)

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
    else:
        # cannot process hype static info correctly. Log it and return None
        logging.getLogger(__name__).error(
            f"Could not get creation block and timestamp for {network}'s hypervisor {address.lower()}. This hype static info will not be saved."
        )
        return None
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
    create_n_add_reward_static(
        network=network,
        dex=dex,
        hypervisors=hypervisors,
        already_processed=already_processed,
        rewrite=rewrite,
    )


def create_n_add_reward_static(
    network: str,
    dex: str,
    hypervisors: list[dict],
    already_processed: list,
    rewrite: bool,
    block: int = 0,
):
    """Chooses the right function to create and add rewards static data to database

    Args:
        network (str):
        dex (str):
        hypervisors (list[dict]): hypervisors as dict
        already_processed (list): already processed rewards static
        rewrite (bool):
    """
    local_db = database_local(
        mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
        db_name=f"{network}_gamma",
    )

    # get hypervisors addresses to process
    hypervisor_addresses = [x["address"] for x in hypervisors]

    rewards_static_lst = []

    # One DEX may have multiple rewarder types

    # select reward type to process
    if dex == Protocol.ZYBERSWAP.database_name:
        rewards_static_lst = create_rewards_static_zyberswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    elif dex == Protocol.THENA.database_name:
        # thena gauges
        rewards_static_lst = create_rewards_static_thena(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    elif dex == Protocol.BEAMSWAP.database_name:
        rewards_static_lst = create_rewards_static_beamswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    elif dex in [
        Protocol.SUSHI.database_name,
        Protocol.RETRO.database_name,
        Protocol.UNISWAPv3.database_name,
    ]:
        rewards_static_lst = create_rewards_static_merkl(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    elif dex == Protocol.RAMSES.database_name:
        rewards_static_lst = create_rewards_static_ramses(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )
        # Merkle also ?

    elif dex == Protocol.SYNTHSWAP.database_name:
        rewards_static_lst = create_rewards_static_synthswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        )

    else:
        # raise NotImplementedError(f"{network} {dex} not implemented")
        return

    # build ids
    for data in rewards_static_lst:
        data["id"] = create_id_rewards_static(
            hypervisor_address=data["hypervisor_address"],
            rewarder_address=data["rewarder_address"],
            rewardToken_address=data["rewardToken"],
        )

    # save all items to the database at once
    if rewards_static_lst:
        db_result = local_db.replace_items_to_database(
            data=rewards_static_lst, collection_name="rewards_static"
        )

        logging.getLogger(__name__).debug(
            f"   database result-> ins: {db_result.inserted_count} mod: {db_result.modified_count} ups: {db_result.upserted_count} del: {db_result.deleted_count}"
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

        # get distributor address
        # distributor_address = distributor_creator.distributor.lower()

        ephemeral_cache = {
            "tokens": {},
            "creation_block": {},
            "rewarder": {
                "block": distributor_creator.block,
                "timestamp": distributor_creator._timestamp,
                "address": distributor_creator_address.lower(),
            },
        }

        # get all distributions from distribution list that match configured hype addresses
        for index, distribution in enumerate(distributor_creator.getAllDistributions):
            # check reward token validity
            if not distributor_creator.isValid_reward_token(
                reward_address=distribution["token"].lower()
            ):
                # reward token not valid
                logging.getLogger(__name__).debug(
                    f" Reward token {distribution['token']} is not valid. Merkl reward index {index} will not be processed."
                )
                continue

            if distribution["pool"] in hype_pools:
                # check token in ephemeral cache
                if not distribution["token"].lower() in ephemeral_cache["tokens"]:
                    logging.getLogger(__name__).debug(
                        f" adding token {distribution['token']} in ephemeral cache"
                    )
                    # bc there is no token info in allDistributions, we need to get it from chain
                    tokenHelper = build_erc20_helper(
                        chain=chain, address=distribution["token"].lower(), cached=True
                    )
                    token_symbol = tokenHelper.symbol
                    token_decimals = tokenHelper.decimals
                    ephemeral_cache["tokens"][distribution["token"].lower()] = {
                        "symbol": token_symbol,
                        "decimals": token_decimals,
                    }

                # add rewards for each hype
                for hype_address in hype_pools[distribution["pool"]]:
                    # build static reward data object
                    reward_data = {
                        "block": distributor_creator.block,
                        "timestamp": distributor_creator._timestamp,
                        "hypervisor_address": hype_address.lower(),
                        "rewarder_address": distribution["rewardId"].lower(),
                        "rewarder_type": rewarderType.ANGLE_MERKLE,
                        "rewarder_refIds": [index],
                        "rewarder_registry": distributor_creator_address.lower(),
                        "rewardToken": distribution["token"].lower(),
                        "rewardToken_symbol": ephemeral_cache["tokens"][
                            distribution["token"].lower()
                        ]["symbol"],
                        "rewardToken_decimals": ephemeral_cache["tokens"][
                            distribution["token"].lower()
                        ]["decimals"],
                        "rewards_perSecond": 0,  # TODO: remove this field from all static rewards
                        "total_hypervisorToken_qtty": 0,  # TODO: remove this field from all static rewards
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


# TODO: complete gamma rewards
def create_rewards_static_gamma(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
) -> list[dict]:
    result = []
    for dex in (
        [dex]
        if dex
        else STATIC_REGISTRY_ADDRESSES.get(chain.database_name, {})
        .get("MasterChefV2Registry", {})
        .keys()
    ):
        logging.getLogger(__name__).info(
            f"   creating static {chain.database_name} rewards for {dex}"
        )

        # create masterchef v2 registry helper
        masterchef_registry = gamma_masterchef_registry(
            address=STATIC_REGISTRY_ADDRESSES[chain.database_name][
                "MasterChefV2Registry"
            ][dex],
            network=chain.database_name,
        )

        # get masterchef addresses from masterchef registry
        for registry_address in masterchef_registry.get_masterchef_addresses():
            # create reward registry
            reward_registry = gamma_masterchef_v1(
                address=registry_address, network=chain.database_name
            )

    return result


# def feed_gamma_masterchef_static(
#     network: str | None = None,
#     dex: str | None = None,
#     protocol: str = "gamma",
#     rewrite: bool = False,
# ):
#     logging.getLogger(__name__).info(f">Feeding rewards static information")

#     for network in [network] if network else STATIC_REGISTRY_ADDRESSES.keys():
#         for dex in (
#             [dex]
#             if dex
#             else STATIC_REGISTRY_ADDRESSES.get(network, {})
#             .get("MasterChefV2Registry", {})
#             .keys()
#         ):
#             logging.getLogger(__name__).info(
#                 f"   feeding {protocol}'s {network} rewards for {dex}"
#             )
#             # set local database name and create manager
#             local_db = database_local(
#                 mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"],
#                 db_name=f"{network}_{protocol}",
#             )

#             # set already processed static rewards
#             try:
#                 already_processed = (
#                     [x["id"] for x in local_db.get_rewards_static()]
#                     if not rewrite
#                     else []
#                 )
#             except Exception as e:
#                 logging.getLogger(__name__).warning(
#                     f" Could not get rewards static data : {e}"
#                 )
#                 already_processed = []


#             # TODO: masterchef v1 registry

#             # masterchef v2 registry
#             address = STATIC_REGISTRY_ADDRESSES[network]["MasterChefV2Registry"][dex]

#             # create masterchef registry
#             registry = gamma_masterchef_registry(address, network)

#             # get masterchef addresses from masterchef registry
#             reward_registry_addresses = registry.get_masterchef_addresses()

#             for registry_address in reward_registry_addresses:
#                 # create reward registry
#                 reward_registry = gamma_masterchef_v1(
#                     address=registry_address, network=network
#                 )

#                 for i in range(reward_registry.poolLength):
#                     # TODO: try catch exceptions and rise them for hypervisor_address
#                     # get hypervisor address
#                     hypervisor_address = reward_registry.lpToken(pid=i)

#                     # TODO: how to scrape rid ?
#                     for rid in range(100):
#                         try:
#                             # get reward address
#                             rewarder_address = reward_registry.getRewarder(
#                                 pid=i, rid=rid
#                             )

#                             # get rewarder
#                             rewarder = gamma_masterchef_rewarder(
#                                 address=rewarder_address, network=network
#                             )

#                             result = rewarder.as_dict(convert_bint=True)

#                             # manually add hypervisor address to rewarder
#                             result["hypervisor_address"] = hypervisor_address.lower()

#                             # manually add dex
#                             result["dex"] = dex
#                             result["pid"] = i
#                             result["rid"] = rid

#                             # save to database
#                             local_db.set_rewards_static(data=result)

#                         except ValueError:
#                             # no more rid's
#                             break
#                         except Exception as e:
#                             if rewarder_address:
#                                 logging.getLogger(__name__).exception(
#                                     f"   Unexpected error while feeding db with rewarder {rewarder_address}. hype: {hypervisor_address}  . error:{e}"
#                                 )
#                             else:
#                                 logging.getLogger(__name__).exception(
#                                     f"   Unexpected error while feeding db with rewarders from {reward_registry_addresses} registry. hype: {hypervisor_address}  . error:{e}"
#                                 )
#                             break


# helpers
def _get_static_hypervisor_addresses_to_process(
    protocol: str,
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
    local_db = database_local(mongo_url=mongo_url, db_name=f"{network}_{protocol}")

    hypervisor_addresses_database = []
    if not rewrite:
        hypervisor_addresses_database = local_db.get_distinct_items_from_database(
            collection_name="static", field="id"
        )
    else:
        logging.getLogger(__name__).info(
            f"   Rewriting all hypervisors static information of {network}'s {protocol} {dex} "
        )

    # return filtered hypervisor addresses from registry
    return _get_filtered_hypervisor_addresses_from_registry(
        network=network,
        dex=dex,
        protocol=protocol,
        exclude_addresses=hypervisor_addresses_database,
    )


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
                    creation_tx = dummyw3._getTransactionReceipt(txHash=item["txHash"])
                    block_data = dummyw3._getBlockData(block=creation_tx.blockNumber)
                    return {
                        "block": block_data.number,
                        "timestamp": block_data.timestamp,
                    }
            except Exception as e:
                logging.getLogger(__name__).error(
                    f" Error while fetching contract creation data from etherscan. error: {e}"
                )

    return None
