### Static ######################
import contextlib
import logging
import concurrent.futures
import tqdm

from bins.configuration import (
    CONFIGURATION,
    STATIC_REGISTRY_ADDRESSES,
)
from bins.database.common.db_collections_common import database_local
from bins.general.enums import Chain, Protocol, rewarderType, text_to_chain
from bins.w3.builders import build_erc20_helper, build_hypervisor, convert_dex_protocol

from bins.w3.protocols.general import erc20, bep20

from bins.w3.protocols.gamma.registry import gamma_hypervisor_registry
from bins.w3.protocols.gamma.rewarder import (
    gamma_rewarder,
    gamma_masterchef_registry,
    gamma_masterchef_v1,
)
from bins.w3.protocols.thena.rewarder import thena_voter_v3
from bins.w3.protocols.zyberswap.rewarder import zyberswap_masterchef_v1
from bins.w3.protocols.beamswap.rewarder import beamswap_masterchef_v2
from bins.w3.protocols.angle.rewarder import angle_merkle_distributor_creator

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
    hypervisor_addresses_to_process = _get_static_hypervisor_addresses_to_process(
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

    # create hypervisor object
    hypervisor = build_hypervisor(
        network=network,
        protocol=convert_dex_protocol(dex),
        block=0,
        hypervisor_address=address,
        cached=False,
    )

    # convert hypervisor to dictionary static mode on
    hypervisor_data = hypervisor.as_dict(convert_bint=True, static_mode=True)

    # add contract block and timestamp at creation
    if creation_data := _get_contract_creation_block(
        network=network, contract_address=address
    ):
        logging.getLogger(__name__).debug(
            f"     setting creation block and timestamp for {network}'s {address}"
        )
        hypervisor_data["block"] = creation_data["block"]
        hypervisor_data["timestamp"] = creation_data["timestamp"]

    return hypervisor_data


# rewards static data


def feed_rewards_static(
    network: str | None = None,
    dex: str | None = None,
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

    # zyberswap masterchef
    if dex == Protocol.ZYBERSWAP.database_name:
        for rewards_static in create_rewards_static_zyberswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        ):
            # save to database
            local_db.set_rewards_static(data=rewards_static)

    elif dex == Protocol.THENA.database_name:
        # thena gauges
        for rewards_static in create_rewards_static_thena(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        ):
            # save to database
            local_db.set_rewards_static(data=rewards_static)

    elif dex == Protocol.BEAMSWAP.database_name:
        for rewards_static in create_rewards_static_beamswap(
            network=network,
            hypervisor_addresses=hypervisor_addresses,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        ):
            # save to database
            local_db.set_rewards_static(data=rewards_static)

    elif dex == Protocol.SUSHI.database_name:
        for rewards_static in create_rewards_static_merkl(
            chain=text_to_chain(network),
            hypervisors=hypervisors,
            already_processed=already_processed,
            rewrite=rewrite,
            block=block,
        ):
            # save to database
            local_db.set_rewards_static(data=rewards_static)

    else:
        # raise NotImplementedError(f"{network} {dex} not implemented")
        pass


def create_rewards_static_zyberswap(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
):
    # TODO: zyberswap masterchef duo -> 0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba  creation_block: 80073186
    to_process_contract_addresses = {
        "0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower(): {
            "creation_block": 54769965,
            "type": rewarderType.ZYBERSWAP_masterchef_v1,
        }
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
                # add block creation data
                if creation_data := _get_contract_creation_block(
                    network=network, contract_address=reward_data["rewarder_address"]
                ):
                    reward_data["block"] = creation_data["block"]
                else:
                    # modify block number manually -> block num. is later used to update rewards_status from
                    reward_data["block"] = contract_data["creation_block"]
                if (
                    rewrite
                    or f"{reward_data['hypervisor_address']}_{reward_data['rewarder_address']}"
                    not in already_processed
                ):
                    # save to database
                    yield reward_data


def create_rewards_static_beamswap(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
):
    to_process_contract_addresses = {
        "0x9d48141B234BB9528090E915085E0e6Af5Aad42c".lower(): {
            "creation_block": 3665586,
            "type": rewarderType.BEAMSWAP_masterchef_v2,
        }
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
            # add block creation data
            if creation_data := _get_contract_creation_block(
                network=network, contract_address=reward_data["rewarder_address"]
            ):
                reward_data["block"] = creation_data["block"]
            else:
                # modify block number manually -> block num. is later used to update rewards_status from
                reward_data["block"] = contract_data["creation_block"]
            if (
                rewrite
                or f"{reward_data['hypervisor_address']}_{reward_data['rewarder_address']}"
                not in already_processed
            ):
                # save to database
                yield reward_data


def create_rewards_static_thena(
    network: str,
    hypervisor_addresses: list,
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
):
    to_process_contract_addresses = {
        "0x3a1d0952809f4948d15ebce8d345962a282c4fcb".lower(): {
            "creation_block": 27114632,
            "type": rewarderType.THENA_voter_v3,
        }
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
            # add block creation data
            if creation_data := _get_contract_creation_block(
                network=network, contract_address=reward_data["rewarder_address"]
            ):
                reward_data["block"] = creation_data["block"]
            else:
                # modify block number manually -> block num. is later used to update rewards_status from
                reward_data["block"] = contract_data["creation_block"]
            if (
                rewrite
                or f"{reward_data['hypervisor_address']}_{reward_data['rewarder_address']}"
                not in already_processed
            ):
                yield reward_data


def create_rewards_static_merkl(
    chain: Chain,
    hypervisors: list[dict],
    already_processed: list,
    rewrite: bool = False,
    block: int = 0,
):
    if (
        distributor_creator_address := STATIC_REGISTRY_ADDRESSES.get(chain, {})
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
                # bc there is no token info in allDistributions, we need to get it from chain
                tokenHelper = build_erc20_helper(
                    chain=chain, address=distribution["token"].lower(), cached=True
                )
                token_symbol = tokenHelper.symbol
                token_decimals = tokenHelper.decimals

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
                        "rewardToken_symbol": token_symbol,
                        "rewardToken_decimals": token_decimals,
                        "rewards_perSecond": 0,  # TODO: remove this field from all static rewards
                        "total_hypervisorToken_qtty": 0,  # TODO: remove this field from all static rewards
                    }

                    # save later to database
                    if (
                        rewrite
                        or f"{reward_data['hypervisor_address']}_{reward_data['rewarder_address']}"
                        not in already_processed
                    ):
                        # add block creation data
                        if creation_data := _get_contract_creation_block(
                            network=chain.database_name,
                            contract_address=reward_data["rewarder_registry"],
                        ):
                            reward_data["block"] = creation_data["block"]

                        # save to database
                        yield reward_data


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
) -> list:
    """Create a list of hypervisor addresses to be scraped to feed static database collection

    Args:
        local_db (database_local): _description_
        rewrite (bool, optional): _description_. Defaults to False.

    Returns:
        list:
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
) -> list:
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
            f"   excluding hypervisors: {exclude_addresses}"
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
