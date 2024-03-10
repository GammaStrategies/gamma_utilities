import logging
from apps.feeds.operations import feed_operations
from bins.configuration import CONFIGURATION
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, text_to_chain


def repair_operations():
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(f"  Repairing {network} operations ")
            repair_operations_chain(
                chain=text_to_chain(network),
                block_ini=CONFIGURATION["_custom_"]["cml_parameters"].ini_block,
            )


def repair_operations_chain(
    chain: Chain,
    dex: Protocol | None = None,
    block_ini: int = None,
    use_last_block: bool = False,
    blocks_back: float = 0,
):
    """scrape operations from the specified blocks and save them to database

    Args:
        chain (Chain):
        dex (Protocol | None, optional): . Defaults to None.
        block_ini (int, optional): Force an initial block . Defaults to the first static block found in database.
        use_last_block: block_ini will beguin from the last block found in database. Defaults to False.
        blocks_back (float, optional): percentage of blocks to substract to the last block found in database. Defaults to 0.
    """

    batch_size = 100000

    ########## CONFIG #############
    protocol = "gamma"
    network = chain.database_name
    dex = dex.database_name if dex else None  # can be None
    force_back_time = True
    ###############################

    find = {}
    # filter by dex
    if dex:
        find["dex"] = dex

    # get static hypervisor blocks ( creation)
    hypervisor_list = get_from_localdb(
        network=network,
        collection="static",
        find=find,
        projection={"block": 1},
        batch_size=batch_size,
    )
    # set initial block
    if use_last_block:
        # get the max block from static hypervisor info
        block_ini = block_ini or max([h["block"] for h in hypervisor_list])
    else:
        # get the min block from static hypervisor info
        block_ini = block_ini or min([h["block"] for h in hypervisor_list])

    # remove blocks back when specified
    block_ini = block_ini - int(block_ini * blocks_back)

    # feed operations
    feed_operations(
        protocol=protocol,
        network=network,
        block_ini=block_ini,
        force_back_time=force_back_time,
    )
