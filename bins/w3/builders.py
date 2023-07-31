import logging

from web3 import Web3

from ..configuration import STATIC_REGISTRY_ADDRESSES

from ..w3.protocols.gamma.registry import gamma_hypervisor_registry
from ..general.enums import Chain, Protocol

from ..w3 import protocols
from ..w3.protocols.general import bep20, bep20_cached, erc20, erc20_cached


# build instances of classes


# temporary database comm conversion
def convert_dex_protocol(dex: str) -> Protocol:
    for protocol in Protocol:
        if protocol.database_name == dex:
            return protocol
    raise ValueError(f"{dex} is not a valid DEX name")


def convert_network_chain(network: str) -> Chain:
    for chain in Chain:
        if chain.database_name == network:
            return chain
    raise ValueError(f"{network} is not a valid network name")


def build_db_hypervisor(
    address: str,
    network: str,
    block: int,
    dex: str,
    static_mode=False,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
    cached: bool = True,
    force_rpcType: str | None = None,
) -> dict():
    try:
        hypervisor = build_hypervisor(
            network=network,
            protocol=convert_dex_protocol(dex=dex),
            block=block,
            hypervisor_address=address,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            cached=cached,
        )

        # set custom rpc type if needed
        if force_rpcType:
            hypervisor.custom_rpcType = force_rpcType

        hype_as_dict = hypervisor.as_dict(convert_bint=True, static_mode=static_mode)

        if network == "binance":
            # BEP20 is not ERC20-> TODO: change
            check_erc20_fields(
                hypervisor=hypervisor, hype=hype_as_dict, convert_bint=True
            )

        # return converted hypervisor
        return hype_as_dict

    except Exception as e:
        logging.getLogger(__name__).error(
            f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{e}"
        )

    return None


def check_erc20_fields(
    hypervisor: protocols.uniswap.hypervisor.gamma_hypervisor,
    hype: dict,
    convert_bint: bool = True,
    wrong_values: list | None = None,
) -> bool:
    """Check only the erc20 part correctness and repair

    Args:
        hypervisor (gamma_hypervisor): hype
        hype (dict): hyperivisor as a dict

    Returns:
        bool:  has been modified or not?
    """
    if not wrong_values:
        wrong_values = [None, "None", "none", "null"]
    # control var
    has_been_modified = False

    if hype["totalSupply"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no totalSupply. Will try again"
        )
        # get info from chain
        hype["totalSupply"] = (
            str(hypervisor.totalSupply) if convert_bint else int(hypervisor.totalSupply)
        )
        has_been_modified = True

    if hype["decimals"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no decimals. Will try again"
        )
        # get info from chain
        hype["decimals"] = int(hypervisor.decimals)
        has_been_modified = True

    if hype["symbol"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no symbol. Will try again"
        )
        # get info from chain
        hype["symbol"] = str(hypervisor.symbol)
        has_been_modified = True

    if hype["pool"]["token0"]["decimals"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token0 decimals. Will try again"
        )
        # get info from chain
        hype["pool"]["token0"]["decimals"] = int(hypervisor.pool.token0.decimals)
        has_been_modified = True

    if hype["pool"]["token1"]["decimals"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token1 decimals. Will try again"
        )
        # get info from chain
        hype["pool"]["token1"]["decimals"] = int(hypervisor.pool.token1.decimals)
        has_been_modified = True

    if hype["pool"]["token0"]["symbol"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token0 symbol. Will try again"
        )
        # get info from chain
        hype["pool"]["token0"]["symbol"] = str(hypervisor.pool.token0.symbol)
        has_been_modified = True

    if hype["pool"]["token1"]["symbol"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token1 symbol. Will try again"
        )
        # get info from chain
        hype["pool"]["token1"]["symbol"] = str(hypervisor.pool.token1.symbol)
        has_been_modified = True

    if hype["pool"]["token0"]["totalSupply"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token0 totalSupply. Will try again"
        )
        # get info from chain
        hype["pool"]["token0"]["totalSupply"] = (
            str(hypervisor.pool.token0.totalSupply)
            if convert_bint
            else int(hypervisor.pool.token0.totalSupply)
        )
        has_been_modified = True

    if hype["pool"]["token1"]["totalSupply"] in wrong_values:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token1 totalSupply. Will try again"
        )
        # get info from chain
        hype["pool"]["token1"]["totalSupply"] = (
            str(hypervisor.pool.token1.totalSupply)
            if convert_bint
            else int(hypervisor.pool.token1.totalSupply)
        )
        has_been_modified = True

    return has_been_modified


def build_hypervisor(
    network: str,
    protocol: Protocol,
    block: int,
    hypervisor_address: str,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
    cached: bool = False,
) -> protocols.uniswap.hypervisor.gamma_hypervisor:
    # choose type based on Protocol
    if protocol == Protocol.UNISWAPv3:
        hypervisor = (
            protocols.uniswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.uniswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.ZYBERSWAP:
        hypervisor = (
            protocols.zyberswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.zyberswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.QUICKSWAP:
        hypervisor = (
            protocols.quickswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.quickswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.THENA:
        hypervisor = (
            protocols.thena.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.thena.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.CAMELOT:
        hypervisor = (
            protocols.camelot.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.camelot.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.BEAMSWAP:
        hypervisor = (
            protocols.beamswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.beamswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.RETRO:
        hypervisor = (
            protocols.retro.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.retro.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.SUSHI:
        hypervisor = (
            protocols.sushiswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.sushiswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.STELLASWAP:
        hypervisor = (
            protocols.stellaswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.stellaswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.RAMSES:
        hypervisor = (
            protocols.ramses.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.ramses.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )

    elif protocol == Protocol.GAMMA:
        hypervisor = (
            protocols.gamma.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else protocols.gamma.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    else:
        raise NotImplementedError(f" {protocol} has not been implemented yet")

    return hypervisor


def build_hypervisor_registry(
    network: str,
    protocol: Protocol,
    block: int,
    custom_web3Url: str | None = None,
) -> gamma_hypervisor_registry:
    # get the list of registry addresses

    if registry_address := (
        STATIC_REGISTRY_ADDRESSES.get(network, {})
        .get("hypervisors", {})
        .get(protocol.database_name, None)
    ):
        # build hype
        registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network,
            block=block,
            custom_web3Url=custom_web3Url,
        )

        return registry


def build_protocol_pool(
    chain: Chain,
    protocol: Protocol,
    pool_address: str,
    block: int | None = None,
    cached: bool = False,
):
    # select the right protocol
    if protocol == Protocol.UNISWAPv3:
        # construct helper
        return (
            protocols.uniswap.pool.poolv3(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.uniswap.pool.poolv3_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.ALGEBRAv3:
        # construct helper
        return (
            protocols.algebra.pool.poolv3(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.algebra.pool.poolv3_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.PANCAKESWAP:
        return (
            protocols.pancakeswap.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.pancakeswap.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.BEAMSWAP:
        return (
            protocols.beamswap.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.beamswap.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.THENA:
        return (
            protocols.thena.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.thena.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.CAMELOT:
        return (
            protocols.camelot.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.camelot.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.RAMSES:
        return (
            protocols.ramses.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.ramses.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    else:
        raise NotImplementedError(f"Protocol {protocol} not implemented")


def build_erc20_helper(
    chain: Chain, address: str | None = None, cached: bool = False
) -> bep20 | erc20:
    """Create a bep20 or erc20 with the zero address

    Args:
        chain (Chain):
        cached (bool, optional): . Defaults to False.

    Returns:
        bep20 | erc20:
    """
    if cached:
        return (
            bep20_cached(
                address=address or "0x0000000000000000000000000000000000000000",
                network=chain.database_name,
            )
            if chain == Chain.BSC
            else erc20_cached(
                address=address or "0x0000000000000000000000000000000000000000",
                network=chain.database_name,
            )
        )

    return (
        bep20(
            address="0x0000000000000000000000000000000000000000",
            network=chain.database_name,
        )
        if chain == Chain.BSC
        else erc20(
            address="0x0000000000000000000000000000000000000000",
            network=chain.database_name,
        )
    )
