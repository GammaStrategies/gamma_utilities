import logging

from web3 import Web3
from bins.checkers.hypervisor import check_hypervisor_is_valid
from bins.config.current import MULTICALL3_ADDRESSES
from bins.config.hardcodes import GAMMA_HYPERVISOR_ABIS

from bins.errors.general import CheckingError, ProcessingError

from ..configuration import STATIC_REGISTRY_ADDRESSES

from ..w3.protocols.gamma.registry import gamma_hypervisor_registry
from ..general.enums import Chain, Protocol, error_identity, text_to_chain

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


# TODO: accept static hypervisor:dict as a parameter to use it for some fields instead of querying the chain
def build_db_hypervisor(
    address: str,
    network: str,
    block: int,
    dex: str,
    static_mode=False,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
    cached: bool = False,
    force_rpcType: str | None = None,
    minimal: bool = False,
) -> dict():
    try:
        # build hypervisor
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

        hype_as_dict = hypervisor.as_dict(
            convert_bint=True, static_mode=static_mode, minimal=minimal
        )

        if network == "binance":
            # BEP20 is not ERC20-> TODO: change
            check_erc20_fields(
                hypervisor=hypervisor, hype=hype_as_dict, convert_bint=True
            )

        # check hypervisor validity
        check_hypervisor_is_valid(hypervisor=hype_as_dict)

        # return converted hypervisor
        return hype_as_dict

    except CheckingError as e:
        if e.identity == error_identity.RETURN_NONE and network == "binance":
            # try to recover
            if check_erc20_fields(
                hypervisor=hypervisor, hype=hype_as_dict, convert_bint=True
            ):
                # check if is valid, quietly
                if check_hypervisor_is_valid(hypervisor=hypervisor, quiet=True):
                    logging.getLogger(__name__).debug(
                        f"  {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary process recovered from 'None field' error "
                    )

        logging.getLogger(__name__).debug(
            f"  Unrecoverable error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary"
        )

    except ProcessingError as e:
        if e.identity == error_identity.NO_RPC_AVAILABLE:
            logging.getLogger(__name__).error(
                f" There are no RPCs available error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary"
            )
        else:
            logging.getLogger(__name__).error(
                f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{e.message}"
            )

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{e}"
        )

    return None


def build_db_hypervisor_multicall(
    address: str,
    network: str,
    block: int,
    dex: str,
    pool_address: str,
    token0_address: str,
    token1_address: str,
    static_mode=False,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
    force_rpcType: str | None = None,
    convert_bint: bool = True,
) -> dict():
    try:
        # check if multicall contract is created after block to avoid problems
        if MULTICALL3_ADDRESSES.get(text_to_chain(network), {}).get("block", 0) > block:
            logging.getLogger(__name__).debug(
                f" Returning non multicall db dict hypervisor bcause multicall contract creation block is gt {block}."
            )
            return build_db_hypervisor(
                address=address,
                network=network,
                block=block,
                dex=dex,
                static_mode=static_mode,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
                cached=True,
                force_rpcType=force_rpcType,
            )

        # build hypervisor
        hypervisor = build_hypervisor(
            network=network,
            protocol=convert_dex_protocol(dex=dex),
            block=block,
            hypervisor_address=address,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            cached=False,
            multicall=True,
        )

        # set custom rpc type if needed
        if force_rpcType:
            hypervisor.custom_rpcType = force_rpcType

        # fill with multicall
        hypervisor.fill_with_multicall(
            pool_address=pool_address,
            token0_address=token0_address,
            token1_address=token1_address,
        )

        hype_as_dict = hypervisor.as_dict(
            convert_bint=convert_bint, static_mode=static_mode, minimal=False
        )

        if network == "binance":
            # BEP20 is not ERC20-> TODO: change name
            check_erc20_fields(
                hypervisor=hypervisor, hype=hype_as_dict, convert_bint=convert_bint
            )

        # check hypervisor validity
        check_hypervisor_is_valid(hypervisor=hype_as_dict)

        # return converted hypervisor
        return hype_as_dict

    except CheckingError as e:
        if e.identity == error_identity.RETURN_NONE and network == "binance":
            # try to recover
            if check_erc20_fields(
                hypervisor=hypervisor, hype=hype_as_dict, convert_bint=True
            ):
                # check if is valid, quietly
                if check_hypervisor_is_valid(hypervisor=hypervisor, quiet=True):
                    logging.getLogger(__name__).debug(
                        f"  {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary process recovered from 'None field' error "
                    )

        logging.getLogger(__name__).debug(
            f"  Unrecoverable error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary"
        )

    except ProcessingError as e:
        if e.identity == error_identity.NO_RPC_AVAILABLE:
            logging.getLogger(__name__).error(
                f" There are no RPCs available error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary"
            )
        else:
            logging.getLogger(__name__).error(
                f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{e.message}"
            )

    except Exception as e:
        logging.getLogger(__name__).exception(
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
    check: bool = False,
    multicall: bool = False,
) -> protocols.uniswap.hypervisor.gamma_hypervisor:
    """Create a hypervisor

    Args:
        network (str): _description_
        protocol (Protocol): _description_
        block (int): _description_
        hypervisor_address (str): _description_
        custom_web3 (Web3 | None, optional): _description_. Defaults to None.
        custom_web3Url (str | None, optional): _description_. Defaults to None.
        cached (bool, optional): _description_. Defaults to False.
        check (bool, optional): Check wether this is actually a hypervisor. Defaults to False.

    Raises:
        NotImplementedError: _description_
        ValueError: _description_

    Returns:
        protocols.uniswap.hypervisor.gamma_hypervisor: _description_
    """
    _chain = text_to_chain(network)

    # set hardcoded abis, if any
    abi_filename = (
        GAMMA_HYPERVISOR_ABIS.get(_chain, {})
        .get(hypervisor_address, {})
        .get("file", "")
    )
    abi_path = (
        GAMMA_HYPERVISOR_ABIS.get(_chain, {})
        .get(hypervisor_address, {})
        .get("folder", "")
    )

    # choose type based on Protocol
    if protocol == Protocol.UNISWAPv3:
        hypervisor = (
            protocols.uniswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.uniswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.uniswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.zyberswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.zyberswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.quickswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.quickswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.thena.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.thena.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.camelot.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.camelot.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.beamswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.beamswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.retro.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.retro.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.sushiswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.sushiswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.stellaswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.stellaswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.ramses.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.ramses.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.SYNTHSWAP:
        hypervisor = (
            protocols.synthswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.synthswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.synthswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.SPIRITSWAP:
        hypervisor = (
            protocols.spiritswap.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.spiritswap.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.spiritswap.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.GLACIER:
        hypervisor = (
            protocols.glacier.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.glacier.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.glacier.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.FUSIONX:
        hypervisor = (
            protocols.fusionx.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.fusionx.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.fusionx.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.LYNEX:
        hypervisor = (
            protocols.lynex.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.lynex.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.lynex.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.ASCENT:
        hypervisor = (
            protocols.ascent.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.ascent.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.ascent.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif protocol == Protocol.PANCAKESWAP:
        if _chain == Chain.BSC:
            hypervisor = (
                protocols.pancakeswap.hypervisor.gamma_hypervisor_bep20(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    abi_filename=abi_filename,
                    abi_path=abi_path,
                    custom_web3=custom_web3,
                    custom_web3Url=custom_web3Url,
                )
                if not cached and not multicall
                else protocols.pancakeswap.hypervisor.gamma_hypervisor_bep20_cached(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    abi_filename=abi_filename,
                    abi_path=abi_path,
                    custom_web3=custom_web3,
                    custom_web3Url=custom_web3Url,
                )
                if not multicall
                else protocols.pancakeswap.hypervisor.gamma_hypervisor_bep20_multicall(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    abi_filename=abi_filename,
                    abi_path=abi_path,
                    custom_web3=custom_web3,
                    custom_web3Url=custom_web3Url,
                )
            )
        else:
            hypervisor = (
                protocols.pancakeswap.hypervisor.gamma_hypervisor(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    abi_filename=abi_filename,
                    abi_path=abi_path,
                    custom_web3=custom_web3,
                    custom_web3Url=custom_web3Url,
                )
                if not cached and not multicall
                else protocols.pancakeswap.hypervisor.gamma_hypervisor_cached(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    abi_filename=abi_filename,
                    abi_path=abi_path,
                    custom_web3=custom_web3,
                    custom_web3Url=custom_web3Url,
                )
                if not multicall
                else protocols.pancakeswap.hypervisor.gamma_hypervisor_multicall(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    abi_filename=abi_filename,
                    abi_path=abi_path,
                    custom_web3=custom_web3,
                    custom_web3Url=custom_web3Url,
                )
            )
    elif protocol == Protocol.BASEX:
        hypervisor = (
            protocols.basex.hypervisor.gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.basex.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.basex.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached and not multicall
            else protocols.gamma.hypervisor.gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not multicall
            else protocols.gamma.hypervisor.gamma_hypervisor_multicall(
                address=hypervisor_address,
                network=network,
                block=block,
                abi_filename=abi_filename,
                abi_path=abi_path,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    else:
        raise NotImplementedError(f" {protocol} has not been implemented yet")

    # check if the hypervisor is actually a hypervisor
    if check:
        if not check_hypervisor(hypervisor=hypervisor):
            raise ValueError(
                f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} is not a hypervisor"
            )

    return hypervisor


def check_hypervisor(hypervisor: protocols.uniswap.hypervisor.gamma_hypervisor) -> bool:
    """Check if the hypervisor is actually a hypervisor. A false statement can be thrown if the choosen block is before creation (or after deletion, if that can happen)

    Args:
        hypervisor (protocols.uniswap.hypervisor.gamma_hypervisor): hypervisor to check

    Returns:
        bool: is a hypervisor?
    """
    try:
        if hypervisor.getTotalAmounts:
            return True
    except Exception as e:
        pass
    return False


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
    else:
        raise NotImplementedError(
            f" Registry for {network} {protocol.database_name} not implemented"
        )


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
        if chain == Chain.BSC:
            return (
                protocols.pancakeswap.pool.pool_bep20(
                    address=pool_address, network=chain.database_name, block=block
                )
                if not cached
                else protocols.pancakeswap.pool.pool_bep20_cached(
                    address=pool_address, network=chain.database_name, block=block
                )
            )
        else:
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
    # elif protocol == Protocol.SYNTHSWAP:
    #     return (
    #         protocols.algebra.pool.poolv3(
    #             address=pool_address, network=chain.database_name, block=block
    #         )
    #         if not cached
    #         else protocols.algebra.pool.poolv3_cached(
    #             address=pool_address, network=chain.database_name, block=block
    #         )
    #     )
    # elif protocol == Protocol.GLACIER:
    #     return (
    #         protocols.algebra.pool.poolv3(
    #             address=pool_address, network=chain.database_name, block=block
    #         )
    #         if not cached
    #         else protocols.algebra.pool.poolv3_cached(
    #             address=pool_address, network=chain.database_name, block=block
    #         )
    #     )
    elif protocol == Protocol.SPIRITSWAP:
        return (
            protocols.spiritswap.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.spiritswap.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    # elif protocol == Protocol.LYNEX:
    #     return (
    #         protocols.lynex.pool.pool(
    #             address=pool_address, network=chain.database_name, block=block
    #         )
    #         if not cached
    #         else protocols.lynex.pool.pool_cached(
    #             address=pool_address, network=chain.database_name, block=block
    #         )
    #     )
    elif protocol == Protocol.FUSIONX:
        return (
            protocols.fusionx.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.fusionx.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    elif protocol == Protocol.ASCENT:
        return (
            protocols.ascent.pool.pool(
                address=pool_address, network=chain.database_name, block=block
            )
            if not cached
            else protocols.ascent.pool.pool_cached(
                address=pool_address, network=chain.database_name, block=block
            )
        )
    else:
        raise NotImplementedError(f"Protocol {protocol} not implemented")


def build_erc20_helper(
    chain: Chain,
    address: str | None = None,
    cached: bool = False,
    abi_filename: str = "",
    abi_path: str = "",
    block: int = 0,
    timestamp: int = 0,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
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
                abi_filename=abi_filename,
                abi_path=abi_path,
                block=block,
                timestamp=timestamp,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if chain == Chain.BSC
            else erc20_cached(
                address=address or "0x0000000000000000000000000000000000000000",
                network=chain.database_name,
                abi_filename=abi_filename,
                abi_path=abi_path,
                block=block,
                timestamp=timestamp,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )

    return (
        bep20(
            address=address or "0x0000000000000000000000000000000000000000",
            network=chain.database_name,
            abi_filename=abi_filename,
            abi_path=abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )
        if chain == Chain.BSC
        else erc20(
            address=address or "0x0000000000000000000000000000000000000000",
            network=chain.database_name,
            abi_filename=abi_filename,
            abi_path=abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )
    )


def get_latest_block(chain: Chain) -> int:
    helper = build_erc20_helper(chain=chain)
    return helper.block
