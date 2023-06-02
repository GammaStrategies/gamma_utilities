import random
import logging

from web3 import Web3
from bins.configuration import STATIC_REGISTRY_ADDRESSES
from bins.w3.onchain_utilities.basic import erc20
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_registry,
    gamma_hypervisor_thena,
    gamma_hypervisor_thena_cached,
    gamma_hypervisor_zyberswap,
    gamma_hypervisor_zyberswap_cached,
    gamma_hypervisor_camelot,
    gamma_hypervisor_camelot_cached,
)
from bins.w3.onchain_utilities import rewarders

# build instances of classes


def build_hypervisor(
    network: str,
    dex: str,
    block: int,
    hypervisor_address: str,
    custom_web3: Web3 | None = None,
    custom_web3Url: str | None = None,
    cached: bool = False,
) -> gamma_hypervisor:
    # choose type based on dex
    if dex == "uniswapv3":
        hypervisor = (
            gamma_hypervisor(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif dex == "zyberswap":
        hypervisor = (
            gamma_hypervisor_zyberswap(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_zyberswap_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif dex == "quickswap":
        hypervisor = (
            gamma_hypervisor_quickswap(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_quickswap_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif dex == "thena":
        hypervisor = (
            gamma_hypervisor_thena(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_thena_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif dex == "camelot":
        hypervisor = (
            gamma_hypervisor_camelot(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_camelot_cached(
                address=hypervisor_address,
                network=network,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    else:
        raise NotImplementedError(f" {dex} exchange has not been implemented yet")

    return hypervisor


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
            dex=dex,
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
            check_erc20_fields(hypervisor=hypervisor, hype=hype_as_dict)

        # return converted hypervisor
        return hype_as_dict

    except Exception as e:
        logging.getLogger(__name__).error(
            f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{e}"
        )

    return None


def check_erc20_fields(hypervisor: gamma_hypervisor, hype: dict) -> bool:
    """Check only the erc20 part correctness and repair

    Args:
        hypervisor (gamma_hypervisor): hype
        hype (dict): hyperivisor as a dict

    Returns:
        bool:  has been modified or not?
    """
    # control var
    has_been_modified = False

    if not hype["totalSupply"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no totalSupply. Will try again"
        )
        # get info from chain
        hype["totalSupply"] = int(hypervisor.totalSupply)
        has_been_modified = True

    if not hype["decimals"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no decimals. Will try again"
        )
        # get info from chain
        hype["decimals"] = int(hypervisor.decimals)
        has_been_modified = True

    if not hype["symbol"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no symbol. Will try again"
        )
        # get info from chain
        hype["symbol"] = str(hypervisor.symbol)
        has_been_modified = True

    if not hype["pool"]["token0"]["decimals"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token0 decimals. Will try again"
        )
        # get info from chain
        hype["pool"]["token0"]["decimals"] = int(hypervisor.pool.token0.decimals)
        has_been_modified = True

    if not hype["pool"]["token1"]["decimals"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token1 decimals. Will try again"
        )
        # get info from chain
        hype["pool"]["token1"]["decimals"] = int(hypervisor.pool.token1.decimals)
        has_been_modified = True

    if not hype["pool"]["token0"]["symbol"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token0 symbol. Will try again"
        )
        # get info from chain
        hype["pool"]["token0"]["symbol"] = str(hypervisor.pool.token0.symbol)
        has_been_modified = True

    if not hype["pool"]["token1"]["symbol"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token1 symbol. Will try again"
        )
        # get info from chain
        hype["pool"]["token1"]["symbol"] = str(hypervisor.pool.token1.symbol)
        has_been_modified = True

    if not hype["pool"]["token0"]["totalSupply"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token0 totalSupply. Will try again"
        )
        # get info from chain
        hype["pool"]["token0"]["totalSupply"] = int(hypervisor.pool.token0.totalSupply)
        has_been_modified = True

    if not hype["pool"]["token1"]["totalSupply"]:
        logging.getLogger(__name__).error(
            f" {hypervisor._network}'s hype {hypervisor.address} at block {hypervisor.block} has no token1 totalSupply. Will try again"
        )
        # get info from chain
        hype["pool"]["token1"]["totalSupply"] = int(hypervisor.pool.token1.totalSupply)
        has_been_modified = True

    return has_been_modified
