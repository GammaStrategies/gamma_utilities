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
) -> dict():
    try:
        hypervisor = build_hypervisor(
            network=network,
            dex=dex,
            block=block,
            hypervisor_address=address,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            cached=True,
        )

        # return converted hypervisor
        return hypervisor.as_dict(convert_bint=True, static_mode=static_mode)

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while converting {network}'s hypervisor {address} [dex: {dex}] at block {block}] to dictionary ->    error:{e}"
        )

    return None
