import random

from web3 import Web3
from bins.configuration import STATIC_REGISTRY_ADDRESSES
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
)


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
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_cached(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    if dex == "zyberswap":
        hypervisor = (
            gamma_hypervisor_zyberswap(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_zyberswap_cached(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif dex == "quickswap":
        hypervisor = (
            gamma_hypervisor_quickswap(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_quickswap_cached(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    elif dex == "thena":
        hypervisor = (
            gamma_hypervisor_thena(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
            if not cached
            else gamma_hypervisor_thena_cached(
                address=hypervisor_address,
                network=network.value,
                block=block,
                custom_web3=custom_web3,
                custom_web3Url=custom_web3Url,
            )
        )
    else:
        raise NotImplementedError(f" {dex} exchange has not been implemented yet")

    return hypervisor


def build_hypervisor_registry(
    network: str,
    dex: str,
    block: int,
    custom_web3Url: str | None = None,
) -> gamma_hypervisor_registry:
    # get the list of registry addresses

    if registry_address := (
        STATIC_REGISTRY_ADDRESSES.get(network.value, {})
        .get("hypervisors", {})
        .get(dex.value)
    ):
        # build hype
        registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network.value,
            block=block,
            custom_web3Url=custom_web3Url,
        )

        return registry


def build_hypervisor_anyRpc(
    network: str,
    dex: str,
    block: int,
    hypervisor_address: str,
    rpcUrls: list[str],
    test: bool = False,
    cached: bool = False,
) -> gamma_hypervisor:
    """return a tested hype that uses any of the supplyed RPC urls

    Args:
        network (str):
        dex (str):
        block (int):
        hypervisor_address (str):
        rpcUrls (list[str]): list of RPC urls to be used
        test: (bool): if true, test the hype before returning it

    Returns:
        gamma_hypervisor:
    """
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls
    hypervisor = None
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            hypervisor = build_hypervisor(
                network=network,
                dex=dex,
                block=block,
                hypervisor_address=hypervisor_address,
                custom_web3Url=rpcUrl,
                cached=cached,
            )
            if test:
                # working test
                hypervisor._contract.functions.fee().call()  # test fee without block
            # return hype
            break
        except Exception as e:
            # not working hype
            print(f" error creating hype: {e} -> rpc: {rpcUrl}")
    # return hype
    return hypervisor


def build_hypervisor_registry_anyRpc(
    network: str, dex: str, block: int, rpcUrls: list[str], test: bool = False
) -> gamma_hypervisor_registry:
    """return a hype registry that uses any of the supplyed RPC urls

    Args:
        network (str):
        dex (str):
        block (int):
        test: (bool): if true, test the hype before returning it

    Returns:
        gamma hype registry:
    """
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls
    registry = None
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            registry = build_hypervisor_registry(
                network=network,
                dex=dex,
                block=block,
                custom_web3Url=rpcUrl,
            )
            if test:
                # test its working
                registry._contract.functions.counter().call()
            # return hype
            break
        except:
            # not working hype
            pass

    return registry
