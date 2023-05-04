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
        STATIC_REGISTRY_ADDRESSES.get(network, {}).get("hypervisors", {}).get(dex)
    ):
        # build hype
        registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network,
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
            logging.getLogger(__name__).debug(
                f" error creating hype: {e} -> using rpc: {rpcUrl}"
            )
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
        except Exception as e:
            # not working
            logging.getLogger(__name__).debug(
                f" error creating hype registry: {e} -> using rpc: {rpcUrl}"
            )

    return registry


def build_zyberchef_anyRpc(
    address: str, network: str, block: int, rpcUrls: list[str], test: bool = False
) -> rewarders.zyberswap_masterchef_v1:
    """return a hype registry that uses any of the supplyed RPC urls

    Args:
        network (str):
        block (int):
        test: (bool): if true, test the hype before returning it

    """
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls
    result = None
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            result = rewarders.zyberswap_masterchef_v1(
                address=address,
                network=network,
                block=block,
                custom_web3Url=rpcUrl,
            )
            if test:
                # test its working
                result.poolLength
            # return hype
            break
        except Exception as e:
            # not working
            logging.getLogger(__name__).debug(
                f" error creating zyberswap masterchef: {e} -> using rpc: {rpcUrl}"
            )

    return result


def build_thena_voter_anyRpc(
    network: str, block: int, rpcUrls: list[str], test: bool = False
) -> rewarders.thena_voter_v3:
    result = None
    if voter_url := STATIC_REGISTRY_ADDRESSES.get(network, {}).get("thena_voter", None):
        # shuffle the rpc urls
        random.shuffle(rpcUrls)
        # loop over the rpc urls

        for rpcUrl in rpcUrls:
            try:
                # construct hype
                result = rewarders.thena_voter_v3(
                    address=voter_url,
                    network=network,
                    block=block,
                    custom_web3Url=rpcUrl,
                )
                if test:
                    # test its working
                    result.factoryLength
                # return hype
                break
            except Exception as e:
                # not working hype
                logging.getLogger(__name__).debug(
                    f" error creating thena voter: {e} -> using rpc: {rpcUrl}"
                )
                pass

    return result


def build_thena_gauge_anyRpc(
    address: str, network: str, block: int, rpcUrls: list[str], test: bool = False
) -> rewarders.thena_gauge_v2:
    result = None
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls

    for rpcUrl in rpcUrls:
        try:
            # construct hype
            result = rewarders.thena_gauge_v2(
                address=address,
                network=network,
                block=block,
                custom_web3Url=rpcUrl,
            )
            if test:
                # test its working
                result.lastUpdateTime
            # return hype
            break
        except Exception as e:
            # not working
            logging.getLogger(__name__).debug(
                f" error creating thena gauge: {e} -> using rpc: {rpcUrl}"
            )
            pass

    return result


def build_erc20_anyRpc(
    address: str, network: str, block: int, rpcUrls: list[str], test: bool = False
) -> erc20:
    result = None

    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls

    for rpcUrl in rpcUrls:
        try:
            # construct hype
            result = erc20(
                address=address,
                network=network,
                block=block,
                custom_web3Url=rpcUrl,
            )
            if test:
                # test its working
                result.decimals
            # return hype
            break
        except Exception as e:
            # not working hype
            # TODO: log this
            logging.getLogger(__name__).debug(
                f" error creating erc20: {e} -> using rpc: {rpcUrl}"
            )
            pass

    return result
