import contextlib
import sys
import os
import logging
from datetime import timezone
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pathlib import Path
import tqdm
import concurrent.futures

from datetime import datetime, timedelta

# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)


from bins.database.common.db_collections_common import database_global, database_local
from bins.mixed.price_utilities import price_scraper
from bins.configuration import CONFIGURATION, RPC_URLS, STATIC_REGISTRY_ADDRESSES
from bins.general import general_utilities, file_utilities

from bins.converters.onchain import convert_hypervisor_fromDict

from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_algebra,
    gamma_hypervisor_zyberswap,
    gamma_hypervisor_thena,
    gamma_hypervisor_registry,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor_algebra_cached,
    gamma_hypervisor_zyberswap_cached,
    gamma_hypervisor_thena_cached,
)
from bins.w3.onchain_utilities.rewarders import (
    gamma_masterchef_registry,
    gamma_masterchef_v1,
    gamma_masterchef_rewarder,
    zyberswap_masterchef_v1,
)


def build_hypervisor(
    network: str, dex: str, block: int, hypervisor_address: str
) -> gamma_hypervisor:
    # save current configuration
    for rpcUrl in RPC_URLS[network]:
        try:
            if dex == "zyberswap":
                hypervisor = gamma_hypervisor_zyberswap(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    custom_web3Url=rpcUrl,
                )
            elif dex == "quickswap":
                hypervisor = gamma_hypervisor_quickswap(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    custom_web3Url=rpcUrl,
                )
            elif dex == "thena":
                hypervisor = gamma_hypervisor_thena(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    custom_web3Url=rpcUrl,
                )
            else:
                # build hype
                hypervisor = gamma_hypervisor(
                    address=hypervisor_address,
                    network=network,
                    block=block,
                    custom_web3Url=rpcUrl,
                )
            # test its working
            hypervisor.fee
            # return hype
            return hypervisor
        except:
            pass

    return None


def get_rewards_zyberswap(network="arbitrum", dex="zyberswap", protocol="gamma"):
    for address in STATIC_REGISTRY_ADDRESSES[network]["zyberswap_v1_masterchefs"]:
        # create database connection

        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{network}_{protocol}"
        # local_db = database_local(mongo_url=mongo_url, db_name=db_name)
        global_db = database_global(mongo_url=mongo_url)
        price_helper = price_scraper(cache=False)

        # create zyberchef
        zyberchef = zyberswap_masterchef_v1(address=address, network=network)
        for pid in range(zyberchef.poolLength):
            #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
            pinfo = zyberchef.poolInfo(pid)
            # check lp token corresponds to gamma rewards: search address in database
            if hypervisor := build_hypervisor(
                network=network, dex=dex, block=0, hypervisor_address=pinfo[0]
            ):
                totalSupply = hypervisor.totalSupply / 10**hypervisor.decimals
                totalAmounts = hypervisor.getTotalAmounts
                totalAmounts["total0"] = (
                    totalAmounts["total0"] / 10**hypervisor.token0.decimals
                )
                totalAmounts["total1"] = (
                    totalAmounts["total1"] / 10**hypervisor.token1.decimals
                )
                price_token0 = price_helper.get_price(
                    network=network,
                    token_id=hypervisor.token0.address.lower(),
                )
                price_token1 = price_helper.get_price(
                    network=network,
                    token_id=hypervisor.token1.address.lower(),
                )
                share_price_usd = (
                    totalAmounts["total0"] * price_token0
                    + totalAmounts["total1"] * price_token1
                ) / totalSupply

                #
                # option 1)   rewardRate * secondsPerYear * price of token) / (totalSupply * price per LP Token)
                #
                # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
                poolRewardsPerSec = zyberchef.poolRewardsPerSec(pid)
                pooltotalLp = pinfo[6] / 10**18  # zyberchef.poolTotalLp(pid)
                secondsPerYear = 365 * 24 * 60 * 60

                # get price of poolRewardsPerSec["address"]
                rewards = {}
                for address, symbol, decimals, rewardsPerSec in zip(
                    poolRewardsPerSec[0],
                    poolRewardsPerSec[1],
                    poolRewardsPerSec[2],
                    poolRewardsPerSec[3],
                ):
                    if rewardsPerSec:
                        # get price of token
                        price = price_helper.get_price(
                            network=network,
                            token_id=address.lower(),
                        ) or global_db.get_price_usd(
                            network=network,
                            address=address.lower(),
                        )

                        # rewards[address] = {
                        #     "decimals": decimals,
                        #     "symbol": symbol,
                        #     "price_usd": price,
                        #     "rewardsPerSec": rewardsPerSec / 10**decimals,
                        # }

                        # rewardRate * secondsPerYear * price of token) / (totalSupply * price per LP Token)
                        APY = (
                            (rewardsPerSec / 10**decimals) * secondsPerYear * price
                        ) / (pooltotalLp * share_price_usd)
                        print(
                            f"   APY for {hypervisor.address} is {APY}  in {symbol} at price {price}"
                        )

            else:
                print(f"{pinfo[0]} is not a gamma hype")


def get_rewards_zyberswap(hypervisor_address: str, network: str):
    dex = "zyberswap"
    for address in STATIC_REGISTRY_ADDRESSES[network]["zyberswap_v1_masterchefs"]:
        # create database connection

        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        # db_name = f"{network}_gamma"
        # local_db = database_local(mongo_url=mongo_url, db_name=db_name)
        global_db = database_global(mongo_url=mongo_url)
        price_helper = price_scraper(cache=False)

        # create zyberchef
        zyberchef = zyberswap_masterchef_v1(address=address, network=network)
        for pid in range(zyberchef.poolLength):
            #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
            pinfo = zyberchef.poolInfo(pid)
            # check lp token corresponds to gamma rewards: search address in database
            if hypervisor := build_hypervisor(
                network=network, dex=dex, block=0, hypervisor_address=pinfo[0]
            ):
                totalSupply = hypervisor.totalSupply / 10**hypervisor.decimals
                totalAmounts = hypervisor.getTotalAmounts
                totalAmounts["total0"] = (
                    totalAmounts["total0"] / 10**hypervisor.token0.decimals
                )
                totalAmounts["total1"] = (
                    totalAmounts["total1"] / 10**hypervisor.token1.decimals
                )
                price_token0 = price_helper.get_price(
                    network=network,
                    token_id=hypervisor.token0.address.lower(),
                )
                price_token1 = price_helper.get_price(
                    network=network,
                    token_id=hypervisor.token1.address.lower(),
                )
                share_price_usd = (
                    totalAmounts["total0"] * price_token0
                    + totalAmounts["total1"] * price_token1
                ) / totalSupply

                #
                # option 1)   rewardRate * secondsPerYear * price of token) / (totalSupply * price per LP Token)
                #
                # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
                poolRewardsPerSec = zyberchef.poolRewardsPerSec(pid)
                pooltotalLp = pinfo[6] / 10**18  # zyberchef.poolTotalLp(pid)
                secondsPerYear = 365 * 24 * 60 * 60

                # get price of poolRewardsPerSec["address"]
                rewards = {}
                for address, symbol, decimals, rewardsPerSec in zip(
                    poolRewardsPerSec[0],
                    poolRewardsPerSec[1],
                    poolRewardsPerSec[2],
                    poolRewardsPerSec[3],
                ):
                    if rewardsPerSec:
                        # get price of token
                        price = price_helper.get_price(
                            network=network,
                            token_id=address.lower(),
                        ) or global_db.get_price_usd(
                            network=network,
                            address=address.lower(),
                        )

                        # rewardRate * secondsPerYear * price of token) / (totalSupply * price per LP Token)
                        APY = (
                            (rewardsPerSec / 10**decimals) * secondsPerYear * price
                        ) / (pooltotalLp * share_price_usd)

                        yield {
                            "network": network,
                            "block": zyberchef.block,
                            "hypervisor_address": hypervisor.address,
                            "dex": dex,
                            "rewardToken": address,
                            "rewardToken_symbol": symbol,
                            "rewardToken_priceUsd": price,
                            "rewardAPY": APY,
                            "raw_data": {
                                "totalSupply": totalSupply,
                                "totalAmounts": totalAmounts,
                                "price_token0": price_token0,
                                "price_token1": price_token1,
                                "share_price_usd": share_price_usd,
                                "poolRewardsPerSec": poolRewardsPerSec,
                                "pooltotalLp": pooltotalLp,
                                "secondsPerYear": secondsPerYear,
                            },
                        }


def get_rewards(hypervisor_address: str, network="arbitrum", dex="zyberswap"):
    if dex == "zyberswap":
        return get_rewards_zyberswap(
            hypervisor_address=hypervisor_address, network=network
        )


#####
def build_zyberswap_static_data(network="arbitrum", dex="zyberswap", protocol="gamma"):
    for address in STATIC_REGISTRY_ADDRESSES[network]["zyberswap_v1_masterchefs"]:
        # create zyberchef
        zyberchef = zyberswap_masterchef_v1(address=address, network=network)
        for pid in range(zyberchef.poolLength):
            #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
            pinfo = zyberchef.poolInfo(pid)
            # check lp token corresponds to gamma rewards: search address in database
            if hypervisor := build_hypervisor(
                network=network, dex=dex, block=0, hypervisor_address=pinfo[0]
            ):
                network = network
                dex = dex
                masterchef_address = address
                hypervisor_pid = pid
                hypervisor_address = hypervisor.address
                hypervisor_symbol = hypervisor.symbol


# START ################

if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    # get_rewards_zyberswap(network="arbitrum", dex="zyberswap", protocol="gamma")
    # 0xc2be9df80ce62e4258c27b1ffef741adc118b8b0  # WETH-USDC arbitrum zyberswap
    hype_rewards = list(
        get_rewards(
            hypervisor_address="0xc2be9df80ce62e4258c27b1ffef741adc118b8b0",
            network="arbitrum",
            dex="zyberswap",
        )
    )

    # end time log
    logging.getLogger(__name__).info(
        f" took {general_utilities.log_time_passed.get_timepassed_string(_startime)} to complete"
    )

    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
