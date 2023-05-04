from attr import dataclass

from bins.configuration import RPC_URLS, STATIC_REGISTRY_ADDRESSES
from bins.formulas.apr import calculate_rewards_apr
from bins.mixed.price_utilities import price_scraper
from bins.w3.builders import (
    build_erc20_anyRpc,
    build_thena_gauge_anyRpc,
    build_thena_voter_anyRpc,
    build_zyberchef_anyRpc,
)
from bins.w3.onchain_utilities.rewarders import zyberswap_masterchef_v1


@dataclass
class reward_data:
    network: str
    block: int
    timestamp: int
    hypervisor_address: str
    rewarder_address: str
    rewarder_type: str
    rewarder_refIds: list[str]
    rewardToken: str
    rewardToken_symbol: str
    rewardToken_decimals: int
    rewards_perSecond: int
    total_hypervisorToken_qtty: int


def search_rewards_data_zyberswap(hypervisor_address: str, network: str):
    result = []

    # get the list of registry addresses
    for address in STATIC_REGISTRY_ADDRESSES[network]["zyberswap_v1_masterchefs"]:
        # create database connection

        # create zyberchef
        zyberchef = build_zyberchef_anyRpc(
            address=address,
            network=network,
            block=0,
            rpcUrls=RPC_URLS[network],
            test=True,
        )

        # get the pool length
        poolLength = zyberchef.poolLength
        for pid in range(poolLength):
            result += get_rewards_data_zyberswap(
                hypervisor_address=hypervisor_address,
                network=network,
                pid=pid,
                zyberchef=zyberchef,
            )

    return result


def get_rewards_data_zyberswap(
    hypervisor_address: str,
    network: str,
    pid: int,
    zyberchef_address: str | None = None,
    zyberchef: zyberswap_masterchef_v1 | None = None,
) -> list[dict]:
    result = []

    # create zyberchef
    if not zyberchef and zyberchef_address:
        zyberchef = zyberswap_masterchef_v1(address=zyberchef_address, network=network)
    elif not zyberchef and not zyberchef_address:
        raise Exception("zyberchef_address or zyberchef must be provided")

    #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
    pinfo = zyberchef.poolInfo(pid)

    if pinfo[0].lower() == hypervisor_address.lower():
        # this is the pid we are looking for

        # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
        poolRewardsPerSec = zyberchef.poolRewardsPerSec(pid)
        # poolTotalLp = pinfo[6] / 10**18  # zyberchef.poolTotalLp(pid) # check

        # get rewards data
        rewards = {}
        for address, symbol, decimals, rewardsPerSec in zip(
            poolRewardsPerSec[0],
            poolRewardsPerSec[1],
            poolRewardsPerSec[2],
            poolRewardsPerSec[3],
        ):
            if rewardsPerSec:
                result.append(
                    {
                        "network": network.value,
                        "block": zyberchef.block,
                        "timestamp": zyberchef.timestamp,
                        "hypervisor_address": hypervisor_address,
                        "rewardToken": address,
                        "rewardToken_symbol": symbol,
                        "rewardToken_decimals": decimals,
                        "poolRewardsPerSec": rewardsPerSec,
                        "poolTotalLp": pinfo[6],
                    }
                )

    return result


def get_rewards(dex: str, hypervisor_address: str, network: str):
    result = []

    # price helper
    price_helper = price_scraper(cache=False)

    # retrieve hypervisor related data from database
    hypervisor_data = hypervisors.get_hypervisor_data(
        network=network,
        dex=dex,
        hypervisor_address=hypervisor_address,
        convert_to_decimal=True,
    )
    # add prices hype

    hypervisor_data["token0_price_usd"] = price_helper.get_price(
        token_address=hypervisor_data["token0_address"].lower(),
        network=network,
        block=0,
    )
    hypervisor_data["token1_price_usd"] = price_helper.get_price(
        token_address=hypervisor_data["token1_address"].lower(),
        network=network,
        block=0,
    )

    # add share price
    hypervisor_data["lpToken_price_usd"] = (
        (
            (
                hypervisor_data["token0_price_usd"]
                * (int(hypervisor_data["totalAmounts"]["total0"]))
                + hypervisor_data["token1_price_usd"]
                * (int(hypervisor_data["totalAmounts"]["total1"]))
            )
            / hypervisor_data["totalSupply"]
        )
        if hypervisor_data["totalSupply"]
        else 0
    )

    # choose the right reward data
    if dex == "zyberswap":
        # get rewards data
        rewards_data = search_rewards_data_zyberswap(
            hypervisor_address=hypervisor_address, network=network
        )

        for rewards in rewards_data:
            # calculate rewards APR
            apr = calculate_rewards_apr(
                token_price=price_helper.get_price(
                    token_address=rewards["rewardToken"].lower(),
                    network=network,
                    block=0,
                ),
                token_decimals=rewards["rewardToken_decimals"],
                token_reward_rate=rewards["poolRewardsPerSec"],
                total_lp_locked=(
                    rewards["poolTotalLp"] / (10 ** hypervisor_data["decimals"])
                ),
                lp_token_price=hypervisor_data["lpToken_price_usd"],
            )
            result.append(
                {
                    "symbol": hypervisor_data["symbol"],
                    **rewards,
                    "apr": apr,
                    "token0_price_usd": hypervisor_data["token0_price_usd"],
                    "token1_price_usd": hypervisor_data["token1_price_usd"],
                    "lpToken_price_usd": hypervisor_data["lpToken_price_usd"],
                }
            )

    elif dex == "thena":
        rewards_data = get_rewards_data_thena(
            hypervisor_address=hypervisor_address, network=network
        )

        # calculate rewards APR
        apr = calculate_rewards_apr(
            token_price=price_helper.get_price(
                token_address=rewards_data["rewardToken"].lower(),
                network=network,
                block=0,
            ),
            token_decimals=18,  # TODO: create erc20 instance and reat token decimals from there
            token_reward_rate=rewards_data["poolRewardsPerSec"],
            total_lp_locked=(
                rewards_data["poolTotalLp"] / (10 ** hypervisor_data["decimals"])
            ),
            lp_token_price=hypervisor_data["lpToken_price_usd"],
        )
        result.append(
            {
                "symbol": hypervisor_data["symbol"],
                **rewards_data,
                "apr": apr,
                "token0_price_usd": hypervisor_data["token0_price_usd"],
                "token1_price_usd": hypervisor_data["token1_price_usd"],
                "lpToken_price_usd": hypervisor_data["lpToken_price_usd"],
            }
        )

    return result


def get_rewards_data_thena(hypervisor_address: str, network: str):
    # build thena voter
    thena_voter = build_thena_voter_anyRpc(
        network=network, block=0, rpcUrls=RPC_URLS[network], test=True
    )

    # get managing gauge from hype address
    gauge_address = thena_voter.gauges(address=hypervisor_address)

    # build thena gauge instance
    thena_gauge = build_thena_gauge_anyRpc(
        address=gauge_address,
        network=network,
        block=0,
        rpcUrls=RPC_URLS[network],
        test=True,
    )
    # get gauge data
    rewardRate = thena_gauge.rewardRate
    rewardToken = thena_gauge.rewardToken
    totalSupply = thena_gauge.totalSupply
    block = thena_gauge.block

    # build reward token instance
    reward_token_instance = build_erc20_anyRpc(
        address=rewardToken,
        network=network,
        block=0,
        rpcUrls=RPC_URLS[network],
        test=True,
    )
    # get reward token data
    rewardToken_symbol = reward_token_instance.symbol
    rewardToken_decimals = reward_token_instance.decimals

    # return data
    return {
        "network": network,
        "block": block,
        "timestamp": thena_gauge.timestamp,
        "hypervisor_address": hypervisor_address,
        "rewardToken": rewardToken,
        "rewardToken_symbol": rewardToken_symbol,
        "rewardToken_decimals": rewardToken_decimals,
        "poolRewardsPerSec": rewardRate,
        "poolTotalLp": totalSupply,
    }
