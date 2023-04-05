from decimal import Decimal


def convert_hypervisor_fromDict(hypervisor: dict, toDecimal: bool = True) -> dict:
    """convert hypervisor to numbers.

    Args:
        hypervisor (dict):

    Returns:
        dict:
    """

    # decimals
    decimals_token0 = hypervisor["pool"]["token0"]["decimals"]
    decimals_token1 = hypervisor["pool"]["token1"]["decimals"]
    decimals_contract = hypervisor["decimals"]

    hypervisor["baseUpper"] = int(hypervisor["baseUpper"])
    hypervisor["baseLower"] = int(hypervisor["baseLower"])

    hypervisor["basePosition"]["liquidity"] = int(
        hypervisor["basePosition"]["liquidity"]
    )
    hypervisor["basePosition"]["amount0"] = int(hypervisor["basePosition"]["amount0"])
    hypervisor["basePosition"]["amount1"] = int(hypervisor["basePosition"]["amount1"])
    hypervisor["limitPosition"]["liquidity"] = int(
        hypervisor["limitPosition"]["liquidity"]
    )
    hypervisor["limitPosition"]["amount0"] = int(hypervisor["limitPosition"]["amount0"])
    hypervisor["limitPosition"]["amount1"] = int(hypervisor["limitPosition"]["amount1"])

    hypervisor["currentTick"] = int(hypervisor["currentTick"])

    if toDecimal:
        hypervisor["deposit0Max"] = Decimal(hypervisor["deposit0Max"]) / Decimal(
            10**decimals_token0
        )
        hypervisor["deposit1Max"] = Decimal(hypervisor["deposit1Max"]) / Decimal(
            10**decimals_token1
        )

        hypervisor["fees_uncollected"]["qtty_token0"] = Decimal(
            hypervisor["fees_uncollected"]["qtty_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["fees_uncollected"]["qtty_token1"] = Decimal(
            hypervisor["fees_uncollected"]["qtty_token1"]
        ) / Decimal(10**decimals_token1)

        hypervisor["maxTotalSupply"] = Decimal(hypervisor["maxTotalSupply"]) / Decimal(
            10**decimals_contract
        )

    else:
        hypervisor["deposit0Max"] = int(hypervisor["deposit0Max"])
        hypervisor["deposit1Max"] = int(hypervisor["deposit1Max"])

        hypervisor["fees_uncollected"]["qtty_token0"] = int(
            hypervisor["fees_uncollected"]["qtty_token0"]
        )
        hypervisor["fees_uncollected"]["qtty_token1"] = int(
            hypervisor["fees_uncollected"]["qtty_token1"]
        )

        hypervisor["maxTotalSupply"] = int(hypervisor["maxTotalSupply"])

    hypervisor["limitUpper"] = int(hypervisor["limitUpper"])
    hypervisor["limitLower"] = int(hypervisor["limitLower"])

    hypervisor["pool"]["feeGrowthGlobal0X128"] = int(
        hypervisor["pool"]["feeGrowthGlobal0X128"]
    )
    hypervisor["pool"]["feeGrowthGlobal1X128"] = int(
        hypervisor["pool"]["feeGrowthGlobal1X128"]
    )
    hypervisor["pool"]["liquidity"] = int(hypervisor["pool"]["liquidity"])
    hypervisor["pool"]["maxLiquidityPerTick"] = int(
        hypervisor["pool"]["maxLiquidityPerTick"]
    )

    # choose by dex
    if hypervisor["pool"]["dex"] == "uniswapv3":
        # uniswap
        hypervisor["pool"]["protocolFees"][0] = int(
            hypervisor["pool"]["protocolFees"][0]
        )
        hypervisor["pool"]["protocolFees"][1] = int(
            hypervisor["pool"]["protocolFees"][1]
        )

        hypervisor["pool"]["slot0"]["sqrtPriceX96"] = int(
            hypervisor["pool"]["slot0"]["sqrtPriceX96"]
        )
        hypervisor["pool"]["slot0"]["tick"] = int(hypervisor["pool"]["slot0"]["tick"])
        hypervisor["pool"]["slot0"]["observationIndex"] = int(
            hypervisor["pool"]["slot0"]["observationIndex"]
        )
        hypervisor["pool"]["slot0"]["observationCardinality"] = int(
            hypervisor["pool"]["slot0"]["observationCardinality"]
        )
        hypervisor["pool"]["slot0"]["observationCardinalityNext"] = int(
            hypervisor["pool"]["slot0"]["observationCardinalityNext"]
        )

        hypervisor["pool"]["tickSpacing"] = int(hypervisor["pool"]["tickSpacing"])
    elif hypervisor["pool"]["dex"] == "algebrav3":
        # quickswap
        hypervisor["pool"]["globalState"]["sqrtPriceX96"] = int(
            hypervisor["pool"]["globalState"]["sqrtPriceX96"]
        )
        hypervisor["pool"]["globalState"]["tick"] = int(
            hypervisor["pool"]["globalState"]["tick"]
        )
        hypervisor["pool"]["globalState"]["fee"] = int(
            hypervisor["pool"]["globalState"]["fee"]
        )
        hypervisor["pool"]["globalState"]["timepointIndex"] = int(
            hypervisor["pool"]["globalState"]["timepointIndex"]
        )
    else:
        raise NotImplementedError(f" dex {hypervisor['dex']} not implemented ")

    hypervisor["tickSpacing"] = int(hypervisor["tickSpacing"])

    if toDecimal:
        hypervisor["pool"]["token0"]["totalSupply"] = Decimal(
            hypervisor["pool"]["token0"]["totalSupply"]
        ) / Decimal(10**decimals_token0)
        hypervisor["pool"]["token1"]["totalSupply"] = Decimal(
            hypervisor["pool"]["token1"]["totalSupply"]
        ) / Decimal(10**decimals_token1)

        hypervisor["qtty_depoloyed"]["qtty_token0"] = Decimal(
            hypervisor["qtty_depoloyed"]["qtty_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["qtty_depoloyed"]["qtty_token1"] = Decimal(
            hypervisor["qtty_depoloyed"]["qtty_token1"]
        ) / Decimal(10**decimals_token1)
        hypervisor["qtty_depoloyed"]["fees_owed_token0"] = Decimal(
            hypervisor["qtty_depoloyed"]["fees_owed_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["qtty_depoloyed"]["fees_owed_token1"] = Decimal(
            hypervisor["qtty_depoloyed"]["fees_owed_token1"]
        ) / Decimal(10**decimals_token1)

        hypervisor["totalAmounts"]["total0"] = Decimal(
            hypervisor["totalAmounts"]["total0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["totalAmounts"]["total1"] = Decimal(
            hypervisor["totalAmounts"]["total1"]
        ) / Decimal(10**decimals_token1)

        hypervisor["totalSupply"] = Decimal(hypervisor["totalSupply"]) / Decimal(
            10**decimals_contract
        )

        hypervisor["tvl"]["parked_token0"] = Decimal(
            hypervisor["tvl"]["parked_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["tvl"]["parked_token1"] = Decimal(
            hypervisor["tvl"]["parked_token1"]
        ) / Decimal(10**decimals_token1)
        hypervisor["tvl"]["deployed_token0"] = Decimal(
            hypervisor["tvl"]["deployed_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["tvl"]["deployed_token1"] = Decimal(
            hypervisor["tvl"]["deployed_token1"]
        ) / Decimal(10**decimals_token1)
        hypervisor["tvl"]["fees_owed_token0"] = Decimal(
            hypervisor["tvl"]["fees_owed_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["tvl"]["fees_owed_token1"] = Decimal(
            hypervisor["tvl"]["fees_owed_token1"]
        ) / Decimal(10**decimals_token1)
        hypervisor["tvl"]["tvl_token0"] = Decimal(
            hypervisor["tvl"]["tvl_token0"]
        ) / Decimal(10**decimals_token0)
        hypervisor["tvl"]["tvl_token1"] = Decimal(
            hypervisor["tvl"]["tvl_token1"]
        ) / Decimal(10**decimals_token1)

    else:
        hypervisor["pool"]["token0"]["totalSupply"] = int(
            hypervisor["pool"]["token0"]["totalSupply"]
        )
        hypervisor["pool"]["token1"]["totalSupply"] = int(
            hypervisor["pool"]["token1"]["totalSupply"]
        )

        hypervisor["qtty_depoloyed"]["qtty_token0"] = int(
            hypervisor["qtty_depoloyed"]["qtty_token0"]
        )
        hypervisor["qtty_depoloyed"]["qtty_token1"] = int(
            hypervisor["qtty_depoloyed"]["qtty_token1"]
        )
        hypervisor["qtty_depoloyed"]["fees_owed_token0"] = int(
            hypervisor["qtty_depoloyed"]["fees_owed_token0"]
        )
        hypervisor["qtty_depoloyed"]["fees_owed_token1"] = int(
            hypervisor["qtty_depoloyed"]["fees_owed_token1"]
        )

        hypervisor["totalAmounts"]["total0"] = int(hypervisor["totalAmounts"]["total0"])
        hypervisor["totalAmounts"]["total1"] = int(hypervisor["totalAmounts"]["total1"])

        hypervisor["totalSupply"] = int(hypervisor["totalSupply"])

        hypervisor["tvl"]["parked_token0"] = int(hypervisor["tvl"]["parked_token0"])
        hypervisor["tvl"]["parked_token1"] = int(hypervisor["tvl"]["parked_token1"])
        hypervisor["tvl"]["deployed_token0"] = int(hypervisor["tvl"]["deployed_token0"])
        hypervisor["tvl"]["deployed_token1"] = int(hypervisor["tvl"]["deployed_token1"])
        hypervisor["tvl"]["fees_owed_token0"] = int(
            hypervisor["tvl"]["fees_owed_token0"]
        )
        hypervisor["tvl"]["fees_owed_token1"] = int(
            hypervisor["tvl"]["fees_owed_token1"]
        )
        hypervisor["tvl"]["tvl_token0"] = int(hypervisor["tvl"]["tvl_token0"])
        hypervisor["tvl"]["tvl_token1"] = int(hypervisor["tvl"]["tvl_token1"])

    return hypervisor
