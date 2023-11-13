def VolumeThroughVaults(fees, feeTier, time) -> float:
    """Volume Through Vaults (VTV)
        The total volume facilitated by the vaults TVL per unit time

        VTV = [Fees * (1 / Fee Tier)] / Time

    Args:
        fees ():
        feeTier ():
        time ():

    Returns:
        float: Volume Through Vaults
    """
    try:
        return (fees * (1 / feeTier)) / time
    except ZeroDivisionError:
        return 0


def IncentivizedLiquidityRate(
    averageIncentiveTVL, averageBaselineTVL, incentivesPerPeriod
) -> float:
    """Incentivized Liquidity Rate (ILR)
        The amount of TVL increase per $ of incentives

        ILR = (Average incentive TVL - Average baseline TVL)  / (Incentives per period)

    Args:
        averageIncentiveTVL ():
        averageBaselineTVL ():
        incentivesPerPeriod ():

    Returns:
        float: Incentivized Liquidity Rate
    """
    try:
        return (averageIncentiveTVL - averageBaselineTVL) / incentivesPerPeriod
    except ZeroDivisionError:
        return 0


def CapitalEfficiency(volume, TVL) -> float:
    """Capital Efficiency (CE)
        The total volume compared to the TVL of a liquidity pool

        CE = Volume / TVL

    Args:
        volume ():
        TVL ():

    Returns:
        float: Capital Efficiency ratio %
    """
    try:
        return volume / TVL
    except ZeroDivisionError:
        return 0


def TimeInRange(timeInRange, operatingTime) -> float:
    """Time In Range (TIR)
        The percentage of time a vault was in range and producing fees/revenue.

        TIR = Sum of Time In Range / Total operating Time * 100%

    Args:
        timeInRange ():
        operatingTime ():

    Returns:
        float: Time In Range %
    """
    try:
        return timeInRange / operatingTime
    except ZeroDivisionError:
        return 0


def RebalanceFrequency(numberOfRebalances, time) -> float:
    """Rebalance Frequency (RF)
        The number of times a vault was rebalanced per unit time

        RF = Number of Rebalances / Time

    Args:
        numberOfRebalances ():
        time ():

    Returns:
        float: Rebalance Frequency
    """
    try:
        return numberOfRebalances / time
    except ZeroDivisionError:
        return 0


def HistoricalVolatility(standardDeviationOfAssetPrice, rootTime) -> float:
    """Historical Volatility (HV)
        The volatility of the LP token price

        HV = Standard Deviation of Asset Price / Root(Time)

    Args:
        standardDeviationOfAssetPrice ():
        rootTime ():

    Returns:
        float: Historical Volatility
    """
    try:
        return standardDeviationOfAssetPrice / rootTime
    except ZeroDivisionError:
        return 0


def VolumeGrowthRate(
    averageVolume, averageBaselineVolume, incentivesPerPeriod
) -> float:
    """Volume Growth Rate (VG)
        The amount of volume increase per $ of incentives

        VG = (Average incentive Volume - Average baseline Volume)  / (Incentives per period)

    Args:
        averageVolume ():
        averageBaselineVolume ():
        incentivesPerPeriod ():

    Returns:
        float: Volume Growth Rate
    """
    try:
        return (averageVolume - averageBaselineVolume) / incentivesPerPeriod
    except ZeroDivisionError:
        return 0


def TransactionGrowthRate(
    averageTxns, averageBaselineTxns, incentivesPerPeriod
) -> float:
    """Transaction Growth Rate (TG)
        The amount of transaction increase per $ of incentives

        TG = (Average incentive Txns - Average baseline Txns)  / (Incentives per period)

    Args:
        averageTxns ():
        averageBaselineTxns ():
        incentivesPerPeriod ():

    Returns:
        float: Transaction Growth Rate
    """
    try:
        return (averageTxns - averageBaselineTxns) / incentivesPerPeriod
    except ZeroDivisionError:
        return 0
