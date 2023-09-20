from web3 import Web3
from bins.database.common.database_ids import create_id_hypervisor_status
from .general import token_group_object
from ....w3.protocols.gamma.hypervisor import gamma_hypervisor
from .hypervisor import (
    fee_growth_object,
    fees_object,
    hypervisor_database_object,
    pool_database_object,
    position_object,
    time_object,
    token_object,
)
from ....general.enums import Chain, Protocol


def convert_hypervisor_status_from_Olddb_to_object(
    hypervisor_db: dict, chain: Chain
) -> hypervisor_database_object:
    """Convert hypervisor status from old db to object"""

    # create positions
    position_base = position_object(
        name="base",
        liquidity=hypervisor_db["basePosition"]["liquidity"],
        qtty=token_group_object(
            token0=hypervisor_db["basePosition"]["amount0"],
            token1=hypervisor_db["basePosition"]["amount1"],
        ),
        lowerTick=hypervisor_db["baseLower"],
        upperTick=hypervisor_db["baseUpper"],
        fees=fees_object(
            collected=token_group_object(
                token0=0,
                token1=0,
            ),
            uncollected=token_group_object(
                token0=hypervisor_db["fees_uncollected"]["qtty_token0"],
                token1=hypervisor_db["fees_uncollected"]["qtty_token1"],
            ),
        ),
    )
    position_limit = position_object(
        name="limit",
        liquidity=hypervisor_db["limitPosition"]["liquidity"],
        qtty=token_group_object(
            token0=hypervisor_db["limitPosition"]["amount0"],
            token1=hypervisor_db["limitPosition"]["amount1"],
        ),
        lowerTick=hypervisor_db["limitLower"],
        upperTick=hypervisor_db["limitUpper"],
        fees=fees_object(
            collected=token_group_object(
                token0=0,
                token1=0,
            ),
            uncollected=token_group_object(
                token0=hypervisor_db["fees_uncollected"]["qtty_token0"],
                token1=hypervisor_db["fees_uncollected"]["qtty_token1"],
            ),
        ),
    )
    positions = [position_base, position_limit]

    # create pool tokens
    pool_token0 = token_object(
        address=hypervisor_db["pool"]["token0"]["address"],
        decimals=hypervisor_db["pool"]["token0"]["decimals"],
        symbol=hypervisor_db["pool"]["token0"]["symbol"],
        totalSupply=hypervisor_db["pool"]["token0"]["totalSupply"],
    )
    pool_token1 = token_object(
        address=hypervisor_db["pool"]["token1"]["address"],
        decimals=hypervisor_db["pool"]["token1"]["decimals"],
        symbol=hypervisor_db["pool"]["token1"]["symbol"],
        totalSupply=hypervisor_db["pool"]["token1"]["totalSupply"],
    )
    tokens = [pool_token0, pool_token1]

    # return hypervisor object
    return hypervisor_database_object(
        id=hypervisor_db["id"],
        chain=chain,
        protocol=Protocol(hypervisor_db["dex"]),
        token_info=token_object(
            address=hypervisor_db["address"],
            decimals=hypervisor_db["decimals"],
            symbol=hypervisor_db["symbol"],
            totalSupply=hypervisor_db["totalSupply"],
        ),
        time=time_object(
            block=hypervisor_db["block"],
            timestamp=hypervisor_db["timestamp"],
        ),
        positions=positions,
        fee=hypervisor_db["fee"],
        pool=pool_database_object(
            address=hypervisor_db["pool"]["address"],
            fee=hypervisor_db["pool"]["fee"],
            chain=chain,
            protocol=Protocol(hypervisor_db["pool"]["dex"]),
            time=time_object(
                block=hypervisor_db["pool"]["block"],
                timestamp=hypervisor_db["pool"]["timestamp"],
            ),
            tickSpacing=hypervisor_db["pool"]["tickSpacing"],
            tokens=tokens,
            protocolFees=hypervisor_db["pool"]["protocolFees"],
            feeGrowthGlobal0X128=hypervisor_db["pool"]["feeGrowthGlobal0X128"],
            feeGrowthGlobal1X128=hypervisor_db["pool"]["feeGrowthGlobal1X128"],
            liquidity=hypervisor_db["pool"]["liquidity"],
            maxLiquidityPerTick=hypervisor_db["pool"]["maxLiquidityPerTick"],
            slot0=hypervisor_db["pool"]["slot0"],
        ),
    )


### CONVERT W3 ##############################################################################################################


class converter_hypervisor_status_from_w3_to_object:
    @staticmethod
    def convert_positions(
        w3_hypervisor: gamma_hypervisor,
    ) -> list[position_object]:
        """Convert positions from the w3 object to the database object

        Args:
            w3_hypervisor (gamma_hypervisor): w3 hypervisor object

        Returns:
            list[position_object]: list of position objects
        """

        # get current tick
        tickCurrent = w3_hypervisor.currentTick

        # get base position variables
        _base_position_amounts = w3_hypervisor.getBasePosition()
        _base_position_info = w3_hypervisor.pool.position(
            ownerAddress=Web3.toChecksumAddress(w3_hypervisor.address.lower()),
            tickLower=w3_hypervisor.baseLower,
            tickUpper=w3_hypervisor.baseUpper,
        )
        _base_ticks_lower = w3_hypervisor.pool.ticks(w3_hypervisor.baseLower)
        _base_ticks_upper = w3_hypervisor.pool.ticks(w3_hypervisor.baseUpper)
        _base_fee_growth = [
            fee_growth_object(
                index=0,
                feeGrowthGlobalX128=w3_hypervisor.pool.feeGrowthGlobal0X128,
                feeGrowthOutsideLowerX128=_base_ticks_lower["feeGrowthOutside0X128"],
                feeGrowthOutsideUpperX128=_base_ticks_upper["feeGrowthOutside0X128"],
                feeGrowthInsideLastX128=_base_position_info["feeGrowthInside0LastX128"],
            ),
            fee_growth_object(
                index=1,
                feeGrowthGlobalX128=w3_hypervisor.pool.feeGrowthGlobal1X128,
                feeGrowthOutsideLowerX128=_base_ticks_lower["feeGrowthOutside1X128"],
                feeGrowthOutsideUpperX128=_base_ticks_upper["feeGrowthOutside1X128"],
                feeGrowthInsideLastX128=_base_position_info["feeGrowthInside1LastX128"],
            ),
        ]
        _base_collected_fees = w3_hypervisor.get_fees_collected(False)
        _base_uncollected_fees = w3_hypervisor.get_fees_uncollected(False)

        # get limit position variables
        _limit_position_amounts = w3_hypervisor.getLimitPosition()
        _limit_position_info = w3_hypervisor.pool.position(
            ownerAddress=Web3.toChecksumAddress(w3_hypervisor.address.lower()),
            tickLower=w3_hypervisor.limitLower,
            tickUpper=w3_hypervisor.limitUpper,
        )
        _limit_ticks_lower = w3_hypervisor.pool.ticks(w3_hypervisor.limitLower)
        _limit_ticks_upper = w3_hypervisor.pool.ticks(w3_hypervisor.limitUpper)
        _limit_fee_growth = [
            fee_growth_object(
                index=0,
                feeGrowthGlobalX128=w3_hypervisor.pool.feeGrowthGlobal0X128,
                feeGrowthOutsideLowerX128=_limit_ticks_lower["feeGrowthOutside0X128"],
                feeGrowthOutsideUpperX128=_limit_ticks_upper["feeGrowthOutside0X128"],
                feeGrowthInsideLastX128=_limit_position_info[
                    "feeGrowthInside0LastX128"
                ],
            ),
            fee_growth_object(
                index=1,
                feeGrowthGlobalX128=w3_hypervisor.pool.feeGrowthGlobal1X128,
                feeGrowthOutsideLowerX128=_limit_ticks_lower["feeGrowthOutside1X128"],
                feeGrowthOutsideUpperX128=_limit_ticks_upper["feeGrowthOutside1X128"],
                feeGrowthInsideLastX128=_limit_position_info[
                    "feeGrowthInside1LastX128"
                ],
            ),
        ]
        _limit_collected_fees = w3_hypervisor.get_fees_collected(False)
        _limit_uncollected_fees = w3_hypervisor.get_fees_uncollected(False)

        # create positions
        _position_base = position_object(
            name="base",
            liquidity=_base_position_amounts["liquidity"],
            qtty=token_group_object(
                token0=_base_position_amounts["amount0"],
                token1=_base_position_amounts["amount1"],
            ),
            lowerTick=w3_hypervisor.baseLower,
            upperTick=w3_hypervisor.baseUpper,
            fees=fees_object(
                collected=token_group_object(
                    token0=_base_collected_fees["qtty_token0"],
                    token1=_base_collected_fees["qtty_token1"],
                ),
                uncollected_lp=token_group_object(
                    token0=_base_uncollected_fees["lps_qtty_token0"],
                    token1=_base_uncollected_fees["lps_qtty_token1"],
                ),
                uncollected_gamma=token_group_object(
                    token0=_base_uncollected_fees["gamma_qtty_token0"],
                    token1=_base_uncollected_fees["gamma_qtty_token1"],
                ),
                fee_growth=_base_fee_growth,
            ),
        )
        _position_limit = position_object(
            name="limit",
            liquidity=_limit_position_amounts["liquidity"],
            qtty=token_group_object(
                token0=_limit_position_amounts["amount0"],
                token1=_limit_position_amounts["amount1"],
            ),
            lowerTick=w3_hypervisor.limitLower,
            upperTick=w3_hypervisor.limitUpper,
            fees=fees_object(
                collected=token_group_object(
                    token0=_limit_collected_fees["qtty_token0"],
                    token1=_limit_collected_fees["qtty_token1"],
                ),
                uncollected_lp=token_group_object(
                    token0=_limit_uncollected_fees["lps_qtty_token0"],
                    token1=_limit_uncollected_fees["lps_qtty_token1"],
                ),
                uncollected_gamma=token_group_object(
                    token0=_limit_uncollected_fees["gamma_qtty_token0"],
                    token1=_limit_uncollected_fees["gamma_qtty_token1"],
                ),
                fee_growth=_limit_fee_growth,
            ),
        )

        # return positions
        return [_position_base, _position_limit]

    def convert_pool(
        w3_hypervisor: gamma_hypervisor, chain: Chain
    ) -> pool_database_object:
        # create pool tokens
        _pool_token0 = token_object(
            address=w3_hypervisor.pool.token0.address,
            decimals=w3_hypervisor.pool.token0.decimals,
            symbol=w3_hypervisor.pool.token0.symbol,
            totalSupply=w3_hypervisor.pool.token0.totalSupply,
        )
        _pool_token1 = token_object(
            address=w3_hypervisor.pool.token1.address,
            decimals=w3_hypervisor.pool.token1.decimals,
            symbol=w3_hypervisor.pool.token1.symbol,
            totalSupply=w3_hypervisor.pool.token1.totalSupply,
        )
        tokens = [_pool_token0, _pool_token1]

        # slot
        slot0 = w3_hypervisor.pool.slot0

        return (
            pool_database_object(
                address=w3_hypervisor.pool.address,
                fee=w3_hypervisor.pool.fee,
                chain=chain,
                protocol=Protocol(w3_hypervisor.pool.identify_dex_name()),
                time=time_object(
                    block=w3_hypervisor.pool.block,
                    timestamp=w3_hypervisor.pool._timestamp,
                ),
                tickSpacing=w3_hypervisor.pool.tickSpacing,
                tokens=tokens,
                protocolFees=w3_hypervisor.pool.protocolFees,
                feeGrowthGlobal0X128=w3_hypervisor.pool.feeGrowthGlobal0X128,
                feeGrowthGlobal1X128=w3_hypervisor.pool.feeGrowthGlobal1X128,
                liquidity=w3_hypervisor.pool.liquidity,
                maxLiquidityPerTick=w3_hypervisor.pool.maxLiquidityPerTick,
                slot0=slot0,
                sqrtPriceX96=slot0["sqrtPriceX96"],
                tick=slot0["tick"],
            ),
        )

    @staticmethod
    def convert_hypervisor_status(
        w3_hypervisor: gamma_hypervisor,
        chain: Chain,
    ) -> hypervisor_database_object:
        """Convert hypervisor status from w3 object to database object

        Args:
            w3_hypervisor (gamma_hypervisor): w3 hypervisor object
            chain (Chain): chain

        Returns:
            hypervisor_database_object:  database object
        """
        # create positions
        positions = converter_hypervisor_status_from_w3_to_object.convert_positions(
            w3_hypervisor=w3_hypervisor
        )

        pool = converter_hypervisor_status_from_w3_to_object.convert_pool(
            w3_hypervisor=w3_hypervisor, chain=Chain(w3_hypervisor.chain)
        )

        return hypervisor_database_object(
            id=create_id_hypervisor_status(
                hypervisor_address=w3_hypervisor.address, block=w3_hypervisor.block
            ),
            chain=chain,
            protocol=Protocol(w3_hypervisor.identify_dex_name()),
            token_info=token_object(
                address=w3_hypervisor.address,
                decimals=w3_hypervisor.decimals,
                symbol=w3_hypervisor.symbol,
                totalSupply=w3_hypervisor.totalSupply,
            ),
            time=time_object(
                block=w3_hypervisor.block,
                timestamp=w3_hypervisor._timestamp,
            ),
            positions=positions,
            maxTotalSupply=w3_hypervisor.maxTotalSupply,
            deposit0Max=w3_hypervisor.deposit0Max,
            deposit1Max=w3_hypervisor.deposit1Max,
            fee=w3_hypervisor.fee,
            pool=pool,
        )
