
from web3 import Web3


X96 = 2**96
X128 = 2**128
X256 = 2**256

def subIn256(x, y):
    difference = x - y
    if difference < 0:
        difference += X256

    return difference

def get_uncollected_fees( feeGrowthGlobal, feeGrowthOutsideLower, feeGrowthOutsideUpper, feeGrowthInsideLast,
              tickCurrent, liquidity, tickLower, tickUpper):
    """ Precise method to calc uncollected fees

     Args:
        feeGrowthGlobal (_type_): _description_
        feeGrowthOutsideLower (_type_): _description_
        feeGrowthOutsideUpper (_type_): _description_
        feeGrowthInsideLast (_type_): _description_
        tickCurrent (_type_): _description_
        liquidity (_type_): _description_
        tickLower (_type_): _description_
        tickUpper (_type_): _description_

     Returns:
        fees
    """    
            
    feeGrowthBelow = 0
    if (tickCurrent >= tickLower):
        feeGrowthBelow = feeGrowthOutsideLower
    else:
        feeGrowthBelow = subIn256(feeGrowthGlobal, feeGrowthOutsideLower)

    feeGrowthAbove= 0
    if (tickCurrent < tickUpper):
        feeGrowthAbove = feeGrowthOutsideUpper
    else:
        feeGrowthAbove = subIn256(feeGrowthGlobal, feeGrowthOutsideUpper)
    
    feeGrowthInside = subIn256(subIn256(feeGrowthGlobal, feeGrowthBelow), feeGrowthAbove)

    return (subIn256(feeGrowthInside, feeGrowthInsideLast)*(liquidity))/X128

def get_positionKey(ownerAddress:str, tickLower:int, tickUpper:int)->str:
    """ Position key 
    
     Args:
        ownerAddress (_type_): position owner wallet address
        tickLower (_type_): lower tick
        tickUpper (_type_): upper tick

        Returns:
            position key 
        """        
    val_types = ["address","int24","int24"]
    values =[ownerAddress,tickLower,tickUpper]
    return Web3.solidityKeccak(val_types, values).hex()

def convert_tick_to_price(tick:int)->float:
    """ convert int ticks into not decimal adjusted float price
        
     Args:
        tick (int)

     Returns:
        float: price (not decimal adjusted)
     """ 
    return float(1.0001 ** tick)
def convert_tick_to_price_float(tick:int, token0_decimal:int, token1_decimal:int)->float:
    """ convert int ticks into decimal float price
        
     Args:
        tick (int)
        token0_decimal

     Returns:
        float: price (not decimal adjusted)
     """ 
    return convert_tick_to_price(tick) * 10 ** (token0_decimal - token1_decimal)

def get_position_quantity_and_price(liquidity, tickCurrent, tickUpper, tickLower, decimals_token0, decimals_token1)->dict:
    """  Calculate the position's locked token quantity and its prices ( token0 in token1 prices... not usd )

     Args:
        liquidity (int): _description_
        tickCurrent (int): _description_
        tickUpper (int): _description_
        tickLower (int): _description_
        decimals_token0 (int): _description_
        decimals_token1 (int): _description_

     Returns:
        dict: { "qtty_token0": (float)
                "qtty_token1": (float)
                "price_token0": (float)
                "price_token1":  (float)
                }
     """

    # get decimal difference btween tokens
    decimal_diff = decimals_token1-decimals_token0

    # Tick PRICEs
    # calc tick prices (not decimal adjusted)
    prices = {"priceCurrent": convert_tick_to_price(tickCurrent),
              "priceUpper": convert_tick_to_price(tickUpper),
              "priceLower": convert_tick_to_price(tickLower)
            }
    # prepare price related vars 
    prices_sqrt = dict()
    prices_adj = dict()
    for k,v in prices.items():
        # Square root prices 
        prices_sqrt[k] = v**2
        # adjust decimals and reverse bc price in Uniswap is defined to be equal to token1/token0
        prices_adj[k] = 1/(v/ (10 ** decimal_diff))
    

    if (prices["priceCurrent"] <= prices["priceLower"]):
        amount0 = float(liquidity * float(1 / prices_sqrt["priceLower"] - 1 / prices_sqrt["priceUpper"]))
        amount1 = 0
    elif (prices["priceCurrent"] < prices["priceUpper"]):
        amount0 = float(liquidity * float(1 / prices_sqrt["priceCurrent"] - 1 / prices_sqrt["priceUpper"]))
        amount1 = float(liquidity * float(prices_sqrt["priceCurrent"] - prices_sqrt["priceLower"]))
    else:
        amount1 = float(liquidity * float(prices_sqrt["priceUpper"] - prices_sqrt["priceLower"]))
        amount0 = 0


    return {"qtty_token0": amount0,
            "qtty_token1": amount1,
            "price_token0": prices["priceCurrent"]/ (10**decimal_diff),
            "price_token1": prices_adj["priceCurrent"]
            }

def sqrtPriceX96_to_price_float(sqrtPriceX96:int, token0_decimals:int, token1_decimals:int)->float:
    return ((sqrtPriceX96**2) / 2 ** (96 * 2)) * 10 ** (token0_decimal - token1_decimal)








######### for comparison purposes
######### def as they are defined at : 
######### https://github.com/GammaStrategies/uniswap-v3-performance
#########
def get_uncollected_fees_vGammawire(
    fee_growth_global_0,
    fee_growth_global_1,
    tick_current,
    tick_lower,
    tick_upper,
    fee_growth_outside_0_lower,
    fee_growth_outside_1_lower,
    fee_growth_outside_0_upper,
    fee_growth_outside_1_upper,
    liquidity,
    fee_growth_inside_last_0,
    fee_growth_inside_last_1,
    ):

        if tick_current >= tick_lower:
            fee_growth_below_pos_0 = fee_growth_outside_0_lower
            fee_growth_below_pos_1 = fee_growth_outside_1_lower
        else:
            fee_growth_below_pos_0 = subIn256(
                fee_growth_global_0, fee_growth_outside_0_lower
            )
            fee_growth_below_pos_1 = subIn256(
                fee_growth_global_1, fee_growth_outside_1_lower
            )

        if tick_current >= tick_upper:
            fee_growth_above_pos_0 = subIn256(
                fee_growth_global_0, fee_growth_outside_0_upper
            )
            fee_growth_above_pos_1 = subIn256(
                fee_growth_global_1, fee_growth_outside_1_upper
            )
        else:
            fee_growth_above_pos_0 = fee_growth_outside_0_upper
            fee_growth_above_pos_1 = fee_growth_outside_1_upper

        fees_accum_now_0 = subIn256(
            subIn256(fee_growth_global_0, fee_growth_below_pos_0),
            fee_growth_above_pos_0,
        )
        fees_accum_now_1 = subIn256(
            subIn256(fee_growth_global_1, fee_growth_below_pos_1),
            fee_growth_above_pos_1,
        )

        uncollectedFees_0 = (
            liquidity * (subIn256(fees_accum_now_0, fee_growth_inside_last_0))
        ) / X128
        uncollectedFees_1 = (
            liquidity * (subIn256(fees_accum_now_1, fee_growth_inside_last_1))
        ) / X128

        return uncollectedFees_0, uncollectedFees_1


