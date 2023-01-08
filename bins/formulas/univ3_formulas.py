
from web3 import Web3


X32 = 2**32
X96 = 2**96
X96_RESOLLUTION = 96
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

    # return result
    return {"qtty_token0": amount0/(10**decimals_token0),
            "qtty_token1": amount1/(10**decimals_token1),
            "price_token0": prices["priceCurrent"]/ (10**decimal_diff),
            "price_token1": prices_adj["priceCurrent"]
            }

def sqrtPriceX96_to_price_float(sqrtPriceX96:int, token0_decimals:int, token1_decimals:int)->float:
    return ((sqrtPriceX96**2) / 2 ** (96 * 2)) * 10 ** (token0_decimal - token1_decimal)







class LiquidityAmounts:

    @staticmethod
    def getLiquidityForAmount0(sqrtRatioAX96, sqrtRatioBX96, amount0)->int:
        """  Computes the amount of liquidity received for a given amount of token0 and price range

        Args:
            sqrtRatioAX96 (_type_): _description_
            sqrtRatioBX96 (_type_): _description_
            amount0 (_type_): _description_

        Returns:
            int: liquidity The amount of returned liquidity
        """    
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        intermediate = (RA*RB)/X96
        return int((amount0*intermediate)/(RB - RA))
    @staticmethod
    def getLiquidityForAmount1(sqrtRatioAX96, sqrtRatioBX96, amount1)->int:
        """  Computes the amount of liquidity received for a given amount of token1 and price range

        Args:
            sqrtRatioAX96 (_type_): _description_
            sqrtRatioBX96 (_type_): _description_
            amount1 (_type_): _description_

        Returns:
            int: liquidity The amount of returned liquidity
        """    
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        return int((amount0*intermediate)/(RB - RA))
    @staticmethod
    def getLiquidityForAmounts(sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, amount0, amount1)->int:
        """ Computes the maximum amount of liquidity received for a given amount of token0, token1, the current
            pool prices and the prices at the tick boundaries

         Args:
            sqrtRatioX96 (_type_): A sqrt price representing the current pool prices
            sqrtRatioAX96 (_type_): A sqrt price representing the first tick boundary
            sqrtRatioBX96 (_type_): A sqrt price representing the second tick boundary
            amount0 (int): The amount of token0 being sent in
            amount1 (int): The amount of token1 being sent in

         Returns:
            int: liquidity
         """    
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        if sqrtRatioX96 <= sqrtRatioAX96:
            liquidity = LiquidityAmounts.getLiquidityForAmount0(sqrtRatioAX96, sqrtRatioBX96, amount0)
        elif sqrtRatioX96 < sqrtRatioBX96:
            liquidity0 = LiquidityAmounts.getLiquidityForAmount0(sqrtRatioX96, sqrtRatioBX96, amount0)
            liquidity1 = LiquidityAmounts.getLiquidityForAmount1(sqrtRatioAX96, sqrtRatioX96, amount1)
            # decide
            liquidity = liquidity0 if (liquidity0 < liquidity1) else liquidity1

        else:
            liquidity = LiquidityAmounts.getLiquidityForAmount1(sqrtRatioAX96, sqrtRatioBX96, amount1)

        # result
        return liquidity


    @staticmethod
    def getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)->int:
        """ Computes the amount of token0 for a given amount of liquidity and a price range

        Args:
            sqrtRatioAX96 (_type_): A sqrt price representing the first tick boundary
            sqrtRatioBX96 (_type_): A sqrt price representing the second tick boundary
            liquidity (_type_): The liquidity being valued

        Returns:
            int: The amount of token0
        """    
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96
        
        return int((((liquidity << X96_RESOLLUTION) * (RB - RA)) / RB) / RA)
    @staticmethod
    def getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)->int:
        """ Computes the amount of token1 for a given amount of liquidity and a price range

        Args:
            sqrtRatioAX96 (_type_): A sqrt price representing the first tick boundary
            sqrtRatioBX96 (_type_): A sqrt price representing the second tick boundary
            liquidity (_type_): The liquidity being valued

        Returns:
            int: The amount of token1
        """    
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96
        
        return int((liquidity * (RB - RA)) / X96)
    @staticmethod
    def getAmountsForLiquidity(sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, liquidity)->tuple:
        """getAmountsForLiquidity _summary_

         Args:
            sqrtRatioX96 (_type_): _description_
            sqrtRatioAX96 (_type_): _description_
            sqrtRatioBX96 (_type_): _description_
            liquidity (_type_): _description_

         Returns:
            tuple: _description_
         """        
        if sqrtRatioAX96 > sqrtRatioBX96:
            # reverse
            RA = sqrtRatioBX96
            RB = sqrtRatioAX96
        else:
            RA = sqrtRatioAX96
            RB = sqrtRatioBX96

        amount0 = 0
        amount1 = 0
        if sqrtRatioX96 <= sqrtRatioAX96:
            amount0 = LiquidityAmounts.getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)
        elif sqrtRatioX96 < sqrtRatioBX96:
            amount0 = LiquidityAmounts.getAmount0ForLiquidity(sqrtRatioX96, sqrtRatioBX96, liquidity)
            amount1 = LiquidityAmounts.getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioX96, liquidity)
        else:
            amount1 = LiquidityAmounts.getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)

        return amount0, amount1


class TickMath:

  MIN_TICK: int = -887272 # min tick that can be used on any pool
  MAX_TICK: int = -MIN_TICK # max tick that can be used on any pool

  MIN_SQRT_RATIO: int = 4295128739 # sqrt ratio of the min tick
  MAX_SQRT_RATIO: int = 1461446703485210103287273052203988822378723970342 # sqrt ratio of the max tick

  @staticmethod
  def getSqrtRatioAtTick(tick: int) -> int:
    """ Calculates sqrt(1.0001^tick) * 2^96

     Args:
          tick (int): The input tick for the above formula

     Returns:
          int: A Fixed point Q64.96 number representing the sqrt of the ratio of the two assets (token1/token0) at the given tick
     """    

    if tick < TickMath.MIN_TICK or tick > TickMath.MAX_TICK or type(tick) != int:
        raise ValueError(" Tick is not within uniswap's min-max parameters")

    absTick: int = abs(tick)

    ratio: int = 0xfffcb933bd6fad37aa2d162d1a594001 if (absTick & 0x1) != 0 else 0x100000000000000000000000000000000

    if ((absTick & 0x2) != 0):
        ratio = (ratio * 0xfff97272373d413259a46990580e213a) >> 128

    if ((absTick & 0x4) != 0):
        ratio = (ratio * 0xfff2e50f5f656932ef12357cf3c7fdcc) >> 128

    if ((absTick & 0x8) != 0):
        ratio = (ratio * 0xffe5caca7e10e4e61c3624eaa0941cd0) >> 128

    if ((absTick & 0x10) != 0):
        ratio = (ratio * 0xffcb9843d60f6159c9db58835c926644) >> 128

    if ((absTick & 0x20) != 0):
        ratio = (ratio * 0xff973b41fa98c081472e6896dfb254c0) >> 128

    if ((absTick & 0x40) != 0):
        ratio = (ratio * 0xff2ea16466c96a3843ec78b326b52861) >> 128

    if ((absTick & 0x80) != 0):
        ratio = (ratio * 0xfe5dee046a99a2a811c461f1969c3053) >> 128

    if ((absTick & 0x100) != 0):
        ratio = (ratio * 0xfcbe86c7900a88aedcffc83b479aa3a4) >> 128

    if ((absTick & 0x200) != 0):
        ratio = (ratio * 0xf987a7253ac413176f2b074cf7815e54) >> 128

    if ((absTick & 0x400) != 0):
        ratio = (ratio * 0xf3392b0822b70005940c7a398e4b70f3) >> 128

    if ((absTick & 0x800) != 0):
        ratio = (ratio * 0xe7159475a2c29b7443b29c7fa6e889d9) >> 128

    if ((absTick & 0x1000) != 0):
        ratio = (ratio * 0xd097f3bdfd2022b8845ad8f792aa5825) >> 128

    if ((absTick & 0x2000) != 0):
        ratio = (ratio * 0xa9f746462d870fdf8a65dc1f90e061e5) >> 128

    if ((absTick & 0x4000) != 0):
        ratio = (ratio * 0x70d869a156d2a1b890bb3df62baf32f7) >> 128

    if ((absTick & 0x8000) != 0):
        ratio = (ratio * 0x31be135f97d08fd981231505542fcfa6) >> 128

    if ((absTick & 0x10000) != 0):
        ratio = (ratio * 0x9aa508b5b7a84e1c677de54f3e99bc9) >> 128

    if ((absTick & 0x20000) != 0):
        ratio = (ratio * 0x5d6af8dedb81196699c329225ee604) >> 128

    if ((absTick & 0x40000) != 0):
        ratio = (ratio * 0x2216e584f5fa1ea926041bedfe98) >> 128

    if ((absTick & 0x80000) != 0):
        ratio = (ratio * 0x48a170391f7dc42444e8fa2) >> 128


    if (tick > 0):
        ratio = ((2**256)-1) // ratio

    # back to Q96
    return (ratio // X32) + 1 if ratio % X32 > 0 else ratio // X32

  @staticmethod
  def getTickAtSqrtRatio(sqrtRatioX96: int) -> int:
    """
    * Returns the tick corresponding to a given sqrt ratio, s.t. #getSqrtRatioAtTick(tick) <= sqrtRatioX96
    * and #getSqrtRatioAtTick(tick + 1) > sqrtRatioX96
    * @param sqrtRatioX96 the sqrt ratio as a Q64.96 for which to compute the tick
    """

    if sqrtRatioX96 < TickMath.MIN_SQRT_RATIO or sqrtRatioX96 > TickMath.MAX_SQRT_RATIO:
        raise ValueError(" Tick is not within uniswap's min-max parameters")

    sqrtRatioX128 = sqrtRatioX96 << 32

    msb = mostSignificantBit(sqrtRatioX128)

    if (msb >= 128):
      r = sqrtRatioX128 >> (msb - 127)
    else:
      r = sqrtRatioX128 << (127 - msb)

    log_2: int = (msb - 128) << 64

    for i in range(14):
      r = (r**2) >> 127
      f = r >> 128
      log_2 = log_2 | (f << (63 - i))
      r = r >> f

    log_sqrt10001 = log_2 * 255738958999603826347141

    tickLow = (log_sqrt10001 - 3402992956809132418596140100660247210) >> 128
    tickHigh = (log_sqrt10001 + 291339464771989622907027621153398088495) >> 128

    return tickLow if tickLow == tickHigh else tickHigh if TickMath.getSqrtRatioAtTick(tickHigh) <= sqrtRatioX96 else tickLow





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


