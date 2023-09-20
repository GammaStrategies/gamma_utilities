import logging
import sys

from decimal import Decimal
from hexbytes import HexBytes
from web3 import Web3

from bins.errors.general import ProcessingError

from ....configuration import WEB3_CHAIN_IDS
from ....formulas import dex_formulas
from ....general.enums import Protocol, error_identity, text_to_chain
from ..general import (
    bep20,
    bep20_cached,
    web3wrap,
    erc20,
    erc20_cached,
)
from ....cache import cache_utilities


# ALGEBRA POOL


class dataStorageOperator(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "dataStorageOperator"
        self._abi_path = abi_path or f"{self.abi_root_path}/algebra/v3"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    # TODO: Implement contract functs calculateVolumePerLiquidity, getAverages, getFee, getSingleTimepoint, getTimepoints and timepoints

    @property
    def feeConfig(self) -> dict:
        """feeConfig _summary_

        Returns:
            dict:   { alpha1   uint16 :  100
                        alpha2   uint16 :  3600
                        beta1   uint32 :  500
                        beta2   uint32 :  80000
                        gamma1   uint16 :  80
                        gamma2   uint16 :  11750
                        volumeBeta   uint32 :  0
                        volumeGamma   uint16 :  10
                        baseFee   uint16 :  400 }

        """
        return self.call_function_autoRpc("feeConfig")

    @property
    def window(self) -> int:
        """window _summary_

        Returns:
            int: 86400 uint32
        """
        return self.call_function_autoRpc("window")


class poolv3(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "algebrav3pool"
        self._abi_path = abi_path or f"{self.abi_root_path}/algebra/v3"

        self._token0: erc20 = None
        self._token1: erc20 = None

        self._dataStorage: dataStorageOperator = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    def identify_dex_name(self) -> str:
        return Protocol.ALGEBRAv3.database_name

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = {
            "decimals": False,
            "symbol": False,
            "factory": False,
            "tickSpacing": False,
        }

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

    # PROPERTIES

    @property
    def activeIncentive(self) -> str:
        """activeIncentive

        Returns:
            str: address
        """
        return self.call_function_autoRpc("activeIncentive")

    @property
    def dataStorageOperator(self) -> dataStorageOperator:
        """ """
        if self._dataStorage is None:
            self._dataStorage = dataStorageOperator(
                address=self.call_function_autoRpc("dataStorageOperator"),
                network=self._network,
                block=self.block,
            )
        return self._dataStorage

    @property
    def factory(self) -> str:
        return self.call_function_autoRpc("factory")

    def getInnerCumulatives(self, bottomTick: int, topTick: int):
        """

        Args:
            bottomTick (int): _description_
            topTick (int): _description_

        Returns:
            innerSecondsSpentPerLiquidity uint160, innerSecondsSpent uint32
        """
        return self.call_function_autoRpc(
            "getInnerCumulatives", None, bottomTick, topTick
        )

    def getTimepoints(self, secondsAgo: int) -> dict:
        return self.call_function_autoRpc("getTimepoints", None, secondsAgo)

    @property
    def globalState(self) -> dict:
        """

        Returns:
           dict:   sqrtPriceX96  uint160 :  28854610805518743926885543006518067  ( <price> at contract level)
                   tick   int24 :  256121
                   fee   uint16 :  198
                   timepointIndex   uint16 :  300
                   communityFeeToken0   uint8 :  300
                   communityFeeToken1   uint8 :  0
                   unlocked   bool :  true
        """
        if tmp := self.call_function_autoRpc("globalState"):
            return {
                "sqrtPriceX96": tmp[0],
                "tick": tmp[1],
                "fee": tmp[2],
                "timepointIndex": tmp[3],
                "communityFeeToken0": tmp[4],
                "communityFeeToken1": tmp[5],
                "unlocked": tmp[6],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "pool_address": self.address,
                    "block": self.block,
                    "object": "pool.globalState",
                },
                identity=error_identity.RETURN_NONE,
                action="",
                message=f" globalState function of {self.address} at block {self.block} returned none. (Check contract creation block)",
            )

    @property
    def liquidity(self) -> int:
        """liquidity _summary_

        Returns:
            int: 14468296980040792163 uint128
        """
        return self.call_function_autoRpc("liquidity")

    @property
    def liquidityCooldown(self) -> int:
        """liquidityCooldown _summary_

        Returns:
            int: 0 uint32
        """
        return self.call_function_autoRpc("liquidityCooldown")

    @property
    def maxLiquidityPerTick(self) -> int:
        """maxLiquidityPerTick _summary_

        Returns:
            int: 11505743598341114571880798222544994 uint128
        """
        return self.call_function_autoRpc("maxLiquidityPerTick")

    def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   lastLiquidityAddTimestamp
                   innerFeeGrowth0Token   uint256 :  (feeGrowthInside0LastX128)
                   innerFeeGrowth1Token   uint256 :  (feeGrowthInside1LastX128)
                   fees0   uint128 :  0  (tokensOwed0)
                   fees1   uint128 :  0  ( tokensOwed1)
        """
        position_key = (
            HexBytes(position_key) if type(position_key) == str else position_key
        )
        if result := self.call_function_autoRpc("positions", None, position_key):
            return {
                "liquidity": result[0],
                "lastLiquidityAddTimestamp": result[1],
                "feeGrowthInside0LastX128": result[2],
                "feeGrowthInside1LastX128": result[3],
                "tokensOwed0": result[4],
                "tokensOwed1": result[5],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "pool_address": self.address,
                    "block": self.block,
                    "object": "pool.positions",
                },
                identity=error_identity.RETURN_NONE,
                action="",
                message=f" positions function of {self.address} at block {self.block} returned none using {position_key} as position_key",
            )

    @property
    def tickSpacing(self) -> int:
        """tickSpacing _summary_

        Returns:
            int: 60 int24
        """
        return self.call_function_autoRpc("tickSpacing")

    def tickTable(self, value: int) -> int:
        return self.call_function_autoRpc("tickTable", None, value)

    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0        liquidityTotal
                       liquidityNet   int128 :  0           liquidityDelta
                       feeGrowthOutside0X128   uint256 :  0 outerFeeGrowth0Token
                       feeGrowthOutside1X128   uint256 :  0 outerFeeGrowth1Token
                       tickCumulativeOutside   int56 :  0   outerTickCumulative
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0    outerSecondsPerLiquidity
                       secondsOutside   uint32 :  0         outerSecondsSpent
                       initialized   bool :  false          initialized
        """
        if result := self.call_function_autoRpc("ticks", None, tick):
            return {
                "liquidityGross": result[0],
                "liquidityNet": result[1],
                "feeGrowthOutside0X128": result[2],
                "feeGrowthOutside1X128": result[3],
                "tickCumulativeOutside": result[4],
                "secondsPerLiquidityOutsideX128": result[5],
                "secondsOutside": result[6],
                "initialized": result[7],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "pool_address": self.address,
                    "block": self.block,
                    "object": "pool.ticks",
                },
                identity=error_identity.RETURN_NONE,
                action="",
                message=f" ticks function of {self.address} at block {self.block} returned none. (Check contract creation block)",
            )

    def timepoints(self, index: int) -> dict:
        #   initialized bool, blockTimestamp uint32, tickCumulative int56, secondsPerLiquidityCumulative uint160, volatilityCumulative uint88, averageTick int24, volumePerLiquidityCumulative uint144
        return self.call_function_autoRpc("timepoints", None, index)

    @property
    def token0(self) -> erc20:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 is None:
            self._token0 = erc20(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
        return self.call_function_autoRpc("totalFeeGrowth0Token")

    @property
    def feeGrowthGlobal1X128(self) -> int:
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
        return self.call_function_autoRpc("totalFeeGrowth1Token")

    # CUSTOM PROPERTIES
    @property
    def block(self) -> int:
        return self._block

    @block.setter
    def block(self, value: int):
        # set block
        self._block = value
        self.token0.block = value
        self.token1.block = value

    @property
    def custom_rpcType(self) -> str | None:
        """ """
        return self._custom_rpcType

    @custom_rpcType.setter
    def custom_rpcType(self, value: str | None):
        self._custom_rpcType = value
        self.token0.custom_rpcType = value
        self.token1.custom_rpcType = value

    @property
    def sqrtPriceX96(self) -> int:
        """get the sqrtPriceX96 value"""
        return self.globalState["sqrtPriceX96"]

    # CUSTOM FUNCTIONS
    def position(self, ownerAddress: str, tickLower: int, tickUpper: int) -> dict:
        """

        Returns:
           dict:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        return self.positions(
            dex_formulas.get_positionKey_algebra(
                ownerAddress=ownerAddress,
                tickLower=tickLower,
                tickUpper=tickUpper,
            )
        )

    def get_qtty_depoloyed(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Args:
           ownerAddress (str):
           tickUpper (int):
           tickLower (int):
           inDecimal (bool): return result in a decimal format?

        Returns:
           dict: {
                   "qtty_token0":0,        (int or Decimal) # quantity of token 0 deployed in dex
                   "qtty_token1":0,        (int or Decimal) # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,   (int or Decimal) # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,   (int or Decimal) # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """

        result = {
            "qtty_token0": 0,  # quantity of token 0 deployed in dex
            "qtty_token1": 0,  # quantity of token 1 deployed in dex
            "fees_owed_token0": 0,  # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
            "fees_owed_token1": 0,  # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )
        # get slot data
        slot0 = self.globalState

        # get current tick from slot
        tickCurrent = slot0["tick"]
        sqrtRatioX96 = slot0["sqrtPriceX96"]
        sqrtRatioAX96 = dex_formulas.TickMath.getSqrtRatioAtTick(tickLower)
        sqrtRatioBX96 = dex_formulas.TickMath.getSqrtRatioAtTick(tickUpper)
        # calc quantity from liquidity
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = dex_formulas.LiquidityAmounts.getAmountsForLiquidity(
            sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, pos["liquidity"]
        )

        # add owed tokens
        result["fees_owed_token0"] = pos["tokensOwed0"]
        result["fees_owed_token1"] = pos["tokensOwed1"]

        # convert to decimal as needed
        if inDecimal:
            self._get_qtty_depoloyed_todecimal(result)
        # return result
        return result.copy()

    def _get_qtty_depoloyed_todecimal(self, result):
        # get token decimals
        decimals_token0 = self.token0.decimals
        decimals_token1 = self.token1.decimals

        result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
            10**decimals_token0
        )
        result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
            10**decimals_token1
        )
        result["fees_owed_token0"] = Decimal(result["fees_owed_token0"]) / Decimal(
            10**decimals_token0
        )
        result["fees_owed_token1"] = Decimal(result["fees_owed_token1"]) / Decimal(
            10**decimals_token1
        )

    def get_fees_uncollected(
        self,
        ownerAddress: str,
        tickUpper: int,
        tickLower: int,
        protocolFee: int,
        inDecimal: bool = True,
    ) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Args:
            ownerAddress (str):
            tickUpper (int):
            tickLower (int):
            protocolFee (int) gamma protocol fee percentage
            inDecimal (bool): return result in a decimal format?

        Returns:
            dict: {
                    "qtty_token0":0,   (int or Decimal)     # quantity of uncollected token 0
                    "qtty_token1":0,   (int or Decimal)     # quantity of uncollected token 1
                }
        """

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
            "gamma_qtty_token0": 0,
            "gamma_qtty_token1": 0,
            "lps_qtty_token0": 0,
            "lps_qtty_token1": 0,
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # get ticks
        tickCurrent = self.globalState["tick"]
        ticks_lower = self.ticks(tickLower)
        ticks_upper = self.ticks(tickUpper)

        # TODO: check if should do something else:  do not continue if position is outside of the range, return zero
        if tickCurrent < tickLower or tickCurrent > tickUpper:
            logging.getLogger(__name__).warning(
                f" one position of {ownerAddress} is outside of range. No uncollected fees to calculate."
            )
            return result.copy()

        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = dex_formulas.get_uncollected_fees_vGammawire(
            fee_growth_global_0=self.feeGrowthGlobal0X128,
            fee_growth_global_1=self.feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower,
            tick_upper=tickUpper,
            fee_growth_outside_0_lower=ticks_lower["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=ticks_lower["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=ticks_upper["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=ticks_upper["feeGrowthOutside1X128"],
            liquidity=pos["liquidity"],
            fee_growth_inside_last_0=pos["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pos["feeGrowthInside1LastX128"],
        )

        # calculate LPs and Gamma fees
        result["gamma_qtty_token0"] = dex_formulas.mulDiv(
            result["qtty_token0"], protocolFee, 100
        )
        result["gamma_qtty_token1"] = dex_formulas.mulDiv(
            result["qtty_token1"], protocolFee, 100
        )
        result["lps_qtty_token0"] = result["qtty_token0"] - result["gamma_qtty_token0"]
        result["lps_qtty_token1"] = result["qtty_token1"] - result["gamma_qtty_token1"]

        # convert to decimal as needed
        if inDecimal:
            result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
                10**self.token0.decimals
            )
            result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
                10**self.token1.decimals
            )
            result["gamma_qtty_token0"] = Decimal(
                result["gamma_qtty_token0"]
            ) / Decimal(10**self.token0.decimals)
            result["gamma_qtty_token1"] = Decimal(
                result["gamma_qtty_token1"]
            ) / Decimal(10**self.token1.decimals)
            result["lps_qtty_token0"] = Decimal(result["lps_qtty_token0"]) / Decimal(
                10**self.token0.decimals
            )
            result["lps_qtty_token1"] = Decimal(result["lps_qtty_token1"]) / Decimal(
                10**self.token1.decimals
            )

        # return result
        return result.copy()

    def get_fees_collected(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of fees collected by the deployed position"""

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
        }

        # get position data
        pos = self.position(
            ownerAddress=Web3.toChecksumAddress(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        # convert the position known feeGrowth to fees
        result["qtty_token0"] = dex_formulas.feeGrowth_to_fee(
            feeGrowthX128=pos["feeGrowthInside0LastX128"], liquidity=pos["liquidity"]
        )
        result["qtty_token1"] = dex_formulas.feeGrowth_to_fee(
            feeGrowthX128=pos["feeGrowthInside1LastX128"], liquidity=pos["liquidity"]
        )

        # convert to decimal as needed
        if inDecimal:
            result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
                10**self.token0.decimals
            )
            result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
                10**self.token1.decimals
            )
        # return result
        return result.copy()

    def get_total_pool_fees(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees collected by all LPs"""

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
        }

        # convert the position known feeGrowth to fees
        result["qtty_token0"] = dex_formulas.feeGrowth_to_fee(
            feeGrowthX128=self.feeGrowthGlobal0X128, liquidity=self.liquidity
        )
        result["qtty_token1"] = dex_formulas.feeGrowth_to_fee(
            feeGrowthX128=self.feeGrowthGlobal1X128, liquidity=self.liquidity
        )

        # convert to decimal as needed
        if inDecimal:
            result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
                10**self.token0.decimals
            )
            result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
                10**self.token1.decimals
            )
        # return result
        return result.copy()

    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): convert big integers to string. Defaults to False.
            static_mode (bool, optional): return  static fields only. Defaults to False.

        Returns:
            dict:
        """

        result = super().as_dict(convert_bint=convert_bint)

        result["activeIncentive"] = self.activeIncentive

        result["liquidityCooldown"] = (
            str(self.liquidityCooldown) if convert_bint else self.liquidityCooldown
        )

        result["maxLiquidityPerTick"] = (
            str(self.maxLiquidityPerTick) if convert_bint else self.maxLiquidityPerTick
        )

        # t spacing
        # result["tickSpacing"] = (
        #     self.tickSpacing if not convert_bint else str(self.tickSpacing)
        # )

        # add fee so that it has same field as univ3 pool to dict
        result["fee"] = self.globalState["fee"]

        # identify pool dex
        result["dex"] = self.identify_dex_name()

        result["token0"] = self.token0.as_dict(convert_bint=convert_bint)
        result["token1"] = self.token1.as_dict(convert_bint=convert_bint)

        if not static_mode:
            result["feeGrowthGlobal0X128"] = (
                str(self.feeGrowthGlobal0X128)
                if convert_bint
                else self.feeGrowthGlobal0X128
            )

            result["feeGrowthGlobal1X128"] = (
                str(self.feeGrowthGlobal1X128)
                if convert_bint
                else self.feeGrowthGlobal1X128
            )

            result["liquidity"] = (
                str(self.liquidity) if convert_bint else self.liquidity
            )

            result["globalState"] = self.globalState
            if convert_bint:
                try:
                    result["globalState"]["sqrtPriceX96"] = (
                        str(result["globalState"]["sqrtPriceX96"])
                        if "sqrtPriceX96" in result["globalState"]
                        else ""
                    )
                    # result["globalState"]["price"] = (
                    #     str(result["globalState"]["price"])
                    #     if "price" in result["globalState"]
                    #     else ""
                    # )
                    result["globalState"]["tick"] = (
                        str(result["globalState"]["tick"])
                        if "tick" in result["globalState"]
                        else ""
                    )
                    result["globalState"]["fee"] = (
                        str(result["globalState"]["fee"])
                        if "fee" in result["globalState"]
                        else ""
                    )
                    result["globalState"]["timepointIndex"] = (
                        str(result["globalState"]["timepointIndex"])
                        if "timepointIndex" in result["globalState"]
                        else ""
                    )
                except Exception:
                    logging.getLogger(__name__).warning(
                        f' Unexpected error converting globalState of {result["address"]} at block {result["block"]}     error-> {sys.exc_info()[0]}   globalState: {result["globalState"]}'
                    )

        return result


class poolv3_bep20(poolv3):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "algebrav3pool"
        self._abi_path = abi_path or f"{self.abi_root_path}/algebra/v3"

        self._token0: bep20 = None
        self._token1: bep20 = None

        self._dataStorage: dataStorageOperator = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def token0(self) -> bep20:
        if self._token0 is None:
            self._token0 = bep20(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20:
        if self._token1 is None:
            self._token1 = bep20(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1


# -> Cached version of the pool


class dataStorageOperator_cached(dataStorageOperator):
    @property
    def feeConfig(self) -> dict:
        prop_name = "feeConfig"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class poolv3_cached(poolv3):
    SAVE2FILE = True

    # PROPERTIES

    @property
    def activeIncentive(self) -> str:
        prop_name = "activeIncentive"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def dataStorageOperator(self) -> dataStorageOperator:
        """ """
        if self._dataStorage is None:
            self._dataStorage = dataStorageOperator_cached(
                address=self.call_function_autoRpc("dataStorageOperator"),
                # address=self._contract.functions.dataStorageOperator().call(
                #     block_identifier=self.block
                # ),
                network=self._network,
                block=self.block,
            )
        return self._dataStorage

    @property
    def factory(self) -> str:
        prop_name = "factory"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def globalState(self) -> dict:
        prop_name = "globalState"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result.copy()

    @property
    def liquidity(self) -> int:
        prop_name = "liquidity"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def liquidityCooldown(self) -> int:
        prop_name = "liquidityCooldown"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxLiquidityPerTick(self) -> int:
        prop_name = "maxLiquidityPerTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20_cached:
        if self._token0 is None:
            self._token0 = erc20_cached(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20_cached:
        if self._token1 is None:
            self._token1 = erc20_cached(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def feeGrowthGlobal0X128(self) -> int:
        prop_name = "feeGrowthGlobal0X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeGrowthGlobal1X128(self) -> int:
        prop_name = "feeGrowthGlobal1X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class poolv3_bep20_cached(poolv3_bep20):
    SAVE2FILE = True

    # PROPERTIES

    @property
    def activeIncentive(self) -> str:
        prop_name = "activeIncentive"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def dataStorageOperator(self) -> dataStorageOperator:
        """ """
        if self._dataStorage is None:
            self._dataStorage = dataStorageOperator_cached(
                address=self.call_function_autoRpc("dataStorageOperator"),
                # address=self._contract.functions.dataStorageOperator().call(
                #     block_identifier=self.block
                # ),
                network=self._network,
                block=self.block,
            )
        return self._dataStorage

    @property
    def factory(self) -> str:
        prop_name = "factory"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def globalState(self) -> dict:
        prop_name = "globalState"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result.copy()

    @property
    def liquidity(self) -> int:
        prop_name = "liquidity"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def liquidityCooldown(self) -> int:
        prop_name = "liquidityCooldown"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxLiquidityPerTick(self) -> int:
        prop_name = "maxLiquidityPerTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> bep20_cached:
        if self._token0 is None:
            self._token0 = bep20_cached(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20_cached:
        if self._token1 is None:
            self._token1 = bep20_cached(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def feeGrowthGlobal0X128(self) -> int:
        prop_name = "feeGrowthGlobal0X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeGrowthGlobal1X128(self) -> int:
        prop_name = "feeGrowthGlobal1X128"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result
