from copy import deepcopy
import logging
import sys

from decimal import Decimal
from hexbytes import HexBytes
from web3 import Web3
from eth_abi import abi

from bins.general import file_utilities
from bins.w3.helpers.multicaller import execute_multicall
from ....errors.general import ProcessingError
from ..multicall import multicall3

from ....config.current import WEB3_CHAIN_IDS  # ,CFG
from ....formulas.full_math import mulDiv
from ....formulas.fees import feeGrowth_to_fee, fees_uncollected_inRange
from ....formulas.position import (
    get_positionKey_algebra,
    get_positionKey_ramses,
    get_positionKey,
)
from ....formulas.tick_math import getSqrtRatioAtTick
from ....formulas.liquidity_math import getAmountsForLiquidity
from ....general.enums import Protocol, error_identity, text_to_chain
from ..base_wrapper import web3wrap
from ..general import (
    bep20,
    bep20_cached,
    erc20,
    erc20_cached,
    erc20_multicall,
    bep20_multicall,
)
from ....cache import cache_utilities


# ALGEBRA POOL
ABI_FILENAME = "algebrav3pool"
ABI_FOLDERNAME = "algebra/v3"
DEX_NAME = Protocol.ALGEBRAv3.database_name
INMUTABLE_FIELDS = {
    "symbol": False,
    "fee": False,
    "decimals": True,
    "factory": True,
    "token0": True,
    "token1": True,
    "maxLiquidityPerTick": False,
    "tickSpacing": False,
}


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
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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
        return DEX_NAME

    def inmutable_fields(self) -> dict[str, bool]:
        """algebra inmutable fields by contract

        Returns:
            dict[str, bool]:  field name: is inmutable?
        """
        return INMUTABLE_FIELDS

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = self.inmutable_fields()

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
        """Returns the information about active incentive
        if there is no active incentive at the moment, incentiveAddress would be equal to address(0)

        Returns:
            str: incentiveAddress The address associated with the current active incentive
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
            get_positionKey_algebra(
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
        sqrtRatioAX96 = getSqrtRatioAtTick(tickLower)
        sqrtRatioBX96 = getSqrtRatioAtTick(tickUpper)
        # calc quantity from liquidity
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = getAmountsForLiquidity(
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

        result["qtty_token0"] = fees_uncollected_inRange(
            liquidity=pos["liquidity"],
            tick=tickCurrent,
            tickLower=tickLower,
            tickUpper=tickUpper,
            feeGrowthGlobal=self.feeGrowthGlobal0X128,
            feeGrowthOutsideLower=ticks_lower["feeGrowthOutside0X128"],
            feeGrowthOutsideUpper=ticks_upper["feeGrowthOutside0X128"],
            feeGrowthInsideLast=pos["feeGrowthInside0LastX128"],
        )
        result["qtty_token1"] = fees_uncollected_inRange(
            liquidity=pos["liquidity"],
            tick=tickCurrent,
            tickLower=tickLower,
            tickUpper=tickUpper,
            feeGrowthGlobal=self.feeGrowthGlobal1X128,
            feeGrowthOutsideLower=ticks_lower["feeGrowthOutside1X128"],
            feeGrowthOutsideUpper=ticks_upper["feeGrowthOutside1X128"],
            feeGrowthInsideLast=pos["feeGrowthInside1LastX128"],
        )

        # calculate LPs and Gamma fees
        result["gamma_qtty_token0"] = mulDiv(result["qtty_token0"], protocolFee, 100)
        result["gamma_qtty_token1"] = mulDiv(result["qtty_token1"], protocolFee, 100)
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
        self,
        ownerAddress: str,
        tickUpper: int,
        tickLower: int,
        protocolFee: int,
        inDecimal: bool = True,
    ) -> dict:
        """Retrieve the quantity of fees collected by the deployed position"""

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

        # convert the position known feeGrowth to fees
        result["qtty_token0"] = feeGrowth_to_fee(
            feeGrowthX128=pos["feeGrowthInside0LastX128"], liquidity=pos["liquidity"]
        )
        result["qtty_token1"] = feeGrowth_to_fee(
            feeGrowthX128=pos["feeGrowthInside1LastX128"], liquidity=pos["liquidity"]
        )

        # calculate LPs and Gamma fees
        result["gamma_qtty_token0"] = mulDiv(result["qtty_token0"], protocolFee, 100)
        result["gamma_qtty_token1"] = mulDiv(result["qtty_token1"], protocolFee, 100)
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

    def get_total_pool_fees(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees collected by all LPs"""

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
        }

        # convert the position known feeGrowth to fees
        result["qtty_token0"] = feeGrowth_to_fee(
            feeGrowthX128=self.feeGrowthGlobal0X128, liquidity=self.liquidity
        )
        result["qtty_token1"] = feeGrowth_to_fee(
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

    def as_dict(
        self, convert_bint=False, static_mode: bool = False, minimal: bool = False
    ) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): convert big integers to string. Defaults to False.
            static_mode (bool, optional): return  static fields only. Defaults to False.

        Returns:
            dict:
        """

        result = super().as_dict(convert_bint=convert_bint, minimal=minimal)

        if not minimal:
            try:
                result["liquidityCooldown"] = (
                    str(self.liquidityCooldown)
                    if convert_bint
                    else self.liquidityCooldown
                )
            except Exception as e:
                # some algebra contracts have no liquidityCooldown function
                pass

            result["maxLiquidityPerTick"] = (
                str(self.maxLiquidityPerTick)
                if convert_bint
                else self.maxLiquidityPerTick
            )

            # t spacing
            # result["tickSpacing"] = (
            #     self.tickSpacing if not convert_bint else str(self.tickSpacing)
            # )

            result["token0"] = self.token0.as_dict(
                convert_bint=convert_bint, minimal=minimal
            )
            result["token1"] = self.token1.as_dict(
                convert_bint=convert_bint, minimal=minimal
            )

        result["activeIncentive"] = self.activeIncentive

        # identify pool dex
        result["dex"] = self.identify_dex_name()

        # add fee so that it has same field as univ3 pool to dict
        result["fee"] = self.globalState["fee"]

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

    def _get_dict_from_multicall(self) -> dict:
        """Only this object and its super() will be returned as dict ( no pools nor objects within this)
             When any of the function is not returned ( for any reason ), its key will not appear in the result.
            All functions defined in the abi without "inputs" will be returned here
        Returns:
            dict:
        """
        # create multicall helper
        _multicall_helper = multicall3(network=self._network, block=self.block)
        # get data thru multicall contract
        multicall_result = _multicall_helper.try_get_data(
            contract_functions=self.contract_functions, address=self.address
        )
        # decode result
        result = {}
        for idx, _res_itm in enumerate(multicall_result):
            # first item returned is success bool
            if _res_itm[0]:
                # success

                data = abi.decode(
                    [out["type"] for out in self.contract_functions[idx]["outputs"]],
                    _res_itm[1],
                )

                # build result key = function name
                key = self.contract_functions[idx]["name"]

                # set var context
                if len(self.contract_functions[idx]["outputs"]) > 1:
                    if not [
                        1
                        for x in self.contract_functions[idx]["outputs"]
                        if x["name"] != ""
                    ]:
                        # dictionary
                        result[key] = {}
                    else:
                        # list
                        result[key] = []
                else:
                    # one item
                    result[key] = None

                # loop thru output
                for output_idx, output in enumerate(
                    self.contract_functions[idx]["outputs"]
                ):
                    # add to result
                    if isinstance(result[key], list):
                        result[key].append(data[output_idx])
                    elif isinstance(result[key], dict):
                        result[key][output["name"]] = data[output_idx]
                    else:
                        result[key] = data[output_idx]

        # build fixed objects like (token0, token1..)
        self._build_fixed_objects_from_multicall_result(multicall_result=result)
        # format list vars ( like globalState...)
        result = self._convert_formats_multicall_result(multicall_result=result)

        return result

    def _build_fixed_objects_from_multicall_result(self, multicall_result: dict):
        # build fixed objects like (token0, token1..)
        try:
            if self._token0 is None:
                self._token0 = erc20(
                    address=multicall_result["token0"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass
        try:
            if self._token1 is None:
                self._token1 = erc20(
                    address=multicall_result["token1"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass

    def _convert_formats_multicall_result(self, multicall_result: dict) -> dict:
        # format list vars ( like getTotalAmounts...)
        try:
            multicall_result["globalState"] = {
                "sqrtPriceX96": multicall_result["globalState"][0],
                "tick": multicall_result["globalState"][1],
                "fee": multicall_result["globalState"][2],
                "timepointIndex": multicall_result["globalState"][3],
                "communityFeeToken0": multicall_result["globalState"][4],
                "communityFeeToken1": multicall_result["globalState"][5],
                "unlocked": multicall_result["globalState"][6],
            }
        except:
            pass

        return multicall_result


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
    def dataStorageOperator(self) -> dataStorageOperator_cached:
        """ """
        if self._dataStorage is None:
            # check if cached
            prop_name = "dataStorageOperator"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._dataStorage = dataStorageOperator_cached(
                address=result,
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
            # check if token is cached
            prop_name = "token0"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            # create token object with cached address
            self._token0 = erc20_cached(
                address=result,  # self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20_cached:
        if self._token1 is None:
            # check if token is cached
            prop_name = "token1"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            # create token object with cached address
            self._token1 = erc20_cached(
                address=result,  # self.call_function_autoRpc("token1"),
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


class poolv3_multicall(poolv3):
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
        known_data: dict = None,
    ):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

        self._ticks = {}
        self._positions = {}

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

        if known_data:
            self._fill_from_known_data(known_data=known_data)

    @property
    def activeIncentive(self) -> str:
        return self._activeIncentive

    @property
    def dataStorageOperator(self) -> dataStorageOperator:
        """ """
        return self._dataStorageOperator

    @property
    def factory(self) -> str:
        return self._factory

    @property
    def globalState(self) -> dict:
        return deepcopy(self._globalState)

    @property
    def liquidity(self) -> int:
        return self._liquidity

    @property
    def liquidityCooldown(self) -> int:
        return self._liquidityCooldown

    @property
    def maxLiquidityPerTick(self) -> int:
        return self._maxLiquidityPerTick

    @property
    def tickSpacing(self) -> int:
        return self._tickSpacing

    @property
    def token0(self) -> erc20_multicall:
        return self._token0

    @property
    def token1(self) -> erc20_multicall:
        return self._token1

    @property
    def feeGrowthGlobal0X128(self) -> int:
        return self._feeGrowthGlobal0X128

    @property
    def feeGrowthGlobal1X128(self) -> int:
        return self._feeGrowthGlobal1X128

    # CUSTOM
    # def ticks(self, tick: int) -> dict:
    #     """

    #     Args:
    #        tick (int):

    #     Returns:
    #        _type_:     liquidityGross   uint128 :  0
    #                    liquidityNet   int128 :  0
    #                    feeGrowthOutside0X128   uint256 :  0
    #                    feeGrowthOutside1X128   uint256 :  0
    #                    tickCumulativeOutside   int56 :  0
    #                    spoolecondsPerLiquidityOutsideX128   uint160 :  0
    #                    secondsOutside   uint32 :  0
    #                    initialized   bool :  false
    #     """
    #     if not tick in self._ticks:
    #         if result := self.call_function_autoRpc("ticks", None, tick):
    #             self._ticks[tick] = {
    #                 "liquidityGross": result[0],
    #                 "liquidityNet": result[1],
    #                 "feeGrowthOutside0X128": result[2],
    #                 "feeGrowthOutside1X128": result[3],
    #                 "tickCumulativeOutside": result[4],
    #                 "secondsPerLiquidityOutsideX128": result[5],
    #                 "secondsOutside": result[6],
    #                 "initialized": result[7],
    #             }
    #         else:
    #             raise ProcessingError(
    #                 chain=text_to_chain(self._network),
    #                 item={
    #                     "pool_address": self.address,
    #                     "block": self.block,
    #                     "object": "pool.ticks",
    #                 },
    #                 identity=error_identity.RETURN_NONE,
    #                 action="",
    #                 message=f" ticks function of {self.address} at block {self.block} returned none. (Check contract creation block)",
    #             )
    #     return deepcopy(self._ticks[tick])

    # def positions(self, position_key: str) -> dict:
    #     """

    #     Args:
    #        position_key (str): 0x....

    #     Returns:
    #        _type_:
    #                liquidity   uint128 :  99225286851746
    #                feeGrowthInside0LastX128   uint256 :  0
    #                feeGrowthInside1LastX128   uint256 :  0
    #                tokensOwed0   uint128 :  0
    #                tokensOwed1   uint128 :  0
    #     """

    #     if not position_key in self._positions:
    #         position_key = (
    #             HexBytes(position_key) if type(position_key) == str else position_key
    #         )
    #         if result := self.call_function_autoRpc("positions", None, position_key):
    #             self._positions[position_key] = {
    #                 "liquidity": result[0],
    #                 "feeGrowthInside0LastX128": result[1],
    #                 "feeGrowthInside1LastX128": result[2],
    #                 "tokensOwed0": result[3],
    #                 "tokensOwed1": result[4],
    #             }
    #         else:
    #             raise ProcessingError(
    #                 chain=text_to_chain(self._network),
    #                 item={
    #                     "pool_address": self.address,
    #                     "block": self.block,
    #                     "object": "pool.positions",
    #                 },
    #                 identity=error_identity.RETURN_NONE,
    #                 action="",
    #                 message=f" positions function of {self.address} at block {self.block} returned none using {position_key} as position_key",
    #             )

    #     return deepcopy(self._positions[position_key])

    def fill_with_multicall(
        self,
        pool_address: str | None = None,
        token0_address: str | None = None,
        token1_address: str | None = None,
    ):
        # POOL
        pool_abi_filename = self._abi_filename
        pool_abi_path = self._abi_path

        data = execute_multicall(
            network=self._network,
            block=self.block,
            hypervisor_address=self._address,
            pool_address=pool_address,
            token0_address=token0_address,
            token1_address=token1_address,
            hypervisor_abi_filename=None,
            hypervisor_abi_path=None,
            pool_abi_filename=pool_abi_filename,
            pool_abi_path=pool_abi_path,
            convert_bint=False,
        )

        self._fill_from_known_data(known_data=data)

    def _fill_from_known_data(self, known_data: dict):
        self._factory = known_data["factory"]
        self._feeGrowthGlobal0X128 = known_data["totalFeeGrowth0Token"]
        self._feeGrowthGlobal1X128 = known_data["totalFeeGrowth1Token"]
        self._liquidity = known_data["liquidity"]
        self._maxLiquidityPerTick = known_data["maxLiquidityPerTick"]
        # self._protocolFees = known_data["protocolFees"]
        self._tickSpacing = known_data["tickSpacing"]
        self._activeIncentive = known_data["activeIncentive"]
        self._liquidityCooldown = known_data["liquidityCooldown"]
        self._globalState = {
            "sqrtPriceX96": known_data["globalState"][0],
            "tick": known_data["globalState"][1],
            "fee": known_data["globalState"][2],
            "timepointIndex": known_data["globalState"][3],
            "communityFeeToken0": known_data["globalState"][4],
            "communityFeeToken1": known_data["globalState"][5],
            "unlocked": known_data["globalState"][6],
        }

        self._fill_from_known_data_objects(known_data=known_data)

    def _fill_from_known_data_objects(self, known_data: dict):
        self._token0 = erc20_multicall(
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token0"],
        )
        self._token1 = erc20_multicall(
            address=known_data["token1"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )

        self._dataStorageOperator = dataStorageOperator(
            address=known_data["dataStorageOperator"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
        )


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
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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

    def _build_fixed_objects_from_multicall_result(self, multicall_result: dict):
        # build fixed objects like (token0, token1..)
        try:
            if self._token0 is None:
                self._token0 = bep20(
                    address=multicall_result["token0"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass
        try:
            if self._token1 is None:
                self._token1 = bep20(
                    address=multicall_result["token1"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass


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
    def dataStorageOperator(self) -> dataStorageOperator_cached:
        """ """
        if self._dataStorage is None:
            # check if cached
            prop_name = "dataStorageOperator"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._dataStorage = dataStorageOperator_cached(
                address=result,
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
            # check if token is cached
            prop_name = "token0"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            # create token object with cached address
            self._token0 = bep20_cached(
                address=result,  # self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20_cached:
        if self._token1 is None:
            # check if token is cached
            prop_name = "token1"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            # create token object with cached address
            self._token1 = bep20_cached(
                address=result,  # self.call_function_autoRpc("token1"),
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


class poolv3_bep20_multicall(poolv3_multicall):
    @property
    def token0(self) -> bep20_multicall:
        return self._token0

    @property
    def token1(self) -> bep20_multicall:
        return self._token1

    def _fill_from_known_data_objects(self, known_data: dict):
        self._token0 = bep20_multicall(
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token0"],
        )
        self._token1 = bep20_multicall(
            address=known_data["token1"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )

        self._dataStorageOperator = dataStorageOperator(
            address=known_data["dataStorageOperator"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
        )
