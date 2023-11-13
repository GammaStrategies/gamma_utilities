from copy import deepcopy
from decimal import Decimal
import logging
from web3 import Web3
from hexbytes import HexBytes
from eth_abi import abi

from bins.general import file_utilities
from bins.w3.helpers.multicaller import execute_multicall

from ....errors.general import ProcessingError
from ..multicall import multicall3

from ....config.current import WEB3_CHAIN_IDS  # ,CFG
from ....cache import cache_utilities
from ....general.enums import Protocol, error_identity, text_to_chain
from ....formulas.full_math import mulDiv
from ....formulas.position import (
    get_positionKey,
    get_positionKey_algebra,
    get_positionKey_ramses,
)
from ....formulas.liquidity_math import getAmountsForLiquidity
from ....formulas.tick_math import getSqrtRatioAtTick
from ....formulas.fees import feeGrowth_to_fee, fees_uncollected_inRange
from ..base_wrapper import web3wrap
from ..general import (
    bep20,
    erc20,
    erc20_cached,
    bep20_cached,
    erc20_multicall,
    bep20_multicall,
)


# UNISWAP POOL
ABI_FILENAME = "univ3_pool"
ABI_FOLDERNAME = "uniswap/v3"
DEX_NAME = Protocol.UNISWAPv3.database_name
INMUTABLE_FIELDS = {
    "symbol": False,
    "fee": False,
    "decimals": True,
    "factory": True,
    "token0": True,
    "token1": True,
    "maxLiquidityPerTick": True,
    "tickSpacing": True,
}


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
        """uniswapv3 inmutable fields by contract
            https://vscode.blockscan.com/optimism/0x2f449bd78a72b18f8758ac38c3ff8dcb094416f6
        Returns:
            dict[str, bool]:  field name: is inmutable?
        """
        return INMUTABLE_FIELDS

    # PROPERTIES
    @property
    def factory(self) -> str:
        return self.call_function_autoRpc("factory")

    @property
    def fee(self) -> int:
        """The pool's fee in hundredths of a bip, i.e. 1e-6"""
        return self.call_function_autoRpc("fee")

    @property
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
        return self.call_function_autoRpc("feeGrowthGlobal0X128")

    @property
    def feeGrowthGlobal1X128(self) -> int:
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
        return self.call_function_autoRpc("feeGrowthGlobal1X128")

    @property
    def liquidity(self) -> int:
        """The currently in range liquidity available to the pool
            This value has no relationship to the total liquidity across all ticks

        Returns:
            int: _description_
        """
        return self.call_function_autoRpc("liquidity")

    @property
    def maxLiquidityPerTick(self) -> int:
        return self.call_function_autoRpc("maxLiquidityPerTick")

    def observations(self, input: int):
        return self.call_function_autoRpc("observations", None, input)

    def observe(self, secondsAgo: int):
        """observe _summary_

        Args:
           secondsAgo (int): _description_

        Returns:
           _type_: tickCumulatives   int56[] :  12731930095582
                   secondsPerLiquidityCumulativeX128s   uint160[] :  242821134689165142944235398318169

        """
        return self.call_function_autoRpc("observe", None, secondsAgo)

    def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        position_key = (
            HexBytes(position_key) if type(position_key) == str else position_key
        )
        if result := self.call_function_autoRpc("positions", None, position_key):
            return {
                "liquidity": result[0],
                "feeGrowthInside0LastX128": result[1],
                "feeGrowthInside1LastX128": result[2],
                "tokensOwed0": result[3],
                "tokensOwed1": result[4],
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
    def protocolFees(self) -> list[int]:
        """The amounts of token0 and token1 that are owed to the protocol
            Protocol fees will never exceed uint128 max in either token
        Returns:
           list: [0,0]

        """
        return self.call_function_autoRpc("protocolFees")

    @property
    def slot0(self) -> dict:
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  the current protocol fee as a percentage of the swap fee taken on withdrawal represented as an integer denominator (1/x)%
                   unlocked   bool :  true
        """
        if tmp := self.call_function_autoRpc("slot0"):
            return {
                "sqrtPriceX96": tmp[0],
                "tick": tmp[1],
                "observationIndex": tmp[2],
                "observationCardinality": tmp[3],
                "observationCardinalityNext": tmp[4],
                "feeProtocol": tmp[5],
                "unlocked": tmp[6],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "pool_address": self.address,
                    "block": self.block,
                    "object": "pool.slot0",
                },
                identity=error_identity.RETURN_NONE,
                action="",
                message=f" slot0 of {self.address} at block {self.block} returned none. (Check contract creation block)",
            )

    def snapshotCumulativeInside(self, tickLower: int, tickUpper: int):
        return self.call_function_autoRpc(
            "snapshotCumulativeInside", None, tickLower, tickUpper
        )

    def tickBitmap(self, input: int) -> int:
        return self.call_function_autoRpc("tickBitmap", None, input)

    @property
    def tickSpacing(self) -> int:
        return self.call_function_autoRpc("tickSpacing")

    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0
                       liquidityNet   int128 :  0
                       feeGrowthOutside0X128   uint256 :  0
                       feeGrowthOutside1X128   uint256 :  0
                       tickCumulativeOutside   int56 :  0
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0
                       secondsOutside   uint32 :  0
                       initialized   bool :  false
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

    @property
    def token0(self) -> erc20:
        """The first of the two tokens of the pool, sorted by address

        Returns:
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
        """The second of the two tokens of the pool, sorted by address_

        Returns:
           erc20:
        """
        if self._token1 is None:
            self._token1 = erc20(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1

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
        """ """
        self._custom_rpcType = value
        self.token0.custom_rpcType = value
        self.token1.custom_rpcType = value

    @property
    def sqrtPriceX96(self) -> int:
        """get the sqrtPriceX96 value"""
        return self.slot0["sqrtPriceX96"]

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
            get_positionKey(
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
        slot0 = self.slot0

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
            protocolFee (int)  gamma protocol fee percentage ( 0-100)
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
        tickCurrent = self.slot0["tick"]
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
        """feeGrowthInside_Last * position liquidity:  Retrieve the quantity of fees collected by the currently deployed position"""

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
        """Retrieve the quantity of fees collected by all current LPs of the pool.
        This is not all fees all time the pool has accrued but only all time fees collected by current LPs
        (It will decrease when big withdrawals occur.)"""

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
            convert_bint (bool, optional): convert big integers to string . Defaults to False.
            static_mode (bool, optional): return only static pool parameters. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint, minimal=minimal)

        # result["factory"] = self.factory
        result["fee"] = self.fee

        if not minimal:
            # t spacing
            result["tickSpacing"] = (
                str(self.tickSpacing) if convert_bint else self.tickSpacing
            )
            # tokens
            result["token0"] = self.token0.as_dict(
                convert_bint=convert_bint, minimal=minimal
            )
            result["token1"] = self.token1.as_dict(
                convert_bint=convert_bint, minimal=minimal
            )

        # protocolFees
        result["protocolFees"] = self.protocolFees
        if convert_bint and result["protocolFees"]:
            result["protocolFees"] = [str(i) for i in result["protocolFees"]]

        # identify pool dex
        result["dex"] = self.identify_dex_name()

        if not static_mode:
            self._as_dict_not_static_items(convert_bint, result, minimal)
        return result

    def _as_dict_not_static_items(self, convert_bint, result, minimal: bool = False):
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

        result["liquidity"] = str(self.liquidity) if convert_bint else self.liquidity

        # slot0
        result["slot0"] = self.slot0
        if convert_bint:
            result["slot0"]["sqrtPriceX96"] = str(result["slot0"]["sqrtPriceX96"])
            result["slot0"]["tick"] = str(result["slot0"]["tick"])
            result["slot0"]["observationIndex"] = str(
                result["slot0"]["observationIndex"]
            )
            result["slot0"]["observationCardinality"] = str(
                result["slot0"]["observationCardinality"]
            )
            result["slot0"]["observationCardinalityNext"] = str(
                result["slot0"]["observationCardinalityNext"]
            )

        # not minimal
        if not minimal:
            result["maxLiquidityPerTick"] = (
                str(self.maxLiquidityPerTick)
                if convert_bint
                else self.maxLiquidityPerTick
            )

    # multicall
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

        # build fixed objects
        self._build_fixed_objects_from_multicall_result(multicall_result=result)
        # modify result dict with output variables
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
            multicall_result["slot0"] = {
                "sqrtPriceX96": multicall_result["slot0"][0],
                "tick": multicall_result["slot0"][1],
                "observationIndex": multicall_result["slot0"][2],
                "observationCardinality": multicall_result["slot0"][3],
                "observationCardinalityNext": multicall_result["slot0"][4],
                "feeProtocol": multicall_result["slot0"][5],
                "unlocked": multicall_result["slot0"][6],
            }
        except:
            pass

        return multicall_result


class poolv3_cached(poolv3):
    SAVE2FILE = True

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        # define fixed fields
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
    def fee(self) -> int:
        """The pool's fee in hundredths of a bip, i.e. 1e-6"""
        prop_name = "fee"
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
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
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
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
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
    def protocolFees(self) -> list:
        """The amounts of token0 and token1 that are owed to the protocol

        Returns:
           list: token0   uint128 :  0, token1   uint128 :  0
        """
        prop_name = "protocolFees"
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
    def slot0(self) -> dict:
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  0
                   unlocked   bool :  true
        """
        prop_name = "slot0"
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
    def token0(self) -> erc20:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 is None:
            # check if token0 is cached
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
            # create token0 object with cached address
            self._token0 = erc20_cached(
                address=result,  # self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        """The second of the two tokens of the pool, sorted by address_

        Returns:
           erc20:
        """
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


class poolv3_multicall(poolv3):
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
        known_data: dict | None = None,
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

    # PROPERTIES
    @property
    def factory(self) -> str:
        return self._factory

    @property
    def fee(self) -> int:
        return self._fee

    @property
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
        return self._feeGrowthGlobal0X128

    @property
    def feeGrowthGlobal1X128(self) -> int:
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
        return self._feeGrowthGlobal1X128

    @property
    def liquidity(self) -> int:
        return self._liquidity

    @property
    def maxLiquidityPerTick(self) -> int:
        return self._maxLiquidityPerTick

    @property
    def protocolFees(self) -> list:
        """The amounts of token0 and token1 that are owed to the protocol

        Returns:
           list: token0   uint128 :  0, token1   uint128 :  0
        """
        return deepcopy(self._protocolFees)

    @property
    def slot0(self) -> dict:
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  0
                   unlocked   bool :  true
        """
        return deepcopy(self._slot0)

    @property
    def tickSpacing(self) -> int:
        return self._tickSpacing

    @property
    def token0(self) -> erc20_multicall:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        return self._token0

    @property
    def token1(self) -> erc20_multicall:
        """The second of the two tokens of the pool, sorted by address_

        Returns:
           erc20:
        """
        return self._token1

    # CUSTOM
    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0
                       liquidityNet   int128 :  0
                       feeGrowthOutside0X128   uint256 :  0
                       feeGrowthOutside1X128   uint256 :  0
                       tickCumulativeOutside   int56 :  0
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0
                       secondsOutside   uint32 :  0
                       initialized   bool :  false
        """
        if not tick in self._ticks:
            if result := self.call_function_autoRpc("ticks", None, tick):
                self._ticks[tick] = {
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
        return deepcopy(self._ticks[tick])

    def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """

        if not position_key in self._positions:
            position_key = (
                HexBytes(position_key) if type(position_key) == str else position_key
            )
            if result := self.call_function_autoRpc("positions", None, position_key):
                self._positions[position_key] = {
                    "liquidity": result[0],
                    "feeGrowthInside0LastX128": result[1],
                    "feeGrowthInside1LastX128": result[2],
                    "tokensOwed0": result[3],
                    "tokensOwed1": result[4],
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

        return deepcopy(self._positions[position_key])

    # FILL PROPS

    def fill_with_multicall(
        self,
        pool_address: str | None = None,
        token0_address: str | None = None,
        token1_address: str | None = None,
    ):
        # POOL
        pool_abi_filename = ABI_FILENAME
        pool_abi_path = f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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
        self._fee = known_data["fee"]
        self._feeGrowthGlobal0X128 = known_data["feeGrowthGlobal0X128"]
        self._feeGrowthGlobal1X128 = known_data["feeGrowthGlobal1X128"]
        self._liquidity = known_data["liquidity"]
        self._maxLiquidityPerTick = known_data["maxLiquidityPerTick"]
        self._protocolFees = known_data["protocolFees"]
        self._tickSpacing = known_data["tickSpacing"]

        self._slot0 = {
            "sqrtPriceX96": known_data["slot0"][0],
            "tick": known_data["slot0"][1],
            "observationIndex": known_data["slot0"][2],
            "observationCardinality": known_data["slot0"][3],
            "observationCardinalityNext": known_data["slot0"][4],
            "feeProtocol": known_data["slot0"][5],
            "unlocked": known_data["slot0"][6],
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

    # def _create_call_position(self, ownerAddress, tickLower, tickUpper) -> dict:
    #     _position_key = get_positionKey(
    #         ownerAddress=ownerAddress,
    #         tickLower=tickLower,
    #         tickUpper=tickUpper,
    #     )

    #     _position_key = (
    #         HexBytes(_position_key) if type(_position_key) == str else _position_key
    #     )

    #     return {
    #         "inputs": [_position_key],
    #         "name": "positions",
    #         "outputs": [
    #             {"internalType": "uint128", "name": "liquidity", "type": "uint128"},
    #             {
    #                 "internalType": "uint256",
    #                 "name": "feeGrowthInside0LastX128",
    #                 "type": "uint256",
    #             },
    #             {
    #                 "internalType": "uint256",
    #                 "name": "feeGrowthInside1LastX128",
    #                 "type": "uint256",
    #             },
    #             {
    #                 "internalType": "uint128",
    #                 "name": "tokensOwed0",
    #                 "type": "uint128",
    #             },
    #             {
    #                 "internalType": "uint128",
    #                 "name": "tokensOwed1",
    #                 "type": "uint128",
    #             },
    #         ],
    #         "stateMutability": "view",
    #         "type": "function",
    #         "address": self._address,
    #         "object": "pool",
    #     }

    # def _create_call_ticks(self, tick: int) -> dict:
    #     return {
    #         "inputs": [tick],
    #         "name": "ticks",
    #         "outputs": [
    #             {
    #                 "internalType": "uint128",
    #                 "name": "liquidityGross",
    #                 "type": "uint128",
    #             },
    #             {"internalType": "int128", "name": "liquidityNet", "type": "int128"},
    #             {
    #                 "internalType": "uint256",
    #                 "name": "feeGrowthOutside0X128",
    #                 "type": "uint256",
    #             },
    #             {
    #                 "internalType": "uint256",
    #                 "name": "feeGrowthOutside1X128",
    #                 "type": "uint256",
    #             },
    #             {
    #                 "internalType": "int56",
    #                 "name": "tickCumulativeOutside",
    #                 "type": "int56",
    #             },
    #             {
    #                 "internalType": "uint160",
    #                 "name": "secondsPerLiquidityOutsideX128",
    #                 "type": "uint160",
    #             },
    #             {"internalType": "uint32", "name": "secondsOutside", "type": "uint32"},
    #             {"internalType": "bool", "name": "initialized", "type": "bool"},
    #         ],
    #         "stateMutability": "view",
    #         "type": "function",
    #         "address": self._address,
    #         "object": "pool",
    #     }


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

    # PROPERTIES
    @property
    def token0(self) -> bep20:
        """The first of the two tokens of the pool, sorted by address

        Returns: bep20
        """
        if self._token0 is None:
            self._token0 = bep20(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20:
        """The second of the two tokens of the pool, sorted by address_

        Returns: bep20
        """
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

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        # define fixed fields
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
    def fee(self) -> int:
        """The pool's fee in hundredths of a bip, i.e. 1e-6"""
        prop_name = "fee"
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
    def feeGrowthGlobal0X128(self) -> int:
        """The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token0
        """
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
        """The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool
        Returns:
           int: as Q128.128 fees of token1
        """
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
    def protocolFees(self) -> list:
        """The amounts of token0 and token1 that are owed to the protocol

        Returns:
           list: token0   uint128 :  0, token1   uint128 :  0
        """
        prop_name = "protocolFees"
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
    def slot0(self) -> dict:
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  0
                   unlocked   bool :  true
        """
        prop_name = "slot0"
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
    def token0(self) -> bep20:
        """The first of the two tokens of the pool, sorted by address

        Returns: bep20
        """
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

            self._token0 = bep20_cached(
                address=result,  # self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20:
        """The second of the two tokens of the pool, sorted by address_

        Returns: bep20
        """
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
            # create object with cached address
            self._token1 = bep20_cached(
                address=result,  # self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1


class poolv3_bep20_multicall(poolv3_multicall):
    @property
    def token0(self) -> bep20_multicall:
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        return self._token0

    @property
    def token1(self) -> bep20_multicall:
        """The second of the two tokens of the pool, sorted by address_

        Returns:
           erc20:
        """
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
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )
