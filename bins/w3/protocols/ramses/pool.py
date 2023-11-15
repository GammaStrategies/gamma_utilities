from copy import deepcopy
import logging
from hexbytes import HexBytes
from web3 import Web3

from bins.w3.protocols.general import erc20_cached

from ....cache import cache_utilities
from ....errors.general import ProcessingError
from ....formulas.position import get_positionKey_ramses
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import uniswap

ABI_FILENAME = "RamsesV2Pool"
ABI_FOLDERNAME = "ramses"
DEX_NAME = Protocol.RAMSES.database_name
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


class pool(uniswap.pool.poolv3):
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

    def boostInfos(self, period: int):
        """

        Returns:
            totalBoostAmount uint128, totalVeRamAmount int128
        """
        return self.call_function_autoRpc("boostInfos", None, period)

    def boostInfos_2(self, period: int, key: str) -> dict | None:
        """Get the boost information for a specific position at a period
                boostAmount the amount of boost this position has for this period,
                veRamAmount the amount of veRam attached to this position for this period,
                secondsDebtX96 used to account for changes in the deposit amount during the period
                boostedSecondsDebtX96 used to account for changes in the boostAmount and veRam locked during the period,
        Returns:
            boostAmount uint128, veRamAmount int128, secondsDebtX96 int256, boostedSecondsDebtX96 int256
        """
        if tmp := self.call_function_autoRpc("boostInfos", None, period, key):
            return {
                "boostAmount": tmp[0],
                "veRamAmount": tmp[1],
                "secondsDebtX96": tmp[2],
                "boostedSecondsDebtX96": tmp[3],
            }
        return

    # RAMSES uses a mutable fee that we will map to fee for our internal coherence but reality is
    # that this fee property is mapped to currentFee prop. in RAMSES.
    # so:
    # in RAMSES contract terms: "currentFee" is the fee charged by the pool for swaps and liquidity provision and "fee" is the initial fee used for mapping tickspacing
    # in our terms: "fee" is the fee charged by the pool for swaps and liquidity provision and "initialFee" is the initial fee used for mapping tickspacing
    @property
    def fee(self) -> int:
        """Returns the fee charged by the pool for swaps and liquidity provision ( can change over time)
        Returns uint24
        """
        return self.currentFee

    @property
    def currentFee(self) -> int:
        """Returns the fee charged by the pool for swaps and liquidity provision ( can change over time)
        Returns uint24
        """
        return self.call_function_autoRpc("currentFee")

    @property
    def initialFee(self) -> int:
        """The pool's initial fee in hundredths of a bip, i.e. 1e-6
           Ramses original inmutable fee made at pool creation ( used for mapping tickspacing )
        Returns uint24
        """
        return self.call_function_autoRpc("fee")

    @property
    def boostedLiquidity(self) -> int:
        return self.call_function_autoRpc("boostedLiquidity")

    @property
    def lastPeriod(self) -> int:
        return self.call_function_autoRpc("lastPeriod")

    @property
    def nfpManager(self) -> str:
        return self.call_function_autoRpc("nfpManager")

    @property
    def protocolFees(self) -> tuple[int, int]:
        """Fees collected by the protocol
        Returns:
            token0 uint128, token1 uint128
        """
        return self.call_function_autoRpc("protocolFees")

    def periodCumulativesInside(self, period: int, tickLower: int, tickUpper: int):
        """
        Returns:
            secondsPerLiquidityInsideX128 uint160, secondsPerBoostedLiquidityInsideX128 uint160
        """
        return self.call_function_autoRpc(
            "periodCumulativesInside", None, period, tickLower, tickUpper
        )

    def periods(self, period: int):
        """
        Returns:
            previousPeriod uint32, startTick int24, lastTick int24, endSecondsPerLiquidityPeriodX128 uint160, endSecondsPerBoostedLiquidityPeriodX128 uint160, boostedInRange uint32
        """
        return self.call_function_autoRpc("periods", None, period)

    def positionPeriodDebt(
        self, period: int, owner: str, index: int, tickLower: int, tickUpper: int
    ):
        """
        Returns:
            secondsDebtX96 int256, boostedSecondsDebtX96 int256
        """
        return self.call_function_autoRpc(
            "positionPeriodDebt",
            None,
            period,
            Web3.toChecksumAddress(owner),
            index,
            tickLower,
            tickUpper,
        )

    def positionPeriodSecondsInRange(
        self, period: int, owner: str, index: int, tickLower: int, tickUpper: int
    ):
        """
        Returns:
            periodSecondsInsideX96 uint256, periodBoostedSecondsInsideX96 uint256
        """
        return self.call_function_autoRpc(
            "positionPeriodSecondsInRange",
            None,
            period,
            Web3.toChecksumAddress(owner),
            index,
            tickLower,
            tickUpper,
        )

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
                   attachedVeRamId uint256
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
                "attachedVeRamId": result[5],
            }
        else:
            raise ValueError(f" positions function call returned None")

    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0
                       liquidityNet   int128 :  0
                       boostedLiquidityGross   uint128 :  0
                       boostedLiquidityNet   int128 :  0
                       feeGrowthOutside0X128   uint256 :  0
                       feeGrowthOutside1X128   uint256 :  0
                       tickCumulativeOutside   int56 :  0
                       secondsPerLiquidityOutsideX128   uint160 :  0
                       secondsOutside   uint32 :  0
                       initialized   bool :  false
        """
        if result := self.call_function_autoRpc("ticks", None, tick):
            return {
                "liquidityGross": result[0],
                "liquidityNet": result[1],
                "boostedLiquidityGross": result[2],
                "boostedLiquidityNet": result[3],
                "feeGrowthOutside0X128": result[4],
                "feeGrowthOutside1X128": result[5],
                "tickCumulativeOutside": result[6],
                "secondsPerLiquidityOutsideX128": result[7],
                "secondsOutside": result[8],
                "initialized": result[9],
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
                message=f" (ramses pool) ticks function of {self.address} at block {self.block} returned none. (Check contract creation block)",
            )

    @property
    def veRam(self) -> str:
        return self.call_function_autoRpc("veRam")

    @property
    def voter(self) -> str:
        return self.call_function_autoRpc("voter")

    # CUSTOM FUNCTIONS

    def position(
        self, ownerAddress: str, tickLower: int, tickUpper: int, index: int = 0
    ) -> dict:
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
            get_positionKey_ramses(
                ownerAddress=ownerAddress,
                tickLower=tickLower,
                tickUpper=tickUpper,
                index=index,
            )
        )


class pool_cached(pool, uniswap.pool.poolv3_cached):
    # PROPERTIES
    @property
    def fee(self) -> int:
        """The pool's initial fee in hundredths of a bip, i.e. 1e-6
        Ramses original fee made at pool creation (used for mapping to ticks for all intents and purposes)
        Returns uint24
        """
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
    def currentFee(self) -> int:
        """The pool's current fee in hundredths of a bip, i.e. 1e-6
        returns the real fee that can change over time
        Returns uint24
        """
        prop_name = "currentFee"
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
    def boostedLiquidity(self) -> int:
        prop_name = "boostedLiquidity"
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
    def lastPeriod(self) -> int:
        prop_name = "lastPeriod"
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
    def nfpManager(self) -> str:
        prop_name = "nfpManager"
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
    def veRam(self) -> str:
        prop_name = "veRam"
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
    def voter(self) -> str:
        prop_name = "voter"
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
    def protocolFees(self) -> tuple[int, int]:
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
    def token0(self) -> erc20_cached:
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
    def token1(self) -> erc20_cached:
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


class pool_multicall(uniswap.pool.poolv3_multicall):
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

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def fee(self) -> int:
        """Return currentFee cause RAMSES uses a mutable fee that we will map to fee for our internal coherence but reality is
        that this fee property is mapped to currentFee prop. in RAMSES.
        """
        return self._currentFee

    @property
    def currentFee(self) -> int:
        """The pool's current fee in hundredths of a bip, i.e. 1e-6
        returns the real fee that can change over time
        Returns uint24
        """
        return self._currentFee

    @property
    def boostedLiquidity(self) -> int:
        return self._boostedLiquidity

    @property
    def lastPeriod(self) -> int:
        return self._lastPeriod

    @property
    def nfpManager(self) -> str:
        return self._nfpManager

    @property
    def veRam(self) -> str:
        return self._veRam

    @property
    def voter(self) -> str:
        return self._voter

    @property
    def protocolFees(self) -> tuple[int, int]:
        return deepcopy(self._protocolFees)

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
    #                 "boostedLiquidityGross": result[2],
    #                 "boostedLiquidityNet": result[3],
    #                 "feeGrowthOutside0X128": result[4],
    #                 "feeGrowthOutside1X128": result[5],
    #                 "tickCumulativeOutside": result[6],
    #                 "secondsPerLiquidityOutsideX128": result[7],
    #                 "secondsOutside": result[8],
    #                 "initialized": result[9],
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
    #                 "attachedVeRamId": result[5],
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

    def _fill_from_known_data(self, known_data: dict):
        self._currentFee = known_data["currentFee"]
        self._boostedLiquidity = known_data["boostedLiquidity"]
        self._lastPeriod = known_data["lastPeriod"]
        self._nfpManager = known_data["nfpManager"]
        self._veRam = known_data["veRam"]
        self._voter = known_data["voter"]
        self._protocolFees = known_data["protocolFees"]

        super()._fill_from_known_data(known_data=known_data)
