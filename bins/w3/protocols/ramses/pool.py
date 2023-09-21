from hexbytes import HexBytes
from web3 import Web3

from bins.errors.general import ProcessingError
from ....formulas.position import get_positionKey_ramses
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import uniswap


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
        self._abi_filename = abi_filename or "RamsesV2Pool"
        self._abi_path = abi_path or f"{self.abi_root_path}/ramses"

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
        return Protocol.RAMSES.database_name

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

    @property
    def boostedLiquidity(self) -> int:
        return self.call_function_autoRpc("boostedLiquidity")

    @property
    def lastPeriod(self) -> int:
        return self.call_function_autoRpc("lastPeriod")

    @property
    def nfpManager(self) -> str:
        return self.call_function_autoRpc("nfpManager")

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


class pool_cached(uniswap.pool.poolv3_cached):
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
        self._abi_filename = abi_filename or "RamsesV2Pool"
        self._abi_path = abi_path or f"{self.abi_root_path}/ramses"

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
        return Protocol.RAMSES.database_name

    def boostInfos(self, period: int):
        """

        Returns:
            totalBoostAmount uint128, totalVeRamAmount int128
        """
        return self.call_function_autoRpc("boostInfos", None, period)

    def boostInfos_2(self, period: int, key: str):
        """

        Returns:
            boostAmount uint128, veRamAmount int128, secondsDebtX96 int256, boostedSecondsDebtX96 int256
        """
        return self.call_function_autoRpc("boostInfos", None, period, key)

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

    # PROPERTIES
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

    # CUSTOM FUNCTIONS

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
            get_positionKey_ramses(
                ownerAddress=ownerAddress,
                tickLower=tickLower,
                tickUpper=tickUpper,
            )
        )
