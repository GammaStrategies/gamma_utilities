from web3 import Web3
from bins.general.enums import Protocol
from bins.w3.protocols import uniswap


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
        self._abi_path = abi_path or "data/abi/ramses"

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

    def boostInfos_2(self, period: int, key: str):
        """

        Returns:
            boostAmount uint128, veRamAmount int128, secondsDebtX96 int256, boostedSecondsDebtX96 int256
        """
        return self.call_function_autoRpc("boostInfos", None, period, key)

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
            "positionPeriodDebt", None, period, owner, index, tickLower, tickUpper
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
            owner,
            index,
            tickLower,
            tickUpper,
        )

    @property
    def veRam(self) -> str:
        return self.call_function_autoRpc("veRam")

    @property
    def voter(self) -> str:
        return self.call_function_autoRpc("voter")


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
        self._abi_path = abi_path or "data/abi/ramses"

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
