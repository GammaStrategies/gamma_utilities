from hexbytes import HexBytes
from web3 import Web3

from bins.errors.general import ProcessingError
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import algebra
from ..general import erc20_cached


ABI_FILENAME = "spiritswap_pool"
ABI_FOLDERNAME = "spiritswap"
DEX_NAME = Protocol.SPIRITSWAP.database_name


class pool(algebra.pool.poolv3):
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

    @property
    def communityVault(self) -> str:
        """The contract to which community fees are transferred

        Returns:
            str: The communityVault address
        """
        # TODO: communityVault object
        return self.call_function_autoRpc("communityVault")

    @property
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault
        Will be sent COMMUNITY_FEE_TRANSFER_FREQUENCY after communityFeeLastTimestamp
        Returns:
            tuple[int,int]: communityFeePending0,communityFeePending1
        """
        return self.call_function_autoRpc("getCommunityFeePending")

    @property
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves
            If at any time the real balance is larger, the excess will be transferred to liquidity providers as additional fee.
            If the balance exceeds uint128, the excess will be sent to the communityVault.
        Returns:
            tuple[int,int]: reserve0,reserve1
        """
        return self.call_function_autoRpc("getReserves")

    @property
    def globalState(self) -> dict:
        """The globalState structure in the pool stores many values but requires only one slot
            and is exposed as a single method to save gas when accessed externally.

        Returns:
           dict:    uint160 price; The current price of the pool as a sqrt(dToken1/dToken0) Q64.96 value
                    int24 tick; The current tick of the pool, i.e. according to the last tick transition that was run.
                                This value may not always be equal to SqrtTickMath.getTickAtSqrtRatio(price) if the price is on a tick boundary;
                    int24 prevInitializedTick; The previous initialized tick
                    uint16 fee; The last pool fee value in hundredths of a bip, i.e. 1e-6
                    uint16 timepointIndex; The index of the last written timepoint
                    uint8 communityFee; The community fee percentage of the swap fee in thousandths (1e-3)
                    bool unlocked; Whether the pool is currently locked to reentrancy
        """
        if tmp := self.call_function_autoRpc("globalState"):
            return {
                "sqrtPriceX96": tmp[0],
                "tick": tmp[1],
                "prevInitializedTick": tmp[2],
                "fee": tmp[3],
                "timepointIndex": tmp[4],
                "communityFeeToken0": tmp[5],
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

    def limitOrders(self, tick: int) -> dict:
        """Returns the summary information about a limit orders at tick

        Args:
            tick (int): The tick to look up

        Returns:
            dict:   amountToSell: The amount of tokens to sell. Has only relative meaning
                    soldAmount: The amount of tokens already sold. Has only relative meaning
                    boughtAmount0Cumulative: The accumulator of bought tokens0 per amountToSell. Has only relative meaning
                    boughtAmount1Cumulative: The accumulator of bought tokens1 per amountToSell. Has only relative meaning
                    initialized: Will be true if a limit order was created at least once on this tick
        """
        if tmp := self.call_function_autoRpc("limitOrders", None, tick):
            return {
                "amountToSell": tmp[0],
                "soldAmount": tmp[1],
                "boughtAmount0Cumulative": tmp[2],
                "boughtAmount1Cumulative": tmp[3],
                "initialized": tmp[4],
            }
        else:
            raise ValueError(f" globalState function call returned None")

    @property
    def secondsPerLiquidityCumulative(self) -> int:
        """The accumulator of seconds per liquidity since the pool was first initialized"""
        return self.call_function_autoRpc("secondsPerLiquidityCumulative")

    @property
    def tickSpacingLimitOrders(self) -> int:
        """The current tick spacing for limit orders
        Ticks can only be used for limit orders at multiples of this value
        This value is an int24 to avoid casting even though it is always positive.
        """
        return self.call_function_autoRpc("tickSpacingLimitOrders")

    @property
    def communityFeeLastTimestamp(self) -> int:
        """The timestamp of the last sending of tokens to community vault"""
        return self.call_function_autoRpc("communityFeeLastTimestamp")

    def getTimepoints(self, secondsAgo: int):
        """Returns the accumulator values as of each time seconds ago from the given time in the array of `secondsAgos`
        Reverts if `secondsAgos` > oldest timepoint
        Args:
            secondsAgo (int): Each amount of time to look back, in seconds, at which point to return a timepoint

        Returns:
            : tickCumulatives The cumulative tick since the pool was first initialized, as of each `secondsAgo`
            int56[] memory tickCumulatives, uint112[] memory volatilityCumulatives
        """
        return self.call_function_autoRpc("getTimepoints", None, secondsAgo)

    @property
    def liquidityCooldown(self) -> int:
        """
        Returns:
            int: 0 uint32
        """
        # does not implement liquidity cooldown
        return 0

    def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   innerFeeGrowth0Token
                   innerFeeGrowth1Token   uint256 :  (feeGrowthInside0LastX128)
                   fees0   uint128 :  0  (tokensOwed0)
                   fees1   uint128 :  0  ( tokensOwed1)
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
                "tokensOwed1": result[3],
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


class pool_cached(pool, algebra.pool.poolv3_cached):
    SAVE2FILE = True

    # PROPERTIES

    @property
    def communityVault(self) -> str:
        """The contract to which community fees are transferred

        Returns:
            str: The communityVault address
        """
        prop_name = "communityVault"
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
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault
        Will be sent COMMUNITY_FEE_TRANSFER_FREQUENCY after communityFeeLastTimestamp
        Returns:
            tuple[int,int]: communityFeePending0,communityFeePending1
        """
        prop_name = "getCommunityFeePending"
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
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves
            If at any time the real balance is larger, the excess will be transferred to liquidity providers as additional fee.
            If the balance exceeds uint128, the excess will be sent to the communityVault.
        Returns:
            tuple[int,int]: reserve0,reserve1
        """
        prop_name = "getReserves"
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
    def globalState(self) -> dict:
        """The globalState structure in the pool stores many values but requires only one slot
            and is exposed as a single method to save gas when accessed externally.

        Returns:
           dict:    uint160 price; The current price of the pool as a sqrt(dToken1/dToken0) Q64.96 value
                    int24 tick; The current tick of the pool, i.e. according to the last tick transition that was run.
                                This value may not always be equal to SqrtTickMath.getTickAtSqrtRatio(price) if the price is on a tick boundary;
                    int24 prevInitializedTick; The previous initialized tick
                    uint16 fee; The last pool fee value in hundredths of a bip, i.e. 1e-6
                    uint16 timepointIndex; The index of the last written timepoint
                    uint8 communityFee; The community fee percentage of the swap fee in thousandths (1e-3)
                    bool unlocked; Whether the pool is currently locked to reentrancy
        """
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
    def secondsPerLiquidityCumulative(self) -> int:
        """The accumulator of seconds per liquidity since the pool was first initialized"""
        prop_name = "secondsPerLiquidityCumulative"
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
    def tickSpacingLimitOrders(self) -> int:
        """The current tick spacing for limit orders
        Ticks can only be used for limit orders at multiples of this value
        This value is an int24 to avoid casting even though it is always positive.
        """
        prop_name = "tickSpacingLimitOrders"
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
    def communityFeeLastTimestamp(self) -> int:
        """The timestamp of the last sending of tokens to community vault"""
        prop_name = "communityFeeLastTimestamp"
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


class pool_multicall(algebra.pool.poolv3_multicall):
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
    def communityVault(self) -> str:
        """The contract to which community fees are transferred

        Returns:
            str: The communityVault address
        """
        return self._communityVault

    @property
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault
        Will be sent COMMUNITY_FEE_TRANSFER_FREQUENCY after communityFeeLastTimestamp
        Returns:
            tuple[int,int]: communityFeePending0,communityFeePending1
        """
        return self._getCommunityFeePending

    @property
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves
            If at any time the real balance is larger, the excess will be transferred to liquidity providers as additional fee.
            If the balance exceeds uint128, the excess will be sent to the communityVault.
        Returns:
            tuple[int,int]: reserve0,reserve1
        """
        return self._getReserves

    @property
    def secondsPerLiquidityCumulative(self) -> int:
        """The accumulator of seconds per liquidity since the pool was first initialized"""
        return self._secondsPerLiquidityCumulative

    @property
    def tickSpacingLimitOrders(self) -> int:
        """The current tick spacing for limit orders
        Ticks can only be used for limit orders at multiples of this value
        This value is an int24 to avoid casting even though it is always positive.
        """
        return self._tickSpacingLimitOrders

    @property
    def communityFeeLastTimestamp(self) -> int:
        """The timestamp of the last sending of tokens to community vault"""
        return self._communityFeeLastTimestamp

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

        self._communityVault = known_data["communityVault"]
        self._getCommunityFeePending = known_data["getCommunityFeePending"]
        self._getReserves = known_data["etReserves"]
        self._secondsPerLiquidityCumulative = known_data[
            "secondsPerLiquidityCumulative"
        ]
        self._tickSpacingLimitOrders = known_data["tickSpacingLimitOrders"]
        self._communityFeeLastTimestamp = known_data["communityFeeLastTimestamp"]

        self._globalState = {
            "sqrtPriceX96": known_data["globalState"][0],
            "tick": known_data["globalState"][1],
            "prevInitializedTick": known_data["globalState"][2],
            "fee": known_data["globalState"][3],
            "timepointIndex": known_data["globalState"][4],
            "communityFeeToken0": known_data["globalState"][5],
            "communityFeeToken1": known_data["globalState"][5],
            "unlocked": known_data["globalState"][6],
        }

        self._fill_from_known_data_objects(known_data=known_data)
