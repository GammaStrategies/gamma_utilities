import logging
from hexbytes import HexBytes
from web3 import Web3
from bins.config.hardcodes import SPECIAL_POOL_ABIS

from bins.errors.general import ProcessingError
from bins.formulas.position import get_positionKey_algebra
from bins.w3.protocols.algebra.pool import dataStorageOperator
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import algebra
from ..general import erc20_cached, erc20_multicall


ABI_FILENAME = "spiritswap_pool"
ABI_FOLDERNAME = "spiritswap"
DEX_NAME = Protocol.SPIRITSWAP.database_name


class pool(algebra.pool.poolv3):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
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
                   liquidity uint256, innerFeeGrowth0Token uint256, innerFeeGrowth1Token uint256, fees0 uint128, fees1 uint128
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

    def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:
                       liquidityGross   uint128 :  0        liquidityTotal
                       liquidityNet   int128 :  0           liquidityDelta
                       feeGrowthOutside0X128   uint256 :  0 outerFeeGrowth0Token
                       feeGrowthOutside1X128   uint256 :  0 outerFeeGrowth1Token
                       prevTick: int24 :  0                 prevTick
                       nextTick: int24 :  0                 nextTick
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0    outerSecondsPerLiquidity
                       secondsOutside   uint32 :  0         outerSecondsSpent
                       hasLimitOrders   bool :  false          hasLimitOrders
        """
        if result := self.call_function_autoRpc("ticks", None, tick):
            return {
                "liquidityGross": result[0],
                "liquidityNet": result[1],
                "feeGrowthOutside0X128": result[2],
                "feeGrowthOutside1X128": result[3],
                "prevTick": result[4],
                "nextTick": result[5],
                "secondsPerLiquidityOutsideX128": result[6],
                "secondsOutside": result[7],
                "hasLimitOrders": result[8],
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


class pool_cached(algebra.pool.poolv3_cached, pool):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME

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

    # builds
    def build_token(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
        processed_calls: list = None,
    ) -> erc20_cached:
        return erc20_cached(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
            processed_calls=processed_calls,
        )


class pool_multicall(algebra.pool.poolv3_multicall, pool):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        self._abi_filename = (
            abi_filename
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_POOL_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def communityVault(self) -> str:
        """The contract to which community fees are transferred

        Returns:
            str: The communityVault address
        """
        return self._communityVault.lower()

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

    def _fill_from_processed_calls(self, processed_calls: list):
        _this_object_names = ["pool"]
        for _pCall in processed_calls:
            # filter by address
            if _pCall["address"].lower() == self._address.lower():
                # filter by type
                if _pCall["object"] in _this_object_names:
                    if _pCall["name"] in [
                        "factory",
                        "totalFeeGrowth0Token",
                        "totalFeeGrowth1Token",
                        "liquidity",
                        "maxLiquidityPerTick",
                        "tickSpacing",
                        "activeIncentive",
                        "liquidityCooldown",
                        "communityVault",
                        "secondsPerLiquidityCumulative",
                        "tickSpacingLimitOrders",
                        "communityFeeLastTimestamp",
                    ]:
                        # one output only
                        if len(_pCall["outputs"]) != 1:
                            raise ValueError(
                                f"Expected only one output for {_pCall['name']}"
                            )
                        # check if value exists
                        if "value" not in _pCall["outputs"][0]:
                            raise ValueError(
                                f"Expected value in output for {_pCall['name']}"
                            )
                        _object_name = f"_{_pCall['name']}"
                        setattr(self, _object_name, _pCall["outputs"][0]["value"])
                    elif _pCall["name"] == "globalState":
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            {
                                "sqrtPriceX96": _pCall["outputs"][0]["value"],
                                "tick": _pCall["outputs"][1]["value"],
                                "fee": _pCall["outputs"][2]["value"],
                                "timepointIndex": _pCall["outputs"][3]["value"],
                                "communityFeeToken0": _pCall["outputs"][4]["value"],
                                "communityFeeToken1": _pCall["outputs"][5]["value"],
                                "unlocked": _pCall["outputs"][6]["value"],
                            },
                        )
                    elif _pCall["name"] in ["token0", "token1"]:
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            self.build_token(
                                address=_pCall["outputs"][0]["value"],
                                network=self._network,
                                block=self.block,
                                timestamp=self._timestamp,
                                processed_calls=processed_calls,
                            ),
                        )
                    elif _pCall["name"] == "dataStorageOperator":
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            dataStorageOperator(
                                address=_pCall["outputs"][0]["value"],
                                network=self._network,
                                block=self.block,
                                timestamp=self._timestamp,
                            ),
                        )
                    elif _pCall["name"] in ["getCommunityFeePending", "getReserves"]:
                        # tuples
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            [_o["value"] for _o in _pCall["outputs"]],
                        )
                    elif _pCall["name"] == "ticks":
                        _object_name = f"_{_pCall['name']}"
                        # create if not exists
                        if getattr(self, _object_name, None) is None:
                            setattr(self, _object_name, {})
                        # set
                        getattr(self, _object_name)[_pCall["inputs"][0]["value"]] = {
                            "liquidityGross": _pCall["outputs"][0]["value"],
                            "liquidityNet": _pCall["outputs"][1]["value"],
                            "feeGrowthOutside0X128": _pCall["outputs"][2]["value"],
                            "feeGrowthOutside1X128": _pCall["outputs"][3]["value"],
                            "tickCumulativeOutside": _pCall["outputs"][4]["value"],
                            "secondsPerLiquidityOutsideX128": _pCall["outputs"][5][
                                "value"
                            ],
                            "secondsOutside": _pCall["outputs"][6]["value"],
                            "initialized": _pCall["outputs"][7]["value"],
                        }
                    elif _pCall["name"] == "positions":
                        _object_name = f"_{_pCall['name']}"
                        # create if not exists
                        if getattr(self, _object_name, None) is None:
                            setattr(self, _object_name, {})
                        # set
                        getattr(self, _object_name)[_pCall["inputs"][0]["value"]] = {
                            "liquidity": _pCall["outputs"][0]["value"],
                            "feeGrowthInside0LastX128": _pCall["outputs"][1]["value"],
                            "feeGrowthInside1LastX128": _pCall["outputs"][2]["value"],
                            "tokensOwed0": _pCall["outputs"][3]["value"],
                            "tokensOwed1": _pCall["outputs"][4]["value"],
                        }
                    else:
                        logging.getLogger(__name__).debug(
                            f" {_pCall['name']} multicall field not defined to be processed. Ignoring"
                        )

    # builds
    def build_token(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
        processed_calls: list = None,
    ) -> erc20_multicall:
        return erc20_multicall(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
            processed_calls=processed_calls,
        )

    def _create_call_position(self, ownerAddress, tickLower, tickUpper) -> dict:
        _position_key = get_positionKey_algebra(
            ownerAddress=ownerAddress,
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        _position_key = (
            HexBytes(_position_key) if type(_position_key) == str else _position_key
        )

        return {
            "inputs": [
                {
                    "internalType": "bytes32",
                    "name": "",
                    "type": "bytes32",
                    "value": _position_key,
                }
            ],
            "name": "positions",
            "outputs": [
                {"internalType": "uint256", "name": "liquidity", "type": "uint128"},
                {
                    "internalType": "uint256",
                    "name": "feeGrowthInside0LastX128",
                    "type": "uint256",
                },
                {
                    "internalType": "uint256",
                    "name": "feeGrowthInside1LastX128",
                    "type": "uint256",
                },
                {
                    "internalType": "uint128",
                    "name": "tokensOwed0",
                    "type": "uint128",
                },
                {
                    "internalType": "uint128",
                    "name": "tokensOwed1",
                    "type": "uint128",
                },
            ],
            "stateMutability": "view",
            "type": "function",
            "address": self._address,
            "object": "pool",
        }

    def _create_call_ticks(self, tick: int) -> dict:
        return {
            "inputs": [
                {
                    "internalType": "int24",
                    "name": "",
                    "type": "int24",
                    "value": tick,
                }
            ],
            "name": "ticks",
            "outputs": [
                {
                    "internalType": "uint128",
                    "name": "liquidityGross",
                    "type": "uint128",
                },
                {"internalType": "int128", "name": "liquidityNet", "type": "int128"},
                {
                    "internalType": "uint256",
                    "name": "feeGrowthOutside0X128",
                    "type": "uint256",
                },
                {
                    "internalType": "uint256",
                    "name": "feeGrowthOutside1X128",
                    "type": "uint256",
                },
                {
                    "internalType": "int24",
                    "name": "prevTick",
                    "type": "int24",
                },
                {
                    "internalType": "int24",
                    "name": "nextTick",
                    "type": "int24",
                },
                {
                    "internalType": "uint160",
                    "name": "secondsPerLiquidityOutsideX128",
                    "type": "uint160",
                },
                {"internalType": "uint32", "name": "secondsOutside", "type": "uint32"},
                {"internalType": "bool", "name": "hasLimitOrders", "type": "bool"},
            ],
            "stateMutability": "view",
            "type": "function",
            "address": self._address,
            "object": "pool",
        }
