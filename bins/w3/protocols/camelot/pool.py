from copy import deepcopy
import logging
from web3 import Web3
from bins.config.hardcodes import SPECIAL_POOL_ABIS

from bins.errors.general import ProcessingError
from bins.w3.protocols.algebra.pool import dataStorageOperator
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import algebra
from ..general import erc20_cached, erc20_multicall


ABI_FILENAME = "camelot_pool"
ABI_FOLDERNAME = "camelot"
DEX_NAME = Protocol.CAMELOT.database_name

# ABI_FILENAMES = {
#     "0xb7Dd20F3FBF4dB42Fd85C839ac0241D09F72955f".lower(): "camelot_pool_old"
# }


class pool(algebra.pool.poolv3):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        # Camelot has two pool ABIs
        # if self.address.lower() in ABI_FILENAMES:
        #     # one address has a different ABI
        #     self._abi_filename = abi_filename or ABI_FILENAMES[self.address.lower()]
        # else:
        #     self._abi_filename = abi_filename or ABI_FILENAME
        # self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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
    def communityFeeLastTimestamp(self) -> int:
        return self.call_function_autoRpc("communityFeeLastTimestamp")

    @property
    def communityVault(self) -> str:
        # TODO: communityVault object
        return self.call_function_autoRpc("communityVault")

    @property
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault

        Returns:
            tuple[int,int]: token0,token1
        """
        return self.call_function_autoRpc("getCommunityFeePending")

    def getTimepoints(self, secondsAgo: int):
        raise NotImplementedError(" No get Timepoints in camelot")

    @property
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves

        Returns:
            tuple[int,int]: token0,token1
        """
        return self.call_function_autoRpc("getReserves")

    @property
    def globalState(self) -> dict:
        """

        Returns:
           dict:    uint160 price; // The square root of the current price in Q64.96 format
                    int24 tick; // The current tick
                    uint16 feeZto; // The current fee for ZtO swap in hundredths of a bip, i.e. 1e-6
                    uint16 feeOtz; // The current fee for OtZ swap in hundredths of a bip, i.e. 1e-6
                    uint16 timepointIndex; // The index of the last written timepoint
                    uint8 communityFee; // The community fee represented as a percent of all collected fee in thousandths (1e-3)
                    bool unlocked; // True if the contract is unlocked, otherwise - false
        """
        if tmp := self.call_function_autoRpc("globalState"):
            return {
                "sqrtPriceX96": tmp[0],
                "tick": tmp[1],
                "fee": tmp[2],
                "timepointIndex": tmp[4],
                "communityFeeToken0": tmp[5],
                "communityFeeToken1": tmp[6],
                "unlocked": tmp[7],
                # special
                "feeZto": tmp[2],
                "feeOtz": tmp[3],
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


class pool_cached(pool):
    SAVE2FILE = True

    def _initialize_objects(self):
        self._token0: erc20_cached = None
        self._token1: erc20_cached = None
        self._dataStorage: dataStorageOperator = None

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
    def dataStorageOperator(self) -> algebra.pool.dataStorageOperator_cached:
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
            self._dataStorage = algebra.pool.dataStorageOperator_cached(
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
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
        if self._token0 is None:
            # check if cached
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
            self._token0 = self.build_token(
                address=result,
                network=self._network,
                block=self.block,
                timestamp=self._timestamp,
            )
        return self._token0

    @property
    def token1(self) -> erc20_cached:
        if self._token1 is None:
            # check if cached
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
            self._token1 = self.build_token(
                address=result,
                network=self._network,
                block=self.block,
                timestamp=self._timestamp,
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

    # builds
    def build_token(
        self,
        address: str,
        network: str,
        block: int,
        timestamp: int | None = None,
    ) -> erc20_cached:
        return erc20_cached(
            address=address,
            network=network,
            block=block,
            timestamp=timestamp,
        )


class pool_multicall(algebra.pool.poolv3_multicall):
    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        # Camelot has two pool ABIs
        # if self.address.lower() in ABI_FILENAMES:
        #     # one address has a different ABI
        #     self._abi_filename = abi_filename or ABI_FILENAMES[self.address.lower()]
        # else:
        #     self._abi_filename = abi_filename or ABI_FILENAME
        # self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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

    def _initialize_objects(self):
        self._token0: erc20_multicall = None
        self._token1: erc20_multicall = None
        self._dataStorage: dataStorageOperator = None
        self._ticks = {}
        self._positions = {}

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def communityFeeLastTimestamp(self) -> int:
        return self._communityFeeLastTimestamp

    @property
    def communityVault(self) -> str:
        return self._communityVault.lower()

    @property
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault

        Returns:
            tuple[int,int]: token0,token1
        """
        return deepcopy(self._getCommunityFeePending)

    @property
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves

        Returns:
            tuple[int,int]: token0,token1
        """
        return deepcopy(self._getReserves)

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
                                "timepointIndex": _pCall["outputs"][4]["value"],
                                "communityFeeToken0": _pCall["outputs"][5]["value"],
                                "communityFeeToken1": _pCall["outputs"][6]["value"],
                                "unlocked": _pCall["outputs"][7]["value"],
                                # special
                                "feeZto": _pCall["outputs"][2]["value"],
                                "feeOtz": _pCall["outputs"][3]["value"],
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
                            "lastLiquidityAddTimestamp": _pCall["outputs"][1]["value"],
                            "feeGrowthInside0LastX128": _pCall["outputs"][2]["value"],
                            "feeGrowthInside1LastX128": _pCall["outputs"][3]["value"],
                            "tokensOwed0": _pCall["outputs"][4]["value"],
                            "tokensOwed1": _pCall["outputs"][5]["value"],
                        }

                    else:
                        logging.getLogger(__name__).debug(
                            f" {_pCall['name']} multicall field not defined to be processed. Ignoring"
                        )
                else:
                    pass
