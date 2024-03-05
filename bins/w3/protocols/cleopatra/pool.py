import logging
from bins.config.hardcodes import SPECIAL_POOL_ABIS

from ....general.enums import Protocol
from .. import ramses

ABI_FILENAME = "cleopatraPool"
ABI_FOLDERNAME = "cleopatra"
DEX_NAME = Protocol.CLEOPATRA.database_name
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


class pool(ramses.pool.pool):
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

    def inmutable_fields(self) -> dict[str, bool]:
        """inmutable fields by contract

        Returns:
            dict[str, bool]:  field name: is inmutable?
        """
        return INMUTABLE_FIELDS


class pool_cached(ramses.pool.pool_cached):
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


class pool_multicall(ramses.pool.pool_multicall):
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

    def _fill_from_processed_calls(self, processed_calls: list):
        _this_object_names = ["pool"]
        for _pCall in processed_calls:
            # filter by address
            if _pCall["address"].lower() == self._address.lower():
                # filter by type
                if _pCall["object"] in _this_object_names:
                    if _pCall["name"] in [
                        "factory",
                        "fee",
                        "feeGrowthGlobal0X128",
                        "feeGrowthGlobal1X128",
                        "liquidity",
                        "maxLiquidityPerTick",
                        "tickSpacing",
                        "lmPool",
                        "boostedLiquidity",
                        "currentFee",
                        "lastPeriod",
                        "nfpManager",
                        "votingEscrow",  # "veRam",
                        "voter",
                    ]:
                        # one output only
                        if len(_pCall["outputs"]) != 1:
                            raise ValueError(
                                f"Expected only one output for {_pCall['name']}"
                            )
                        # check if value exists
                        if "value" not in _pCall["outputs"][0]:
                            # EXCEPTION: Ramses implemented currentFee at some point so lets use fee instead
                            # only on this cases: Ramses pool contract without currentFee output
                            if (
                                _pCall["name"] == "currentFee"
                                and "pool" in _this_object_names
                            ):
                                # search for current object's "fee" output value in processed calls list, and use it instead
                                if _fee_output := [
                                    _xc
                                    for _xc in processed_calls
                                    if _xc["name"] == "fee"
                                    and _xc["object"] in _this_object_names
                                    and "value" in _xc["outputs"][0]
                                ]:
                                    # set output currentFee value to fee
                                    _pCall["outputs"][0]["value"] = _fee_output[0][
                                        "outputs"
                                    ][0]["value"]
                                else:
                                    raise ValueError(
                                        f"Expected value in output for {_pCall['name']}"
                                    )

                            else:
                                raise ValueError(
                                    f"Expected value in output for {_pCall['name']}"
                                )
                        _object_name = f"_{_pCall['name']}"
                        setattr(self, _object_name, _pCall["outputs"][0]["value"])
                    elif _pCall["name"] == "slot0":
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            {
                                "sqrtPriceX96": _pCall["outputs"][0]["value"],
                                "tick": _pCall["outputs"][1]["value"],
                                "observationIndex": _pCall["outputs"][2]["value"],
                                "observationCardinality": _pCall["outputs"][3]["value"],
                                "observationCardinalityNext": _pCall["outputs"][4][
                                    "value"
                                ],
                                "feeProtocol": _pCall["outputs"][5]["value"],
                                "unlocked": _pCall["outputs"][6]["value"],
                            },
                        )
                    elif _pCall["name"] == "protocolFees":
                        _object_name = f"_{_pCall['name']}"
                        setattr(
                            self,
                            _object_name,
                            (
                                _pCall["outputs"][0]["value"],
                                _pCall["outputs"][1]["value"],
                            ),
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

                    elif _pCall["name"] == "ticks":
                        _object_name = f"_{_pCall['name']}"
                        # create if not exists
                        if getattr(self, _object_name, None) is None:
                            setattr(self, _object_name, {})
                        # set
                        getattr(self, _object_name)[_pCall["inputs"][0]["value"]] = {
                            "liquidityGross": _pCall["outputs"][0]["value"],
                            "liquidityNet": _pCall["outputs"][1]["value"],
                            "boostedLiquidityGross": _pCall["outputs"][2]["value"],
                            "boostedLiquidityNet": _pCall["outputs"][3]["value"],
                            "feeGrowthOutside0X128": _pCall["outputs"][4]["value"],
                            "feeGrowthOutside1X128": _pCall["outputs"][5]["value"],
                            "tickCumulativeOutside": _pCall["outputs"][6]["value"],
                            "secondsPerLiquidityOutsideX128": _pCall["outputs"][7][
                                "value"
                            ],
                            "secondsOutside": _pCall["outputs"][8]["value"],
                            "initialized": _pCall["outputs"][9]["value"],
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
                            "attachedVeRamId": _pCall["outputs"][5]["value"],
                        }

                    else:
                        logging.getLogger(__name__).debug(
                            f" {_pCall['name']} multicall field not defined to be processed. Ignoring"
                        )
