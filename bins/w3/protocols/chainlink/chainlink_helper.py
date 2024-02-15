import logging
from web3 import Web3
from bins.config.hardcodes import SPECIAL_HYPERVISOR_ABIS
from bins.w3.helpers.multicaller import (
    build_call_with_abi_part,
    build_calls_fromfiles,
    execute_parse_calls,
)
from bins.w3.protocols.base_wrapper import web3wrap


ABI_FILENAME = "eacaggregator"
ABI_FOLDERNAME = "chainlink"


class chainlink_connector(web3wrap):
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
        # set init vars ( this is needed to be able to use self.address in initialize_abi --> camelot... )
        self._address = Web3.toChecksumAddress(address)
        self._network = network
        self._initialize_abi(abi_filename=abi_filename, abi_path=abi_path)

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

    def _initialize_abi(self, abi_filename: str = "", abi_path: str = ""):
        # check if this is a special hypervisor and abi_filename is not set
        self._abi_filename = (
            abi_filename
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("file", None)
            or ABI_FILENAME
        )
        self._abi_path = (
            abi_path
            or SPECIAL_HYPERVISOR_ABIS.get(self._network, {})
            .get(self._address.lower(), {})
            .get("folder", None)
            or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        )

    @property
    def decimals(self) -> int:
        return self.call_function_autoRpc("decimals")

    @property
    def latestAnswer(self) -> int:
        return self.call_function_autoRpc("latestAnswer")

    @property
    def latestRound(self) -> int:
        return self.call_function_autoRpc("latestRound")

    @property
    def latestRoundData(self) -> dict | None:
        try:
            if result := self.call_function_autoRpc("latestRoundData"):
                return {
                    "roundid": result[0],
                    "answer": result[1],
                    "startedAt": result[2],
                    "updatedAt": result[3],
                    "answeredInRound": result[4],
                }

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error getting latestRoundData from {self.address} : {e}"
            )
            raise e


class chainlink_connector_multicall(chainlink_connector):

    @property
    def decimals(self) -> int:
        return self._decimals

    @property
    def latestAnswer(self) -> int:
        return self._latestAnswer

    @property
    def latestRound(self) -> int:
        return self._latestRound

    @property
    def latestRoundData(self) -> dict | None:
        return self._latestRoundData

    def fill_with_multicall(self):
        _calls = []
        _calls.append(
            build_call_with_abi_part(
                abi_part=self.get_abi_function("decimals"),
                inputs_values=[],
                address=self.address,
                object="eaclink",
            )
        )
        _calls.append(
            build_call_with_abi_part(
                abi_part=self.get_abi_function("latestRoundData"),
                inputs_values=[],
                address=self.address,
                object="eaclink",
            )
        )

        # get all abi functions
        # _calls = [
        #     build_call_with_abi_part(
        #         abi_part=abi_part,
        #         inputs_values=[],
        #         address=self.address,
        #         object="eaclink",
        #     )
        #     for abi_part in self.get_abi_functions()
        # ]

        # execute calls
        calls = execute_parse_calls(
            network=self._network,
            block=self.block,
            calls=_calls,
            convert_bint=False,
            requireSuccess=True,
        )

        # fill objects
        self._fill_from_processed_calls(processed_calls=calls)

    def _fill_from_processed_calls(self, processed_calls: list):
        _this_object_names = ["eaclink"]
        for _pCall in processed_calls:
            # filter by address
            if _pCall["address"].lower() == self._address.lower():
                # filter by object type
                if _pCall["object"] in _this_object_names:
                    if _pCall["name"] in [
                        "latestAnswer",
                        "latestRound",
                        "accessController",
                        "aggregator",
                        "decimals",
                        "description",
                        "latestTimestamp",
                        "owner",
                        "phaseId",
                        "proposedAggregator",
                        "version",
                    ]:
                        # one output only
                        if len(_pCall["outputs"]) != 1:
                            raise ValueError(
                                f"Expected only one output for {_pCall['name']}"
                            )
                        # check if value exists
                        if "value" not in _pCall["outputs"][0]:

                            # we need decimals only for the price
                            if _pCall["name"] == "decimals":
                                # try get it placing a direct call
                                try:
                                    self._decimals = self.call_function_autoRpc(
                                        "decimals"
                                    )
                                except Exception as e:
                                    raise ValueError(
                                        f"Expected value in output for {_pCall['name']}"
                                    )

                            else:
                                logging.getLogger(__name__).debug(
                                    f" {_pCall['name']} not defined for {self.address}. Ignoring"
                                )

                        _object_name = f"_{_pCall['name']}"
                        setattr(self, _object_name, _pCall["outputs"][0]["value"])
                    elif _pCall["name"] in [
                        "latestRoundData",
                        "proposedLatestRoundData",
                    ]:
                        _object_name = f"_{_pCall['name']}"

                        if len(_pCall["outputs"]) != 5:
                            raise ValueError(f"Expected 5 outputs for {_pCall['name']}")
                        else:
                            try:
                                setattr(
                                    self,
                                    _object_name,
                                    {
                                        "roundid": _pCall["outputs"][0]["value"],
                                        "answer": _pCall["outputs"][1]["value"],
                                        "startedAt": _pCall["outputs"][2]["value"],
                                        "updatedAt": _pCall["outputs"][3]["value"],
                                        "answeredInRound": _pCall["outputs"][4][
                                            "value"
                                        ],
                                    },
                                )
                            except Exception as e:
                                if _pCall["name"] == "proposedLatestRoundData":
                                    logging.getLogger(__name__).debug(
                                        f" {_pCall['name']} not defined for {self.address}. Ignoring"
                                    )
                                    setattr(
                                        self,
                                        _object_name,
                                        {
                                            "roundid": None,
                                            "answer": None,
                                            "startedAt": None,
                                            "updatedAt": None,
                                            "answeredInRound": None,
                                        },
                                    )
                                else:
                                    raise e

                    else:
                        logging.getLogger(__name__).debug(
                            f" {_pCall['name']} multicall field not defined to be processed. Ignoring"
                        )
