import logging
from web3 import Web3
from bins.config.hardcodes import SPECIAL_HYPERVISOR_ABIS
from bins.w3.helpers.multicaller import (
    build_call_with_abi_part,
    execute_parse_calls,
)
from bins.w3.protocols.base_wrapper import web3wrap


ABI_FILENAME = "spot_aggregator"
ABI_FOLDERNAME = "1inch"


class oneinch_spot_price_aggregator(web3wrap):
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
    def connectors(self) -> list[str]:
        return self.call_function_autoRpc("connectors")

    def getRate(self, srcToken: str, dstToken: str, useWrappers: bool) -> int:
        return self.call_function_autoRpc(
            "getRate",
            None,
            Web3.toChecksumAddress(srcToken),
            Web3.toChecksumAddress(dstToken),
            useWrappers,
        )

    def getRatetoEth(self, srcToken: str, useWrappers: bool) -> int:
        return self.call_function_autoRpc(
            "getRateToEth", None, Web3.toChecksumAddress(srcToken), useWrappers
        )

    def getRatetoEthWithCustomConnectors(
        self,
        srcToken: str,
        useWrappers: bool,
        customConnectors: list[str],
        thresholdFilter: int,
    ) -> int:
        return self.call_function_autoRpc(
            "getRatetoEthWithCustomConnectors",
            None,
            Web3.toChecksumAddress(srcToken),
            useWrappers,
            customConnectors,
            thresholdFilter,
        )

    def getRatetoEthWithThreshold(
        self, srcToken: str, useWrappers: bool, thresholdFilter: int
    ) -> int:
        return self.call_function_autoRpc(
            "getRatetoEthWithCustomConnectors",
            None,
            Web3.toChecksumAddress(srcToken),
            useWrappers,
            thresholdFilter,
        )

    def getRateWithCustomConnectors(
        self,
        srcToken: str,
        dstToken: str,
        useWrappers: bool,
        customConnectors: list[str],
        thresholdFilter: int,
    ) -> int:
        return self.call_function_autoRpc(
            "getRateWithCustomConnectors",
            None,
            Web3.toChecksumAddress(srcToken),
            Web3.toChecksumAddress(dstToken),
            useWrappers,
            customConnectors,
            thresholdFilter,
        )

    def getRateWithThreshold(
        self, srcToken: str, dstToken: str, useWrappers: bool, thresholdFilter: int
    ) -> int:
        return self.call_function_autoRpc(
            "getRateWithThreshold",
            None,
            Web3.toChecksumAddress(srcToken),
            Web3.toChecksumAddress(dstToken),
            useWrappers,
            thresholdFilter,
        )

    @property
    def multiWrapper(self) -> str:
        return self.call_function_autoRpc("multiWrapper")

    @property
    def oracles(self) -> list[str]:
        return self.call_function_autoRpc("oracles")

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")
