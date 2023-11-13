from web3 import Web3
from eth_abi import abi
from eth_utils import function_signature_to_4byte_selector

from bins.config.current import MULTICALL3_ADDRESSES

from .base_wrapper import web3wrap


ABI_FILENAME = "multicall3"
ABI_FOLDERNAME = "multicall"


class multicall3(web3wrap):
    # SETUP
    def __init__(
        self,
        network: str,
        address: str | None = None,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        if not address:
            address = MULTICALL3_ADDRESSES.get(network, MULTICALL3_ADDRESSES["default"])

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

    def aggregate(self, calls: list[tuple[str, bytes]]):
        """

        Args:
            calls (list[tuple[str, bytes]]): [ address , data bytes]

        Returns:
                list [ tuple( blockNumber (uint256): returnData (bytes[]) ) ]
        """
        return self.call_function_autoRpc("aggregate", None, calls)

    def aggregate3(self, calls: list[tuple[str, bool, bytes]]):
        """Aggregate calls, ensuring each returns success if required

        Args:
            calls (list[tuple[str, bool, bytes]]): [ address , allowFailure, data bytes]

        Returns:
                list [ tuple( success (bool): returnData (bytes) ) ]
        """
        return self.call_function_autoRpc("aggregate3", None, calls)

    def aggregate3Value(self, calls: list[tuple[str, bool, int, bytes]]):
        """Aggregate calls with a msg value
            Reverts if msg.value is less than the sum of the call values

        Args:
            calls (list[tuple[str, bool, int, bytes]]): [ address , allowFailure, value, data bytes]

        Returns:
                list [ tuple( success (bool): returnData (bytes) ) ]
        """
        return self.call_function_autoRpc("aggregate3Value", None, calls)

    def blockAndAggregate(self, calls: list[tuple[str, bytes]]):
        """Aggregate calls and allow failures using tryAggregate

        Args:
            calls (list[tuple[str, bytes]]): An array of Call structs [ address , data bytes]

        Returns:
                list [ tuple( blockNumber (uint256): blockHash (bytes32): returnData (bytes[]) ) ]
        """
        return self.call_function_autoRpc("blockAndAggregate", None, calls)

    def tryAggregate(self, requireSuccess: bool, calls: list[tuple[str, bytes]]):
        """Aggregate calls without requiring success

        Args:
            requireSuccess (bool): If true, require all calls to succeed
            calls (list[tuple[str, bytes]]): An array of Call structs

        Returns:
            List: results
        """
        # call
        return self.call_function_autoRpc(
            "tryAggregate",
            None,
            requireSuccess,
            calls,
        )

    # CUSTOM PROPERTIES

    def build_calls(self, contract_functions: list[dict], address: str | None = None):
        """Buld calls to be placed

        Args:
            contract_functions (list[dict]):
                    [ {"address":"0x0000000", "function": "baseLower()", "inputs": [], "outputs": ["int24"]},
                    {"address":"0x0000000","function": "baseUpper()", "inputs": [], "outputs": ["int24"]},
                        ... ]
            address (str, Optional): if no 'address' key found in contract_functions, this address will be used
        """
        # build function calls
        return [
            [
                Web3.toChecksumAddress(value.get("address", address)),
                function_signature_to_4byte_selector(f"{value['name']}()"),
                *value["inputs"],
            ]
            for value in contract_functions
        ]

    def get_data(self, contract_functions: list[dict], address: str | None = None):
        """Get data and throw error when any of the functions fail

        Args:
            contract_functions (list[dict]):
                    [ {"address":"0x0000000", "function": "baseLower()", "inputs": [], "outputs": ["int24"]},
                    {"address":"0x0000000","function": "baseUpper()", "inputs": [], "outputs": ["int24"]},
                        ... ]
            address (str, Optional): if no 'address' key found in contract_functions, this address will be used
        """
        # call multicall
        return self.aggregate(
            self.build_calls(contract_functions=contract_functions, address=address)
        )

    def try_get_data(
        self,
        contract_functions: list[dict],
        requireSuccess: bool = False,
        address: str | None = None,
    ):
        """Get data

        Args:
            contract_functions (list[dict]):
                    [ {"address":"0x0000000", "function": "baseLower()", "inputs": [], "outputs": ["int24"]},
                    {"address":"0x0000000","function": "baseUpper()", "inputs": [], "outputs": ["int24"]},
                        ... ]
            requireSuccess (bool, Optional): when true, require calls to success
            address (str, Optional): if no 'address' key found in contract_functions, this address will be used
        """
        # call multicall
        return self.tryAggregate(
            requireSuccess=False,
            calls=self.build_calls(
                contract_functions=contract_functions, address=address
            ),
        )
