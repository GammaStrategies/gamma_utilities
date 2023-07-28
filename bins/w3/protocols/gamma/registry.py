import logging
from web3 import Web3
from bins.w3.protocols.general import web3wrap


class gamma_hypervisor_registry(web3wrap):
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
        self._abi_filename = abi_filename or "registry"
        self._abi_path = abi_path or "data/abi/gamma/ethereum"

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

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {
        "ethereum": [
            "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599".lower()
        ],  # address:index
        "polygon": [
            "0xa9782a2c9c3fb83937f14cdfac9a6d23946c9255".lower(),
            "0xfb0bc232CD11dBe804B489860c470B7f9cc80D9F".lower(),
        ],
        "optimism": ["0xc7722271281Aa6D5D027fC9B21989BE99424834f".lower()],
        "arbitrum": ["0x38f81e638f9e268e8417F2Ff76C270597fa077A0".lower()],
    }

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value

        Returns:
            int: positions of hypervisors in registry
        """
        return self.call_function_autoRpc("counter")

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return self.call_function_autoRpc("hypeByIndex", None, index)

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    def registry(self, index: int) -> str:
        return self.call_function_autoRpc("registry", None, index)

    def registryMap(self, address: str) -> int:
        return self.call_function_autoRpc(
            "registryMap", None, Web3.toChecksumAddress(address)
        )

    # CUSTOM FUNCTIONS
    def get_hypervisors_addresses(self) -> tuple[list[str], list[str]]:
        """Retrieve all hypervisors addresses from registry

        Returns:
           list of addresses, applying blacklist
           list of addresses disabled by contract (index 0)
        """

        #
        total_hypervisors_qtty = self.counter
        result = []
        disabled = []
        # retrieve all valid hypervisors addresses
        # loop until all hypervisors have been retrieved ( no while loop to avoid infinite loop)
        for i in range(10000):
            # exit
            if len(result) >= total_hypervisors_qtty:
                break

            try:
                hypervisor_id, idx = self.hypeByIndex(index=i)

                if idx:
                    result.append(hypervisor_id)
                else:
                    disabled.append(hypervisor_id)

            except TypeError as e:
                # hype index is out of bounds
                logging.getLogger(__name__).debug(
                    f" Hypervisor index {i} is out of bounds for {self._network} {self.address}  error-> {e} "
                )
                # break
                logging.getLogger(__name__).error(
                    f" Breaking loop for {self._network} {self.address} while not all hypervisors have been returned. This should not happen."
                )
                break

            except Exception as e:
                # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
                logging.getLogger(__name__).warning(
                    f" Error while retrieving addresses from registry {self._network} {self.address}  error-> {e} "
                )

        # remove blacklisted addresses
        for address in result:
            if (
                self._network in self.__blacklist_addresses
                and address.lower() in self.__blacklist_addresses[self._network]
            ):
                # address is blacklisted
                result.remove(address)

        return result, disabled

    def apply_blacklist(self, blacklist: list[str]):
        """Save filters to be applied to the registry

        Args:
            blacklist (list[str]): list of addresses to blacklist
        """
        if self._network not in self.__blacklist_addresses:
            self.__blacklist_addresses[self._network] = blacklist
        else:
            self.__blacklist_addresses[self._network] += blacklist
