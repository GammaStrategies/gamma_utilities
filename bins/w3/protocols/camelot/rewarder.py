from web3 import Web3
from bins.w3.protocols.base_wrapper import web3wrap


class camelot_rewards_nft_pool_master(web3wrap):
    # https://vscode.blockscan.com/arbitrum-one/0x55401A4F396b3655f66bf6948A1A4DC61Dfc21f4
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
        self._abi_filename = abi_filename or "nft_pool_master"
        self._abi_path = abi_path or f"{self.abi_root_path}/camelot/rewards"

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

    def call_fn(self, fn_name: str, *args):
        """Call any function within the contract, as long as it is a read function, is in the loaded ABI, and the arguments are correct

        Args:
            fn_name (str): Function name
            args: Function arguments ( be aware of the order, and that addresses must be checksummed)

        """
        return self.call_function_autoRpc(fn_name, None, *args)


class camelot_rewards_nft_pool_factory(web3wrap):
    # https://vscode.blockscan.com/arbitrum-one/0x6dB1EF0dF42e30acF139A70C1Ed0B7E6c51dBf6d
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
        self._abi_filename = abi_filename or "nft_pool_factory"
        self._abi_path = abi_path or f"{self.abi_root_path}/camelot/rewards"

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

    def call_fn(self, fn_name: str, *args):
        """Call any function within the contract, as long as it is a read function, is in the loaded ABI, and the arguments are correct

        Args:
            fn_name (str): Function name
            args: Function arguments ( be aware of the order, and that addresses must be checksummed)

        """
        return self.call_function_autoRpc(fn_name, None, *args)


class camelot_rewards_nft_pool(web3wrap):
    # https://vscode.blockscan.com/arbitrum-one/0x6BC938abA940fB828D39Daa23A94dfc522120C11
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
        self._abi_filename = abi_filename or "nft_pool"
        self._abi_path = abi_path or f"{self.abi_root_path}/camelot/rewards"

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

    def call_fn(self, fn_name: str, *args):
        """Call any function within the contract, as long as it is a read function, is in the loaded ABI, and the arguments are correct

        Args:
            fn_name (str): Function name
            args: Function arguments ( be aware of the order, and that addresses must be checksummed)

        """
        return self.call_function_autoRpc(fn_name, None, *args)


class camelot_rewards_nitro_pool_factory(web3wrap):
    # https://vscode.blockscan.com/arbitrum-one/0xe0a6b372Ac6AF4B37c7F3a989Fe5d5b194c24569
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
        self._abi_filename = abi_filename or "nitro_pool_factory"
        self._abi_path = abi_path or f"{self.abi_root_path}/camelot/rewards"

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

    def call_fn(self, fn_name: str, *args):
        """Call any function within the contract, as long as it is a read function, is in the loaded ABI, and the arguments are correct

        Args:
            fn_name (str): Function name
            args: Function arguments ( be aware of the order, and that addresses must be checksummed)

        """
        return self.call_function_autoRpc(fn_name, None, *args)
