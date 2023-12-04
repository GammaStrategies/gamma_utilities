from web3 import Web3
from bins.general.enums import Chain
from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls
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

    def get_all_nft_pools_addresses(self, chain: Chain) -> list[str]:
        """Get all nft pools addresses (using multicall)
           total calls: 1 + activePoolsLength/100 ( 1 + 2 = 3 calls for 200 pools)

        Args:
            chain (Chain):
            master_address (str):
            block (int, optional): . Defaults to 0.

        Returns:
            list[str]: _description_
        """
        # get total number of active nft pools
        active_nft_pools = self.call_fn(fn_name="activePoolsLength")
        # prepare calls for the multicall: for range i to active pools lenght  call getPoolAddressByIndex(i)
        abi_part = self.get_abi_function("getActivePoolAddressByIndex")
        master_calls = [
            build_call_with_abi_part(
                abi_part=abi_part,
                inputs_values=[i],
                address=self.address,
                object="nft_pool_master",
            )
            for i in range(active_nft_pools)
        ]
        # place xxx calls at a time
        _max_calls_atOnce = 1000
        nft_pool_addresses = []
        for i in range(0, len(master_calls), _max_calls_atOnce):
            nft_pool_addresses += execute_parse_calls(
                network=chain.database_name,
                block=self.block,
                calls=master_calls[i : i + _max_calls_atOnce],
                convert_bint=False,
            )
        return [x["outputs"][0]["value"].lower() for x in nft_pool_addresses]

    def get_all_rewards_info(self, chain: Chain):
        """Returns all active rewards found information, using multicall
            * when allocPoint,poolEmissionRate,reserve is 0, the pool is inactive
        Args:
            chain (Chain): network

        Returns:
            dict: {<nft_pool_address>:  allocPoint,lastRewardTime,reserve,poolEmisionRate,lpToken,grailToken, xGrailToken, accRewardsPerShare,lpSupply,lpSupplyWithMultiplier }
        """
        # get total number of active nft pools
        nft_pool_addresses = self.get_all_nft_pools_addresses(chain=chain)

        # from master call getPoolInfo: get emissionRate
        # prepare calls for the multicall: for range i to active pools lenght  call getPoolInfo(i)
        abi_part = self.get_abi_function("getPoolInfo")
        master_calls = [
            build_call_with_abi_part(
                abi_part=abi_part,
                inputs_values=[address],
                address=self.address,
                object="nft_pool_master",
            )
            for address in nft_pool_addresses
        ]

        # from the nftPool, call getPoolInfo
        nftPool_abi_part = camelot_rewards_nft_pool(
            address="0x0000000000000000000000000000000000000000",
            network=chain.database_name,
            block=self.block,
            timestamp=self._timestamp,
        ).get_abi_function("getPoolInfo")
        # create dummy nftPool to get abi from
        nft_pool_calls = [
            build_call_with_abi_part(
                abi_part=nftPool_abi_part,
                inputs_values=[],
                address=nftPool_address,
                object="nft_pool",
            )
            for nftPool_address in nft_pool_addresses
        ]

        # place xxx calls at a time
        _max_calls_atOnce = 1000
        calls_to_place = master_calls + nft_pool_calls
        pool_infos = []
        for i in range(0, len(calls_to_place), _max_calls_atOnce):
            pool_infos += execute_parse_calls(
                network=chain.database_name,
                block=self.block,
                calls=calls_to_place[i : i + _max_calls_atOnce],
                convert_bint=False,
            )

        result = {}
        for pool_info in pool_infos:
            if pool_info["object"] == "nft_pool":
                _addrs = pool_info["address"].lower()
                # init nft pool address in result
                if not _addrs in result:
                    result[_addrs] = {}

                result[_addrs]["lpToken"] = pool_info["outputs"][0]["value"]
                result[_addrs]["grailToken"] = pool_info["outputs"][1]["value"]
                result[_addrs]["xGrailToken"] = pool_info["outputs"][2]["value"]
                # result[_addrs]["lastRewardTime"] = pool_info["outputs"][3]["value"]
                result[_addrs]["accRewardsPerShare"] = pool_info["outputs"][4]["value"]
                result[_addrs]["lpSupply"] = pool_info["outputs"][5]["value"]
                result[_addrs]["lpSupplyWithMultiplier"] = pool_info["outputs"][6][
                    "value"
                ]
                # result[_addrs]["allocPoint"] = pool_info["outputs"][7]["value"]

            elif pool_info["object"] == "nft_pool_master":
                _addrs = pool_info["inputs"][0]["value"].lower()
                # init nft pool address in result
                if not _addrs in result:
                    result[_addrs] = {}
                # set vars
                result[_addrs]["nft_pool_address"] = pool_info["outputs"][0]["value"]
                result[_addrs]["allocPoint"] = pool_info["outputs"][1]["value"]
                result[_addrs]["lastRewardTime"] = pool_info["outputs"][2]["value"]
                result[_addrs]["reserve"] = pool_info["outputs"][3]["value"]
                result[_addrs]["poolEmissionRate"] = pool_info["outputs"][4]["value"]

            else:
                raise ValueError(f" Object not recognized {pool_info['object']}")

        # when allocPoint,poolEmissionRate,reserve is 0, the pool is inactive
        return result


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
