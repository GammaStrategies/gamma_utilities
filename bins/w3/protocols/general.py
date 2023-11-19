import logging
from web3 import Web3
from eth_abi import abi

from .base_wrapper import web3wrap
from .multicall import multicall3

from ...config.current import WEB3_CHAIN_IDS  # ,CFG
from ...cache import cache_utilities


# ERC20
class erc20(web3wrap):
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
        self._initialize_abi(abi_filename=abi_filename, abi_path=abi_path)
        self._initialize_objects()

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
        self._abi_filename = abi_filename or "erc20"
        self._abi_path = abi_path or self.abi_root_path

    def _initialize_objects(self):
        pass

    def inmutable_fields(self) -> dict[str, bool]:
        """inmutable fields by contract

        Returns:
            dict[str, bool]:  field name: is inmutable?
        """
        return {
            "decimals": True,  # decimals should be always fixed
            "symbol": False,
        }

    # PROPERTIES
    @property
    def decimals(self) -> int:
        return self.call_function_autoRpc(function_name="decimals")

    def balanceOf(self, address: str) -> int:
        return self.call_function_autoRpc(
            "balanceOf", None, Web3.toChecksumAddress(address)
        )

    @property
    def totalSupply(self) -> int:
        return self.call_function_autoRpc(function_name="totalSupply")

    @property
    def symbol(self) -> str:
        # MKR special: ( has a too large for python int )
        if self.address == "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2":
            return "MKR"
        return self.call_function_autoRpc(function_name="symbol")

    def allowance(self, owner: str, spender: str) -> int:
        return self.call_function_autoRpc(
            "allowance",
            None,
            Web3.toChecksumAddress(owner),
            Web3.toChecksumAddress(spender),
        )

    def as_dict(self, convert_bint=False, minimal: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to strings ? . Defaults to False.

        Returns:
            dict: decimals, totalSupply(bint) and symbol dict
        """
        result = super().as_dict(convert_bint=convert_bint, minimal=minimal)

        if not minimal:
            # not minimal
            result["decimals"] = self.decimals
            result["symbol"] = self.symbol

        # minimal
        result["totalSupply"] = (
            str(self.totalSupply) if convert_bint else self.totalSupply
        )

        return result

    # dict from multicall
    def _get_dict_from_multicall(self, contract_abi: list[dict] | None = None) -> dict:
        """Only this object and its super() will be returned as dict ( no pools nor objects within this)
             When any of the function is not returned ( for any reason ), its key will not appear in the result.
            All functions defined in the abi without "inputs" will be returned here
        Returns:
            dict:
        """
        contract_functions = contract_abi or self.contract_functions

        # create multicall helper
        _multicall_helper = multicall3(network=self._network, block=self.block)
        # get data thru multicall contract
        multicall_result = _multicall_helper.try_get_data(
            contract_functions=contract_functions, address=self.address
        )
        # decode result
        result = {}
        for idx, _res_itm in enumerate(multicall_result):
            # first item returned is success bool
            if _res_itm[0]:
                # success

                data = abi.decode(
                    [out["type"] for out in contract_functions[idx]["outputs"]],
                    _res_itm[1],
                )

                # build result key = function name
                key = contract_functions[idx]["name"]

                # set var context
                if len(contract_functions[idx]["outputs"]) > 1:
                    if not [
                        1 for x in contract_functions[idx]["outputs"] if x["name"] != ""
                    ]:
                        # dictionary
                        result[key] = {}
                    else:
                        # list
                        result[key] = []
                else:
                    # one item
                    result[key] = None

                # loop thru output
                for output_idx, output in enumerate(contract_functions[idx]["outputs"]):
                    # add to result
                    if isinstance(result[key], list):
                        result[key].append(data[output_idx])
                    elif isinstance(result[key], dict):
                        result[key][output["name"]] = data[output_idx]
                    else:
                        result[key] = data[output_idx]

        return result


class erc20_cached(erc20):
    SAVE2FILE = True

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = self.inmutable_fields()

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

    # PROPERTIES
    @property
    def decimals(self) -> int:
        prop_name = "decimals"
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
    def totalSupply(self) -> int:
        prop_name = "totalSupply"
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
    def symbol(self) -> str:
        prop_name = "symbol"
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


class erc20_multicall(erc20):
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
        processed_calls: list | None = None,
    ):
        self._initialize_abi(abi_filename=abi_filename, abi_path=abi_path)
        self._initialize_objects()

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

        if processed_calls:
            self._fill_from_processed_calls(processed_calls=processed_calls)

    def _initialize_objects(self):
        self._balanceOf: dict[str, int] = {}

    # PROPERTIES
    @property
    def decimals(self) -> int:
        return self._decimals

    @property
    def totalSupply(self) -> int:
        return self._totalSupply

    @property
    def symbol(self) -> str:
        return self._symbol

    def balanceOf(self, address: str) -> int:
        _address = Web3.toChecksumAddress(address)

        if not _address in self._balanceOf:
            self._balanceOf[_address] = super().balanceOf(address)
        return self._balanceOf[_address]

    def _fill_from_processed_calls(self, processed_calls: list):
        """Fill data from known data dict

        Args:
            processed_calls (dict): known data dict
        """
        _this_object_names = ["token0", "token1"]
        for _pCall in processed_calls:
            # filter by address
            if _pCall["address"].lower() == self._address.lower():
                # filter by type
                if _pCall["object"] in _this_object_names:
                    if _pCall["name"] in [
                        "decimals",
                        "totalSupply",
                        "symbol",
                        "name",
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
                    elif _pCall["name"] == "balanceOf":
                        _object_name = f"_{_pCall['name']}"

                        if getattr(self, _object_name, None) is None:
                            setattr(self, _object_name, {})

                        getattr(self, _object_name)[
                            _pCall["inputs"][0]["value"]
                        ] = _pCall["outputs"][0]["value"]
                    else:
                        logging.getLogger(__name__).debug(
                            f" {_pCall['name']} multicall field not defined to be processed. Ignoring"
                        )
                        # raise ValueError(f"Unknown function {_pCall['name']}")


# BEP20


class bep20(erc20):
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
        self._abi_filename = abi_filename or "bep20"
        self._abi_path = abi_path or self.abi_root_path

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


class bep20_cached(bep20):
    SAVE2FILE = True

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = self.inmutable_fields()

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

    # PROPERTIES
    @property
    def decimals(self) -> int:
        prop_name = "decimals"
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
    def totalSupply(self) -> int:
        prop_name = "totalSupply"
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
    def symbol(self) -> str:
        prop_name = "symbol"
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


class bep20_multicall(erc20_multicall):
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
        processed_calls: list | None = None,
    ):
        self._abi_filename = abi_filename or "bep20"
        self._abi_path = abi_path or self.abi_root_path

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
            processed_calls=processed_calls,
        )
