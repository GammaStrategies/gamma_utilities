import copy
import logging
from eth_abi import abi
from web3 import Web3
from ..protocols.multicall import multicall3
from ...configuration import CONFIGURATION
from ...general import file_utilities
from ...general.enums import Chain, Protocol


def execute_multicall(
    network: str,
    block: int,
    hypervisor_address: str | None = None,
    pool_address: str | None = None,
    token0_address: str | None = None,
    token1_address: str | None = None,
    hypervisor_abi_filename: str | None = None,
    hypervisor_abi_path: str | None = None,
    pool_abi_filename: str | None = None,
    pool_abi_path: str | None = None,
    convert_bint: bool = False,
) -> list:
    # build calls
    calls = build_calls_fromfiles(
        network=network,
        hypervisor_address=hypervisor_address,
        pool_address=pool_address,
        token0_address=token0_address,
        token1_address=token1_address,
        hypervisor_abi_filename=hypervisor_abi_filename,
        hypervisor_abi_path=hypervisor_abi_path,
        pool_abi_filename=pool_abi_filename,
        pool_abi_path=pool_abi_path,
    )
    # place em
    return execute_parse_calls(
        network=network, block=block, calls=calls, convert_bint=convert_bint
    )


def execute_parse_calls(
    network: str,
    block: int,
    calls: list,
    convert_bint: bool = False,
    requireSuccess: bool = False,
    custom_rpcType: str | None = None,
):
    # place em
    multicall_raw_result = _get_multicall_result(
        network=network,
        block=block,
        calls=calls,
        requireSuccess=requireSuccess,
        custom_rpcType=custom_rpcType,
    )
    return parse_multicall_readfunctions_result(
        calls=calls,
        multicall_raw_result=multicall_raw_result,
        convert_bint=convert_bint,
    )


def _get_multicall_result(
    network: str,
    block: int,
    calls: list,
    requireSuccess: bool = False,
    custom_rpcType: str | None = None,
):
    # execute call
    multicall_helper = multicall3(
        network=network,
        block=block,
    )
    # set custom rpcType
    if custom_rpcType:
        multicall_helper.custom_rpcType = custom_rpcType

    # return data
    return multicall_helper.try_get_data(
        contract_functions=calls, requireSuccess=requireSuccess
    )


def parse_multicall_readfunctions_result(
    calls: list, multicall_raw_result: list, convert_bint: bool = False
) -> list:
    """Parse returns on read functions

    Args:
        calls (list):
        multicall_raw_result (list):
        convert_bint (bool, optional): . Defaults to False.
    """
    # calls and results indexes match
    for idx, call_result in enumerate(multicall_raw_result):
        # check if there are bytes to process ( cause success without bytes is a fail in any read function)
        # call_result -> [<success bool>, <data returned>]
        if call_result[0] and call_result[1]:
            # success: decode and save data in the calls list var so that can be later used
            try:
                _data_decoded = abi.decode(
                    [out["type"] for out in calls[idx]["outputs"]],
                    call_result[1],
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error decoding data {call_result} -> {e}"
                )
                raise
            for _data_index, data_item in enumerate(_data_decoded):
                # set value field on each output
                calls[idx]["outputs"][_data_index]["value"] = data_item

    # return
    return calls


def build_calls_fromfiles(
    network: str,
    hypervisor_address: str | None = None,
    pool_address: str | None = None,
    token0_address: str | None = None,
    token1_address: str | None = None,
    hypervisor_abi_filename: str | None = None,
    hypervisor_abi_path: str | None = None,
    pool_abi_filename: str | None = None,
    pool_abi_path: str | None = None,
):
    # load abis
    if hypervisor_abi_filename:
        hypervisor_abi = file_utilities.load_json(
            filename=hypervisor_abi_filename, folder_path=hypervisor_abi_path
        )
        # ERC or BEP ?
        erc20_abi = file_utilities.load_json(
            filename="bep20" if network == Chain.BSC.database_name else "erc20",
            folder_path=CONFIGURATION.get("data", {}).get("abi_path", None)
            or "data/abi",
        )
    else:
        hypervisor_abi = []
        erc20_abi = []

    if pool_abi_filename:
        pool_abi = file_utilities.load_json(
            filename=pool_abi_filename, folder_path=pool_abi_path
        )
    else:
        pool_abi = []

    # create calls
    return convert_abi_to_calls(
        hypervisor_abi=hypervisor_abi,
        pool_abi=pool_abi,
        erc20_abi=erc20_abi,
        hypervisor_address=hypervisor_address,
        pool_address=pool_address,
        token0_address=token0_address,
        token1_address=token1_address,
    )


def convert_abi_to_calls(
    hypervisor_abi,
    pool_abi,
    erc20_abi,
    hypervisor_address: str,
    pool_address: str,
    token0_address: str,
    token1_address: str,
    _type: list = ["function"],
    stateMutability: list = ["view", "pure"],
    inputs_lte_qtty: int = 0,
) -> list:
    """Create multicall calls from abi s

    Args:
        hypervisor_abi (_type_):
        pool_abi (_type_):
        erc20_abi (_type_):
        hypervisor_address (str):
        pool_address (str):
        token0_address (str):
        token1_address (str):
        _type (list, optional): type's to include. Defaults to ["function"].
        stateMutability (list, optional): stateMutability to include. Defaults to ["view", "pure"].
        inputs_lte_qtty (int, optional): inputs 'lower than' or 'equal to'  to include. Defaults to 0, will include all without inputs.
    Returns:
        list: calls list
    """

    # create calls from those abi s
    calls = []
    for abi_itm in hypervisor_abi:
        if (
            abi_itm["type"] in _type
            and abi_itm["stateMutability"] in stateMutability
            and len(abi_itm["inputs"]) <= inputs_lte_qtty
        ):
            abi_itm["address"] = hypervisor_address
            abi_itm["object"] = "hypervisor"
            calls.append(abi_itm)

    for abi_itm in pool_abi:
        if (
            abi_itm["type"] in _type
            and abi_itm["stateMutability"] in stateMutability
            and len(abi_itm["inputs"]) <= inputs_lte_qtty
        ):
            abi_itm["address"] = pool_address
            abi_itm["object"] = "pool"
            calls.append(abi_itm)

    for abi_itm in erc20_abi:
        if (
            abi_itm["type"] in _type
            and abi_itm["stateMutability"] in stateMutability
            and len(abi_itm["inputs"]) <= inputs_lte_qtty
        ):
            _tmp = copy.deepcopy(abi_itm)
            _tmp["address"] = token0_address
            _tmp["object"] = "token0"
            calls.append(_tmp)

            abi_itm["address"] = token1_address
            abi_itm["object"] = "token1"
            calls.append(abi_itm)

    return calls


def build_call(
    inputs: list,
    outputs: list,
    address: str,
    object: str,
    name: str,
) -> dict:
    return {
        "inputs": inputs,
        "name": name,
        "outputs": outputs,
        "stateMutability": "view",
        "type": "function",
        "address": address,
        "object": object,
    }


def build_call_with_abi_part(
    abi_part: dict,
    inputs_values: list,
    address: str,
    object: str,
) -> dict:
    result = copy.deepcopy(abi_part)
    for idx, input_value in enumerate(inputs_values):
        result["inputs"][idx]["value"] = input_value
    # add address and object
    result["address"] = Web3.toChecksumAddress(address)
    result["object"] = object
    return result
