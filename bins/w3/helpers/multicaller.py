import copy
import logging
from eth_abi import abi
from ..protocols.multicall import multicall3
from ...configuration import CONFIGURATION
from ...general import file_utilities
from ...general.enums import Chain, Protocol
from .. import protocols


def get_hypervisor_asdict(
    chain: Chain,
    protocol: Protocol,
    hypervisor_address: str,
    pool_address: str,
    token0_address: str,
    token1_address: str,
    block: int,
    convert_bint=False,
    static_mode: bool = False,
) -> dict:
    _root_folder_path = (
        CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
    )

    # set defaults
    # HYPE
    hypervisor_abi_filename = protocols.gamma.hypervisor.ABI_FILENAME
    hypervisor_abi_path = (
        f"{_root_folder_path}/{protocols.gamma.hypervisor.ABI_FOLDERNAME}"
    )
    # POOL
    pool_abi_filename = protocols.uniswap.pool.ABI_FILENAME
    pool_abi_path = f"{_root_folder_path}/{protocols.uniswap.pool.ABI_FOLDERNAME}"
    # ERC or BEP
    erc20_abi_filename = "bep20" if chain == Chain.BSC else "erc20"
    erc20_abi_folder_path = _root_folder_path

    # default convert function
    def hypervisor_convert(multicall_result: dict) -> dict:
        # format list vars ( like getTotalAmounts...)
        try:
            multicall_result["getTotalAmounts"] = {
                "total0": multicall_result["getTotalAmounts"][0],
                "total1": multicall_result["getTotalAmounts"][1],
            }
        except:
            pass
        try:
            multicall_result["getLimitPosition"] = {
                "liquidity": multicall_result["getLimitPosition"][0],
                "amount0": multicall_result["getLimitPosition"][1],
                "amount1": multicall_result["getLimitPosition"][2],
            }
        except:
            pass
        try:
            multicall_result["getBasePosition"] = {
                "liquidity": multicall_result["getBasePosition"][0],
                "amount0": multicall_result["getBasePosition"][1],
                "amount1": multicall_result["getBasePosition"][2],
            }
        except:
            pass

        return multicall_result

    def pool_convert(multicall_result: dict) -> dict:
        # pool conversion slot0
        try:
            multicall_result["pool"]["slot0"] = {
                "sqrtPriceX96": multicall_result["pool"]["slot0"][0],
                "tick": multicall_result["pool"]["slot0"][1],
                "observationIndex": multicall_result["pool"]["slot0"][2],
                "observationCardinality": multicall_result["pool"]["slot0"][3],
                "observationCardinalityNext": multicall_result["pool"]["slot0"][4],
                "feeProtocol": multicall_result["pool"]["slot0"][5],
                "unlocked": multicall_result["pool"]["slot0"][6],
            }
        except:
            pass
        return multicall_result

    pool_convert_function = pool_convert

    if protocol in [Protocol.UNISWAPv3, Protocol.GAMMA, Protocol.RETRO, Protocol.SUSHI]:
        # HYPERVISOR DEFAULT
        # POOL DEFAULT
        # CONVERT DEFAULT
        pool_dex = Protocol.UNISWAPv3
    elif protocol in [
        Protocol.QUICKSWAP,
        Protocol.GLACIER,
        Protocol.STELLASWAP,
        Protocol.SYNTHSWAP,
        Protocol.ZYBERSWAP,
    ]:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.algebra.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.algebra.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.algebra.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.algebra.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.ALGEBRAv3

        # CONVERT
        def pool_convert(multicall_result: dict) -> dict:
            # format list vars ( like getTotalAmounts...)
            try:
                multicall_result["pool"]["globalState"] = {
                    "sqrtPriceX96": multicall_result["pool"]["globalState"][0],
                    "tick": multicall_result["pool"]["globalState"][1],
                    "fee": multicall_result["pool"]["globalState"][2],
                    "timepointIndex": multicall_result["pool"]["globalState"][3],
                    "communityFeeToken0": multicall_result["pool"]["globalState"][4],
                    "communityFeeToken1": multicall_result["pool"]["globalState"][5],
                    "unlocked": multicall_result["pool"]["globalState"][6],
                }
            except:
                pass
            return multicall_result

        pool_convert_function = pool_convert
    elif protocol == Protocol.THENA:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.algebra.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.algebra.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.thena.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.thena.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.THENA

        # CONVERT
        def pool_convert(multicall_result: dict) -> dict:
            # format list vars ( like getTotalAmounts...)
            try:
                multicall_result["pool"]["globalState"] = {
                    "sqrtPriceX96": multicall_result["pool"]["globalState"][0],
                    "tick": multicall_result["pool"]["globalState"][1],
                    "fee": multicall_result["pool"]["globalState"][2],
                    "timepointIndex": multicall_result["pool"]["globalState"][3],
                    "communityFeeToken0": multicall_result["pool"]["globalState"][4],
                    "communityFeeToken1": multicall_result["pool"]["globalState"][5],
                    "unlocked": multicall_result["pool"]["globalState"][6],
                }
            except:
                pass

            return multicall_result

        pool_convert_function = pool_convert
    elif protocol == Protocol.CAMELOT:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.algebra.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.algebra.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.camelot.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.camelot.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.CAMELOT

        # CONVERT
        def pool_convert(multicall_result: dict) -> dict:
            # format list vars ( like getTotalAmounts...)
            try:
                multicall_result["pool"]["globalState"] = {
                    "sqrtPriceX96": multicall_result["pool"]["globalState"][0],
                    "tick": multicall_result["pool"]["globalState"][1],
                    "fee": multicall_result["pool"]["globalState"][2],
                    "timepointIndex": multicall_result["pool"]["globalState"][4],
                    "communityFeeToken0": multicall_result["pool"]["globalState"][5],
                    "communityFeeToken1": multicall_result["pool"]["globalState"][5],
                    "unlocked": multicall_result["pool"]["globalState"][6],
                    # special
                    "feeZto": multicall_result["pool"]["globalState"][2],
                    "feeOtz": multicall_result["pool"]["globalState"][3],
                }
            except:
                pass

            return multicall_result

        pool_convert_function = pool_convert
    elif protocol == Protocol.BEAMSWAP:
        # HYPERVISOR DEFAULT
        # POOL
        pool_abi_filename = protocols.beamswap.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.beamswap.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.BEAMSWAP
        # CONVERT DEFAULT
    elif protocol == Protocol.RAMSES:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.ramses.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.ramses.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.ramses.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.ramses.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.RAMSES
        # CONVERT DEFAULT
    elif protocol == Protocol.SPIRITSWAP:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.algebra.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.algebra.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.spiritswap.pool.ABI_FILENAME
        pool_abi_path = (
            f"{_root_folder_path}/{protocols.spiritswap.pool.ABI_FOLDERNAME}"
        )
        pool_dex = Protocol.SPIRITSWAP

        # CONVERT
        def pool_convert(multicall_result: dict) -> dict:
            # format list vars ( like getTotalAmounts...)
            try:
                multicall_result["pool"]["globalState"] = {
                    "sqrtPriceX96": multicall_result["pool"]["globalState"][0],
                    "tick": multicall_result["pool"]["globalState"][1],
                    "prevInitializedTick": multicall_result["pool"]["globalState"][2],
                    "fee": multicall_result["pool"]["globalState"][3],
                    "timepointIndex": multicall_result["pool"]["globalState"][4],
                    "communityFeeToken0": multicall_result["pool"]["globalState"][5],
                    "communityFeeToken1": multicall_result["pool"]["globalState"][5],
                    "unlocked": multicall_result["pool"]["globalState"][6],
                }
            except:
                pass

            return multicall_result

        pool_convert_function = pool_convert
    elif protocol == Protocol.FUSIONX:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.fusionx.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.fusionx.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.fusionx.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.fusionx.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.FUSIONX
        # CONVERT DEFAULT
    elif protocol == Protocol.LYNEX:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.algebra.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.algebra.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.lynex.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.lynex.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.LYNEX

        # CONVERT
        def pool_convert(multicall_result: dict) -> dict:
            # format list vars ( like getTotalAmounts...)
            try:
                multicall_result["pool"]["globalState"] = {
                    "sqrtPriceX96": multicall_result["pool"]["globalState"][0],
                    "tick": multicall_result["pool"]["globalState"][1],
                    "fee": multicall_result["pool"]["globalState"][2],
                    "timepointIndex": multicall_result["pool"]["globalState"][3],
                    "communityFeeToken0": multicall_result["pool"]["globalState"][4],
                    "communityFeeToken1": multicall_result["pool"]["globalState"][5],
                    "unlocked": multicall_result["pool"]["globalState"][6],
                }
            except:
                pass

            return multicall_result

        pool_convert_function = pool_convert
    elif protocol == Protocol.PEGASYS:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.pegasys.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.pegasys.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.pegasys.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.pegasys.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.PEGASYS
        # CONVERT DEFAULT
    elif protocol == Protocol.ASCENT:
        # HYPERVISOR DEFAULT
        hypervisor_abi_filename = protocols.ascent.hypervisor.ABI_FILENAME
        hypervisor_abi_path = (
            f"{_root_folder_path}/{protocols.ascent.hypervisor.ABI_FOLDERNAME}"
        )
        # POOL
        pool_abi_filename = protocols.ascent.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.ascent.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.ASCENT
        # CONVERT DEFAULT
    elif protocol == Protocol.PANCAKESWAP:
        # HYPERVISOR DEFAULT
        # POOL
        pool_abi_filename = protocols.pancakeswap.pool.ABI_FILENAME
        pool_abi_path = (
            f"{_root_folder_path}/{protocols.pancakeswap.pool.ABI_FOLDERNAME}"
        )
        pool_dex = Protocol.PANCAKESWAP
        # CONVERT DEFAULT
    elif protocol == Protocol.BASEX:
        # HYPERVISOR DEFAULT
        # POOL
        pool_abi_filename = protocols.basex.pool.ABI_FILENAME
        pool_abi_path = f"{_root_folder_path}/{protocols.basex.pool.ABI_FOLDERNAME}"
        pool_dex = Protocol.BASEX
        # CONVERT DEFAULT
    else:
        raise NotImplementedError(
            f"Protocol {protocol} not implemented for multicall yet."
        )

    # build calls
    hypervisor_abi = file_utilities.load_json(
        filename=hypervisor_abi_filename,
        folder_path=hypervisor_abi_path,
    )
    erc20_abi = file_utilities.load_json(
        filename=erc20_abi_filename,
        folder_path=erc20_abi_folder_path,
    )
    pool_abi = file_utilities.load_json(
        filename=pool_abi_filename,
        folder_path=pool_abi_path,
    )
    calls = convert_abi_to_calls(
        hypervisor_abi=hypervisor_abi,
        pool_abi=pool_abi,
        erc20_abi=erc20_abi,
        hypervisor_address=hypervisor_address,
        pool_address=pool_address,
        token0_address=token0_address,
        token1_address=token1_address,
    )

    # execute call
    multicall_helper = multicall3(
        network=chain.database_name,
        block=block,
    )
    multicall_raw_result = multicall_helper.try_get_data(calls)

    # parse calls
    result = {}
    for idx, call_result in enumerate(multicall_raw_result):
        if call_result[0]:
            # success
            data = abi.decode(
                [out["type"] for out in calls[idx]["outputs"]],
                call_result[1],
            )

            # build result key = function name
            main_key = calls[idx]["object"]
            key = calls[idx]["name"]

            # set root to modify
            _root = result
            if main_key != "hypervisor":
                # create root key if not exist or its an address
                if not main_key in result or (
                    isinstance(result[main_key], str)
                    and result[main_key].startswith("0x")
                ):
                    result[main_key] = {}
                # set root
                _root = result[main_key]

            # set var context
            if len(calls[idx]["outputs"]) > 1:
                if not [1 for x in calls[idx]["outputs"] if x["name"] != ""]:
                    # dictionary
                    if not key in _root:
                        _root[key] = {}
                else:
                    # list
                    if not key in _root:
                        _root[key] = []
            else:
                # one item
                if not key in _root:
                    _root[key] = None

            # loop thru output
            for output_idx, output in enumerate(calls[idx]["outputs"]):
                # should convert to str? (timestamps, blocks , decimals, fee, feeProtocol are not converted)
                if (
                    convert_bint
                    and isinstance(data[output_idx], int)
                    and calls[idx]["name"]
                    not in ["timestamp", "block", "decimals", "fee", "feeProtocol"]
                ):
                    _value = str(data[output_idx])
                else:
                    _value = data[output_idx]

                # add to result
                if isinstance(_root[key], list):
                    _root[key].append(_value)
                elif isinstance(_root[key], dict):
                    _root[key][output["name"]] = _value
                else:
                    _root[key] = _value

    # convert hypervisor result
    result = hypervisor_convert(result)
    # convert pool result
    result = pool_convert_function(result)

    # remove unwanted hypervisor keys
    for k in [
        "DOMAIN_SEPARATOR",
        "PRECISION",
        "directDeposit",
        "owner",
        "whitelistedAddress",
    ]:
        try:
            result.pop(k)
        except:
            pass

    # remove unwanted pool keys??

    # move token0,token1 to pool
    if isinstance(result["token0"], dict):
        result["pool"]["token0"] = result.pop("token0")
    if isinstance(result["token1"], dict):
        result["pool"]["token1"] = result.pop("token1")

    # add dex fields
    result["dex"] = protocol.database_name
    result["pool"]["dex"] = pool_dex.database_name

    result["block"] = multicall_helper.block
    result["timestamp"] = multicall_helper._timestamp

    return result


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
    multicall_raw_result = _get_multicall_result(
        network=network, block=block, calls=calls
    )
    return parse_multicall_result(
        calls=calls,
        multicall_raw_result=multicall_raw_result,
        convert_bint=convert_bint,
    )


def _get_multicall_result(network: str, block: int, calls: list):
    # execute call
    multicall_helper = multicall3(
        network=network,
        block=block,
    )
    return multicall_helper.try_get_data(calls)


def parse_multicall_result(
    calls: list, multicall_raw_result: list, convert_bint: bool = False
):
    """_summary_

    Args:
        calls (list): _description_
        multicall_raw_result (list): _description_
        convert_bint (bool, optional): _description_. Defaults to False.

    Returns:
        dict: dictionary result
    """

    # parse result with calls
    result = {}
    for idx, call_result in enumerate(multicall_raw_result):
        if call_result[0] and call_result[1]:
            # check if there are bytes to process ( cause success without bytes is a fail)
            # success
            data = abi.decode(
                [out["type"] for out in calls[idx]["outputs"]],
                call_result[1],
            )

            # build result key = function name
            # object contains .
            main_key = calls[idx]["object"]
            key = calls[idx]["name"]

            # set root to modify
            _root = result
            if main_key != "hypervisor":
                # create root key if not exist or its an address
                if not main_key in result or (
                    isinstance(result[main_key], str)
                    and result[main_key].startswith("0x")
                ):
                    result[main_key] = {}
                # set root
                _root = result[main_key]

            # set var context
            if len(calls[idx]["outputs"]) > 1:
                if not [1 for x in calls[idx]["outputs"] if x["name"] != ""]:
                    # dictionary
                    if not key in _root:
                        _root[key] = {}
                else:
                    # list
                    if not key in _root:
                        _root[key] = []
            else:
                # one item
                if not key in _root:
                    _root[key] = None

            # loop thru output
            for output_idx, output in enumerate(calls[idx]["outputs"]):
                # should convert to str? (timestamps, blocks , decimals, fee, feeProtocol are not converted)
                if (
                    convert_bint
                    and isinstance(data[output_idx], int)
                    and calls[idx]["name"]
                    not in ["timestamp", "block", "decimals", "fee", "feeProtocol"]
                ):
                    _value = str(data[output_idx])
                else:
                    _value = data[output_idx]

                # addresses will always be low cased

                # add to result
                if isinstance(_root[key], list):
                    _root[key].append(_value)
                elif isinstance(_root[key], dict):
                    _root[key][output["name"]] = _value
                else:
                    _root[key] = _value
        else:
            if call_result[0]:
                # success without bytes is a pain
                logging.getLogger(__name__).warning(
                    f" {calls[idx]['name']} has not been returned in the multicall address: {calls[idx].get('address','')} (success without bytes)"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" {calls[idx]['name']} has not been returned in the multicall address: {calls[idx].get('address','')}"
                )

    return result


def parse_multicall_result_2(
    calls: list, multicall_raw_result: list, convert_bint: bool = False
):
    """Parse multicall result with calls
        call object is used to form dict keys ( key.subkey = {key:{subkey:value}} )

    Args:
        calls (list): list of calls {
            "outputs":[{"type":...}, ...]
            "inputs:[],
            "name"...
            ...
        }
        multicall_raw_result (list): multicall result
        convert_bint (bool, optional): convert big integers to string. Defaults to False.

    Returns:
        dict: dictionary result
    """

    # parse result with calls
    result = {}
    for idx, call_result in enumerate(multicall_raw_result):
        if call_result[0] and call_result[1]:
            # check if there are bytes to process ( cause success without bytes is a fail)
            # success
            data = abi.decode(
                [out["type"] for out in calls[idx]["outputs"]],
                call_result[1],
            )

            # set root to modify
            object_keys = calls[idx]["object"].split(".")
            key = calls[idx]["name"]
            _root = result
            for obj_key in object_keys:
                # create result key if needed
                if obj_key not in result:
                    _root[obj_key] = None
                # set root
                _root = _root[obj_key]

            # set var context ( dict, list or any)
            if len(calls[idx]["outputs"]) > 1:
                # multiple items output
                if not [1 for x in calls[idx]["outputs"] if x["name"] != ""]:
                    # dictionary if all outputs have a defined name
                    if not key in _root:
                        _root[key] = {}
                else:
                    # list if any output has an undefined name
                    if not key in _root:
                        _root[key] = []
            else:
                # one item output
                if not key in _root:
                    _root[key] = None

            # set variable: loop thru output
            for output_idx, output in enumerate(calls[idx]["outputs"]):
                # should convert to str? (timestamps, blocks , decimals, fee, feeProtocol are not converted)
                if (
                    convert_bint
                    and isinstance(data[output_idx], int)
                    and calls[idx]["name"]
                    not in ["timestamp", "block", "decimals", "fee", "feeProtocol"]
                ):
                    _value = str(data[output_idx])
                else:
                    _value = data[output_idx]

                # add to result
                if isinstance(_root[key], list):
                    _root[key].append(_value)
                elif isinstance(_root[key], dict):
                    _root[key][output["name"]] = _value
                else:
                    _root[key] = _value
        else:
            if call_result[0]:
                # success without bytes is a pain
                logging.getLogger(__name__).warning(
                    f" {calls[idx]['name']} has not been returned in the multicall address: {calls[idx].get('address','')} (success without bytes)"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" {calls[idx]['name']} has not been returned in the multicall address: {calls[idx].get('address','')}"
                )

    return result


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


# OOO = [
#     {
#         # where in result to place this call (when dot is used, left most will be treated as dict till last var is set)
#         "object":"hypervisor.pool",
#         "inputs": [],
#         "name": "decimals",
#         "outputs": [
#             {
#                 "internalType": "uint8",
#                 "name": "",
#                 "type": "uint8"
#             }
#         ],
#         "stateMutability": "view",
#         "type": "function"
#     }
# ]
