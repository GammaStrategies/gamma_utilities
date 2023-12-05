# web3 calls for all pools defined fields

import logging
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, text_to_protocol
from bins.w3.builders import build_erc20_helper, build_hypervisor, build_protocol_pool
from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls


def get_info_hypervisors(
    chain: Chain,
    static_hypervisors: list[dict],
    max_calls_atOnce: int = 1000,
    block: int = 0,
    timestamp: int = 0,
    custom_rpcType: str | None = None,
    hypervisor_fn_names: list[str] | None = None,
    pool_fn_names: list[str] | None = None,
):
    """Get hype+pool information using web3 multicall

    Args:
        chain (Chain):
        static_hypervisors (list[dict]):   { "address":<hype address>, "dex":< hype Protocol>}
        max_calls_atOnce (int, optional): . Defaults to 1000.
        block (int, optional): . Defaults to 0.
        timestamp (int, optional): . Defaults to 0.
        custom_rpcType (str, optional): Force the type of RPC to use: either 'private' or 'public' . Defaults to None.
        hypervisor_fn_names (list[str], optional): function names to include (read fn only). Defaults to all read functions.
        pool_fn_names (list[str], optional): function names to include (read fn only) . Defaults to all read functions.

    Returns:
        dict:  {<hypervisor_address>:{
                    "objectType": "hypervisor",
                    ...
                    ...
                },
                <pool_address>:{
                    "objectType": "pool",
                    ...
                    ...
                }}
    """

    # prepare calls for the multicall:
    _calls = []
    if block and timestamp:
        _erc_dummy_obj = build_erc20_helper(
            chain=chain, block=block, timestamp=timestamp
        )
    elif block:
        _erc_dummy_obj = build_erc20_helper(chain=chain, block=block)
    else:
        _erc_dummy_obj = build_erc20_helper(chain=chain)

    block = _erc_dummy_obj.block
    timestamp = _erc_dummy_obj._timestamp

    _pool_addresses_processed = set()
    for _hype in static_hypervisors:
        _hype_dummy_obj = build_hypervisor(
            network=chain.database_name,
            protocol=text_to_protocol(_hype["dex"]),
            block=block,
            timestamp=timestamp,
            hypervisor_address=_hype["address"],
            multicall=True,
        )

        # get all hypervisor's read functions and save em in _calls
        for fn in _hype_dummy_obj.get_abi_functions(
            types=["function"], stateMutabilitys=["view", "pure"], inputs_exist=False
        ):
            # filter by function name
            if hypervisor_fn_names and fn["name"] not in hypervisor_fn_names:
                continue
            #
            _calls.append(
                build_call_with_abi_part(
                    abi_part=fn,
                    inputs_values=[],
                    address=_hype["address"],
                    object="hypervisor",
                )
            )

        if _hype["pool"]["address"] not in _pool_addresses_processed:
            _pool_dummy_obj = build_protocol_pool(
                chain=chain,
                protocol=text_to_protocol(_hype["pool"]["dex"]),
                pool_address=_hype["pool"]["address"],
                block=block,
                timestamp=timestamp,
            )
            # get all pool's read functions and save em in _calls
            for fn in _pool_dummy_obj.get_abi_functions(
                types=["function"], stateMutabilitys=["view", "pure"]
            ):
                # filter by function name
                if pool_fn_names and fn["name"] not in pool_fn_names:
                    continue
                #
                _calls.append(
                    build_call_with_abi_part(
                        abi_part=fn,
                        inputs_values=[],
                        address=_hype["pool"]["address"],
                        object="pool",
                    )
                )

            # add pool address to processed
            _pool_addresses_processed.add(_hype["pool"]["address"])

    # log qtty of calls to be executed
    if static_hypervisors:
        logging.getLogger(__name__).info(
            f" {chain.fantasy_name}: {len(_calls)} fn calls will be executed for {len(static_hypervisors)} hypervisors, meaning {len(_calls)/len(static_hypervisors):.1f} fn calls per hype, that will be splited in {round(len(_calls)/max_calls_atOnce):,.0f} batches of {max_calls_atOnce} function calls, for each web3 call to RPC."
        )

    # place all previously build calls
    hypervisors_info = []
    for i in range(0, len(_calls), max_calls_atOnce):
        hypervisors_info += execute_parse_calls(
            network=chain.database_name,
            block=block,
            calls=_calls[i : i + max_calls_atOnce],
            convert_bint=False,
            requireSuccess=False,
            custom_rpcType=custom_rpcType,
        )

    # parse results to an intelligible structure
    result = {}
    for hinfo in hypervisors_info:
        _address = hinfo["address"].lower()
        _fn_name = hinfo["name"]

        if _address not in result:
            result[_address] = {}
            result[_address]["objectType"] = hinfo["object"]
            result[_address]["block"] = block
            result[_address]["timestamp"] = timestamp

        if _fn_name in result[_address]:
            # multiple hypes have common pools
            if hinfo["object"] != "pool":
                # error
                logging.getLogger(__name__).error(
                    f" function {_fn_name} result is duplicated for {_address}: {result[_address]}. Maybe ABI err?"
                )

        # build field values
        # check if all outputs have something in the "name" key.
        if all([len(x["name"]) > 0 for x in hinfo["outputs"]]):
            # output a dictionary
            result[_address][_fn_name] = {}
        elif len(hinfo["outputs"]) == 1:
            # outpuut 1 value
            result[_address][_fn_name] = ""
        else:
            # output a list
            result[_address][_fn_name] = []

        for output in hinfo["outputs"]:
            if not "value" in output:
                # error
                logging.getLogger(__name__).error(
                    f" hype {hinfo['address']} has no result on {_fn_name}>{output['name'] or output}"
                )
            else:
                if isinstance(result[_address][_fn_name], str):
                    # value
                    result[_address][_fn_name] = output["value"]
                elif isinstance(result[_address][_fn_name], dict):
                    # dictionary
                    result[_address][_fn_name][output["name"]] = output["value"]
                elif isinstance(result[_address][_fn_name], list):
                    # list
                    result[_address][_fn_name].append(output["value"])
                else:
                    # error
                    raise ValueError(
                        f" hype {_address} has no result on {output['name']}"
                    )

    # return result
    return result
