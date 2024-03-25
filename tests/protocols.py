import logging
from apps.feeds.static import (
    _create_hypervisor_static_dbObject,
    _get_static_hypervisor_addresses_to_process,
)
from bins.configuration import CONFIGURATION
from bins.general.enums import Chain, Protocol


def test_protocols(
    networks: list[Chain] | None = None,
    protocols: list[Protocol] | None = None,
    addresses: list[str] | None = None,
):
    """Test protocols classes ( to dict ) and summarize result.
        The test is done in sequence, so it can take a while to complete.
        The test is done on all networks and protocols by default, but can be limited by passing a list of networks and protocols.

    Args:
        networks (list[Chain], optional): list of networks to test. Defaults to None.
        protocols (list[Protocol], optional): list of protocols to test. Defaults to None.
        addresses (list[str], optional): list of addresses to test. Defaults to None.
    """

    networks = (
        [network.database_name for network in networks]
        if networks
        else CONFIGURATION["_custom_"]["cml_parameters"].networks
        or CONFIGURATION["script"]["protocols"]["gamma"]["networks"]
    )
    protocols = (
        [protocol.database_name for protocol in protocols]
        if protocols
        else CONFIGURATION["script"]["protocols"]["gamma"]["networks"][network]
    )

    logging.getLogger(__name__).info(
        f" Testing gamma hypervisors :  {protocols} on {networks}"
    )

    for network in networks:
        for dex in protocols:
            # init result
            result = {
                "total_qtty": 0,
                "ok_address_list": [],
                "error_address_list": [],
            }

            if not addresses:
                (
                    addresses,
                    addresses_disabled,
                ) = _get_static_hypervisor_addresses_to_process(
                    network=network, dex=dex, rewrite=True
                )

            # get hypervisor addresses to process
            for hypervisor_address in addresses:
                logging.getLogger(__name__).debug(
                    f" Testing hypervisor {hypervisor_address} {network} {dex}"
                )
                # add result vars
                result["total_qtty"] += 1
                try:
                    # create hypervisor
                    if hype_dict := _create_hypervisor_static_dbObject(
                        address=hypervisor_address,
                        network=network,
                        dex=dex,
                        enforce_contract_creation=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].enforce_contract_creation,
                    ):
                        # add to result
                        result["ok_address_list"].append(hypervisor_address)
                    else:
                        result["error_address_list"].append(hypervisor_address)
                except Exception as e:
                    result["error_address_list"].append(hypervisor_address)
                    logging.getLogger(__name__).exception(
                        f"Error creating hypervisor {hypervisor_address} {e}"
                    )
            logging.getLogger(__name__).info(
                f" Test protocol result for {network} {dex}:"
            )
            logging.getLogger(__name__).info(f"     total qtty: {result['total_qtty']}")
            logging.getLogger(__name__).info(
                f"      passed: {len(result['ok_address_list'])}"
            )
            logging.getLogger(__name__).info(
                f"      failed: {len(result['error_address_list'])}"
            )
            logging.getLogger(__name__).info(
                f"     error list: {result['error_address_list']}"
            )


# test configuration:
#   yaml
#   multicallv3

# test static hypervisors
# test token prices
# test rewards static
# test status

# test operations gathering
