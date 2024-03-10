from apps.checks.telegram_special.analytics import (
    telegram_checks as analytics_telegram_checks,
)
from bins.configuration import CONFIGURATION
from bins.general.enums import Chain, text_to_chain, text_to_protocol
from apps.checks.analytics.check_hypervisor_analytics import check_hypervisors_analytics


def check_analytics():

    # CHAIN/PROTOCOL FEEDS
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:

            if CONFIGURATION["_custom_"]["cml_parameters"].hypervisor_addresses:
                for address in CONFIGURATION["_custom_"][
                    "cml_parameters"
                ].hypervisor_addresses:
                    check_hypervisors_analytics(
                        chain=text_to_chain(network),
                        hypervisor_address=address,
                    )
            elif CONFIGURATION["_custom_"]["cml_parameters"].hypervisor_address:
                check_hypervisors_analytics(
                    chain=text_to_chain(network),
                    hypervisor_address=CONFIGURATION["_custom_"][
                        "cml_parameters"
                    ].hypervisor_address,
                )
            else:

                protocols = None
                if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                    # create a list of protocols to process
                    protocols = [
                        text_to_protocol(protocol)
                        for protocol in CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].protocols
                        if protocol
                        in CONFIGURATION["script"]["protocols"][protocol][
                            "networks"
                        ].get(network, [])
                    ]
                    if len(protocols) == 0:
                        protocols = None

                # check hypervisor analytics
                check_hypervisors_analytics(
                    chain=text_to_chain(network), protocols=protocols
                )


def check_analytics_telegram():
    # CHAIN/PROTOCOL FEEDS
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:

            hypervisor_addresses = CONFIGURATION["_custom_"][
                "cml_parameters"
            ].hypervisor_addresses
            if CONFIGURATION["_custom_"]["cml_parameters"].hypervisor_address:
                hypervisor_addresses = [
                    CONFIGURATION["_custom_"]["cml_parameters"].hypervisor_address
                ]
            protocols = None
            if not hypervisor_addresses:
                if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                    # create a list of protocols to process
                    protocols = [
                        text_to_protocol(protocol)
                        for protocol in CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].protocols
                        if protocol
                        in CONFIGURATION["script"]["protocols"][protocol][
                            "networks"
                        ].get(network, [])
                    ]
                    if len(protocols) == 0:
                        protocols = None

            # telegram monitoring
            analytics_telegram_checks(
                chain=text_to_chain(network),
                protocols=protocols,
                hypervisor_addresses=hypervisor_addresses,
            )
