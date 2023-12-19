from apps.analysis.benchmark import benchmark_logs_analysis
from apps.analysis.network import sumary_network
from apps.analysis.queue import analyze_queues
from apps.analysis.user import analyze_user_transactions, sumary_user
from apps.feeds.reports.execution import feed_global_reports
from bins.general.enums import Chain, queueItemType, reportType, text_to_chain
from bins.configuration import CONFIGURATION


def main(option: str, **kwargs):
    # get dates range from command line
    try:
        ini_datetime = CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime
    except Exception:
        ini_datetime = None
    try:
        end_datetime = CONFIGURATION["_custom_"]["cml_parameters"].end_datetime
    except Exception:
        end_datetime = None

    if option == "user":
        # check if user address to analyze
        if CONFIGURATION["_custom_"]["cml_parameters"].user_address:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    sumary_user(
                        network=network,
                        protocol="gamma",
                        user_address=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].user_address.lower(),
                        ini_date=ini_datetime,
                        end_date=end_datetime,
                    )
        else:
            raise ValueError("user address not provided. Use --user_address <address>")
    elif option == "network":
        for protocol in CONFIGURATION["script"]["protocols"]:
            # override networks if specified in cml
            networks = (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"][protocol]["networks"]
            )
            for network in networks:
                # execute summary
                sumary_network(
                    network=network,
                    protocol=protocol,
                    ini_date=ini_datetime,
                    end_date=end_datetime,
                )
    elif option == "benchmark_logs":
        benchmark_logs_analysis()
    elif option == "queue":
        for protocol in CONFIGURATION["script"]["protocols"]:
            # override networks if specified in cml
            networks = (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"][protocol]["networks"]
            )

            analyze_queues(chains=[text_to_chain(network) for network in networks])
    elif option == "user_deposits":
        # hypervisor addresses must be provided

        print(
            analyze_user_transactions(
                hypervisor_addresses=CONFIGURATION["_custom_"][
                    "cml_parameters"
                ].hypervisor_addresses,
                user_addresses=CONFIGURATION["_custom_"]["cml_parameters"].user_address,
                send_to_telegram=CONFIGURATION["_custom_"][
                    "cml_parameters"
                ].send_to_telegram,
            )
        )
        # elif CONFIGURATION["_custom_"]["cml_parameters"].user_address:
        #     # TODO: replace with user addresses
        #     print(
        #         analyze_user_deposits(

        #             send_to_telegram=CONFIGURATION["_custom_"][
        #                 "cml_parameters"
        #             ].send_to_telegram,
        #         )
        #     )

        # else:
        #     analyze_user_deposits(
        #             send_to_telegram=CONFIGURATION["_custom_"][
        #                 "cml_parameters"
        #             ].send_to_telegram,
        #         )
        #     raise ValueError(
        #         "no addresses provided to analyze. Use --hypervisor_addresses '<address> <address> ...'   or --user_addresses '<address> <address> ...'"
        #     )
    elif option == "global_reports":
        feed_global_reports()
