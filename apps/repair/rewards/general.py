from apps.repair.rewards.missing import repair_missing_rewards_status
from bins.configuration import CONFIGURATION
from bins.general.enums import text_to_chain


def repair_rewards_status():
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            repair_missing_rewards_status(
                chain=text_to_chain(network),
                max_repair=CONFIGURATION["_custom_"]["cml_parameters"].maximum,
            )
