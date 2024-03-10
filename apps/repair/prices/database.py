import logging
from bins.configuration import CONFIGURATION
from apps.feeds.prices import (
    create_tokenBlocks_all,
    feed_prices,
)


def repair_prices_from_database(
    batch_size: int = 100000, max_repair_per_network: int | None = None
):
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )
        for network in networks:
            logging.getLogger(__name__).info(
                f" > Trying to repair {network}'s prices from database (old prices)"
            )
            try:
                feed_prices(
                    network=network,
                    price_ids=create_tokenBlocks_all(network=network),
                    max_prices=max_repair_per_network,
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Error repairing {network} prices  {e} "
                )
