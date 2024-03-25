from apps.feeds.latest.mutifeedistribution.currents import (
    feed_latest_multifeedistribution_snapshot,
)
from apps.feeds.latest.price.latest import (
    create_latest_usd_prices_address_json,
    feed_latest_usd_prices,
)
from apps.feeds.price_paths import create_price_paths_json


def main(option: str, **kwargs):
    if option == "latest_prices":
        feed_latest_usd_prices()
    elif option == "create_json_prices":
        create_latest_usd_prices_address_json()
    elif option == "latest_multifeedistributor":
        feed_latest_multifeedistribution_snapshot()
    elif option == "create_price_paths_json":
        create_price_paths_json()
