from bins.general.enums import Chain, Protocol, rewarderType
from .objects import (
    config_apis,
    config_chain,
    config_filters,
    config_prices,
    config_protocol,
    config_w3Providers,
)

# Configure Ethereum chain >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
eth = config_chain(
    chain=Chain.ETHEREUM,
    enabled=True,
    apis=config_apis(etherscan={}, coingecko={}, geckoterminal={}),
    w3Providers=config_w3Providers(public={}, private={}),
    protocols=[
        config_protocol(
            protocol=Protocol.UNISWAPv3,
            enabled=True,
            hypervisors_registry=[],
            rewards_registry={},
            fee_distributors=[],
        ),
    ],
    filters=config_filters(exclude={}, convert={}),
    prices=config_prices(usdc_addresses=[]),
)
# end Ethereum    <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
