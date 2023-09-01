# <Chain>
#   enabled = True
#   apis
#      etherscan
#          base url
#          API key
#      coingecko
#          network_id
#      geckoterminal
#          network_id

#   w3Providers
#       public
#         ...
#       private
#         ...

#   protocols
#      <Protocol>
#         enabled = True
#         hypervisors_registry: [0x.]
#         rewards_registry: {
#             <reward_type>: [0x.]
#             }
#         fee_distributors: [0x.]

#   filters
#      ...
#      exclude:
#         token_addresses: {
#             <token_address>: <reason>,
#                 }
#      convert:  #-> used rarely in same price assets
#         token_addresses: {
#             <token_address_from>: <token_address_to>,
#                 }

#   prices:
#      usdc_addresses: [0x.]
#

from dataclasses import dataclass
from bins.general.enums import Chain, Protocol


@dataclass
class config_apis:
    etherscan: dict
    coingecko: dict
    geckoterminal: dict


@dataclass
class config_w3Providers:
    public: dict
    private: dict


@dataclass
class config_protocol:
    protocol: Protocol
    enabled: bool
    hypervisors_registry: list
    rewards_registry: dict
    fee_distributors: list


@dataclass
class config_filters:
    exclude: dict
    convert: dict


@dataclass
class config_prices:
    usdc_addresses: list


@dataclass
class config_chain:
    chain: Chain
    enabled: bool
    apis: config_apis
    w3Providers: config_w3Providers
    protocols: list[config_protocol]
    filters: config_filters
    prices: config_prices
