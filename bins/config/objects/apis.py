from dataclasses import dataclass


@dataclass
class config_etherscan_api:
    key: str
    url: str


@dataclass
class config_common_api:
    network_id: str


@dataclass
class config_apis:
    etherscan: config_etherscan_api
    coingecko: config_common_api
    geckoterminal: config_common_api

    def __post_init__(self):
        if isinstance(self.etherscan, dict):
            self.etherscan = config_etherscan_api(**self.etherscan)
        if isinstance(self.coingecko, dict):
            self.coingecko = config_common_api(**self.coingecko)
        if isinstance(self.geckoterminal, dict):
            self.geckoterminal = config_common_api(**self.geckoterminal)
