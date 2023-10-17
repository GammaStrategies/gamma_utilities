from dataclasses import dataclass
import logging

from bins.errors.general import ConfigurationError


from ...general.enums import Chain, Protocol, text_to_chain, text_to_protocol
from .protocols import config_protocol
from .w3providers import config_w3Providers
from .apis import config_apis
from .filters import config_filters
from .prices import config_prices
from .chain_general import config_chain_general


@dataclass
class config_chain:
    chain: Chain
    enabled: bool
    apis: config_apis = None
    w3Providers: config_w3Providers = None
    w3Providers_default_order: list[str] = None

    protocols: dict[Protocol, config_protocol] = None

    general_config: config_chain_general = None

    filters: config_filters = None
    prices: config_prices = None

    def __post_init__(self):
        try:
            # convert to chain enum
            # self.chain = text_to_chain(self.chain)

            if isinstance(self.apis, dict):
                self.apis = config_apis(**self.apis)

            if isinstance(self.w3Providers, dict):
                self.w3Providers = config_w3Providers(**self.w3Providers)

            if isinstance(self.general_config, dict):
                self.general_config = config_chain_general(**self.general_config)

            if isinstance(self.filters, dict):
                self.filters = config_filters(**self.filters)

            if isinstance(self.prices, dict):
                self.prices = config_prices(**self.prices)

            if isinstance(self.protocols, dict):
                if self.protocols:
                    protocols_str = list(self.protocols.keys())
                    for protocol in protocols_str:
                        # pop protocol from dict
                        _temp_val = self.protocols.pop(protocol)
                        # convert protocol to enum
                        _temp_protocol = text_to_protocol(protocol)

                        # check if protocol is already in temp_val
                        if not _temp_val.get("protocol"):
                            _temp_val["protocol"] = _temp_protocol
                        # add protocol to dict
                        self.protocols[_temp_protocol] = config_protocol(**_temp_val)

            if not self.w3Providers_default_order:
                self.w3Providers_default_order = ["public", "private"]

            if self.enabled:
                if not self.w3Providers:
                    raise ValueError(
                        f"w3Providers must be defined for chain {self.chain}"
                    )
                if not self.protocols:
                    raise ValueError(
                        f"protocols must be defined for chain {self.chain}"
                    )
                if not self.prices:
                    raise ValueError(f"prices must be defined for chain {self.chain}")

                if not self.apis:
                    raise ValueError(f"apis must be defined for chain {self.chain}")

        except ConfigurationError as e:
            # add info to configuration error
            e.cascade.append(f"{self.chain}")
            if self.enabled:
                e.action = "exit"
                e.message += f" for chain {self.chain}"
            raise e

        except Exception as e:
            raise e

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        _dict = self.__dict__.copy()
        _dict["apis"] = self.apis.to_dict() if self.apis else None
        _dict["w3Providers"] = self.w3Providers.to_dict() if self.w3Providers else None
        _dict["general_config"] = (
            self.general_config.to_dict() if self.general_config else None
        )
        _dict["filters"] = self.filters.to_dict() if self.filters else None
        _dict["prices"] = self.prices.to_dict() if self.prices else None
        _dict["protocols"] = (
            {
                prot: config_prot.to_dict()
                for prot, config_prot in self.protocols.items()
            }
            if self.protocols
            else None
        )
        return _dict
