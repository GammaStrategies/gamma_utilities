from dataclasses import dataclass


@dataclass
class config_prices:
    usdc_addresses: list

    def __post_init__(self):
        # convert all strings are lowercase
        self.usdc_addresses = (
            [x.lower() for x in self.usdc_addresses] if self.usdc_addresses else []
        )
