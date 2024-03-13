from dataclasses import dataclass


from ...general.enums import (
    Protocol,
    rewarderType,
    text_to_protocol,
    text_to_rewarderType,
)


@dataclass
class overwrites:
    gamma_fee: float
    no_operations_before: int
    returns_forced_starting_block: dict[
        str, int
    ]  # <hypervisor address>: <starting block>

    def __post_init__(self):

        # convert to lowercase addresses
        self.returns_forced_starting_block = (
            {k.lower(): v for k, v in self.returns_forced_starting_block.items()}
            if self.returns_forced_starting_block
            else {}
        )

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        return self.__dict__.copy()


@dataclass
class config_protocol:
    protocol: Protocol
    enabled: bool
    hypervisors_registry: list[str]
    rewards_registry: dict[str, rewarderType]
    fee_distributors: list[str]

    def __post_init__(self):
        # convert to protocol enum
        self.protocol = text_to_protocol(self.protocol)

        # convert all addresses to lowercase
        self.hypervisors_registry = (
            [x.lower() for x in self.hypervisors_registry]
            if self.hypervisors_registry
            else []
        )

        # convert to rewarderType enum and lowercase addresses
        self.rewards_registry = (
            {
                k.lower(): text_to_rewarderType(v)
                for k, v in self.rewards_registry.items()
            }
            if self.rewards_registry
            else {}
        )

        # convert all addresses to lowercase
        for type, address_list in self.fee_distributors.items():
            self.fee_distributors[type] = (
                [x.lower() for x in address_list] if address_list else []
            )

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        return self.__dict__.copy()
