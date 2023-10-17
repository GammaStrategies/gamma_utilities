from dataclasses import dataclass


from ...general.enums import Protocol, rewarderType, text_to_protocol


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

        # convert all strings are lowercase
        self.hypervisors_registry = (
            [x.lower() for x in self.hypervisors_registry]
            if self.hypervisors_registry
            else []
        )
        self.fee_distributors = (
            [x.lower() for x in self.fee_distributors] if self.fee_distributors else []
        )
        self.rewards_registry = (
            {k.lower(): rewarderType(v) for k, v in self.rewards_registry.items()}
            if self.rewards_registry
            else {}
        )

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        return self.__dict__.copy()
