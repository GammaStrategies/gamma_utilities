from dataclasses import dataclass


@dataclass
class config_chain_angle_merkl:
    distributor: str
    distributionCreator: str
    coreMerkl: str

    def __post_init__(self):
        # convert all strings are lowercase
        self.distributor = self.distributor.lower() if self.distributor else None
        self.distributionCreator = (
            self.distributionCreator.lower() if self.distributionCreator else None
        )
        self.coreMerkl = self.coreMerkl.lower() if self.coreMerkl else None


@dataclass
class config_chain_general:
    angle_merkl: config_chain_angle_merkl = None

    def __post_init__(self):
        if isinstance(self.angle_merkl, dict):
            self.angle_merkl = config_chain_angle_merkl(**self.angle_merkl)
