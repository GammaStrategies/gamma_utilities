from dataclasses import dataclass

from ...errors.general import ConfigurationError


@dataclass
class config_w3Providers:
    public: list[str] = None
    private: list[str] = None

    def __post_init__(self):
        # check that one of the two is not None
        if not self.public and not self.private:
            raise ConfigurationError(
                item=self,
                cascade=["w3Providers"],
                action="none",
                message="w3Providers field must have at least one public or private provider",
            )

        # convert all strings are lowercase
        self.public = [x.lower() for x in self.public] if self.public else []
        self.private = [x.lower() for x in self.private] if self.private else []

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        return self.__dict__.copy()
