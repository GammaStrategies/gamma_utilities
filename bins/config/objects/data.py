from dataclasses import dataclass

from bins.errors.general import ConfigurationError


@dataclass
class config_cache:
    enabled: bool = True
    save_path: str = "data/cache"  # path to cache folder <relative to app>


@dataclass
class config_database:
    mongo_server_url: str = "mongodb://localhost:27017"

    def __post_init__(self):
        # make sure mongo_server_url is formated correctly
        if not self.mongo_server_url.startswith("mongodb://"):
            raise ConfigurationError(
                item=self,
                cascade=["database"],
                action="exit",
                message="mongo_server_url must start with mongodb://",
            )


@dataclass
class config_data:
    database: config_database
    abi_path: str = "data/abi"  # path to abi folder <relative to app>
    cache: config_cache = None

    def __post_init__(self):
        if isinstance(self.database, dict):
            self.database = config_database(**self.database)
        if isinstance(self.cache, dict):
            self.cache = config_cache(**self.cache)
        # init cache
        if not self.cache:
            self.cache = config_cache()
