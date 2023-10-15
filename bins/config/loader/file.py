import os
import sys
import yaml
from yaml.parser import ParserError
from ..objects.general import config
from ...errors.general import ConfigurationError


def load_configuration_file(cfg_name="configuration.yaml") -> config:
    """Load and return configuration object
       "config.yaml" file should be placed in root

    Returns:
       [configuration object]
    """
    if os.path.exists(cfg_name):
        with open(cfg_name, "rt", encoding="utf8") as f:
            # try:
            return config(**yaml.safe_load(f.read()))

            # except ConfigurationError as e:
            #     if e.action == "exit":
            #         print(
            #             f" Error loading configuration yaml file {cfg_name}: {e.message}"
            #         )
            #         sys.exit(1)
            #     print(f"Error loading configuration yaml file {cfg_name}: {e}")
            # except ParserError as e:
            #     print(f"Error parsing configuration yaml file {cfg_name}: {e}")
            # except Exception as e:
            #     print(
            #         f" Unknown error found while loading configuration file {cfg_name}: {e}"
            #     )
    else:
        raise FileNotFoundError(f" {cfg_name} configuration file not found")
