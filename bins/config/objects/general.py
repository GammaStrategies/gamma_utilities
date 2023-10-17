from argparse import Namespace
from dataclasses import dataclass
import logging

from ...errors.general import ConfigurationError
from ...general.enums import Chain, text_to_chain
from .logs import config_logs
from .script import config_script
from .chain import config_chain
from .data import config_data


@dataclass
class config:
    logs: config_logs
    data: config_data
    script: config_script
    chains: dict[Chain, config_chain] | None = None
    cml_parameters: Namespace | None = None
    id: str = "client_configuration"

    def __post_init__(self):
        if isinstance(self.logs, dict):
            self.logs = config_logs(**self.logs)

        if isinstance(self.data, dict):
            self.data = config_data(**self.data)

        if isinstance(self.script, dict):
            self.script = config_script(**self.script)

        if isinstance(self.chains, dict):
            if self.chains:
                chains_str = list(self.chains.keys())
                for chain in chains_str:
                    try:
                        # pop chain from dict
                        _temp_val = self.chains.pop(chain)
                        # convert chain to enum
                        _temp_chain = text_to_chain(chain)

                        # check if chain is already in temp_val
                        if not _temp_val.get("chain"):
                            _temp_val["chain"] = _temp_chain
                        # add chain to dict
                        self.chains[_temp_chain] = config_chain(**_temp_val)
                    except ConfigurationError as e:
                        if e.action == "exit":
                            raise e
                        logging.getLogger(__name__).error(
                            f" Can't load {chain} chain configuration: {e}"
                        )
                    except Exception as e:
                        logging.getLogger(__name__).exception(
                            f" Can't load {chain} chain configuration: {e}"
                        )

        # init logs
        if not self.logs:
            self.logs = config_logs()
        # init data
        if not self.data:
            self.data = config_data()

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        _dict = self.__dict__.copy()
        _dict["logs"] = self.logs.to_dict() if self.logs else None
        _dict["data"] = self.data.to_dict() if self.data else None
        _dict["script"] = self.script.to_dict() if self.script else None
        _dict["chains"] = {}
        if self.chains:
            for chain in self.chains:
                _dict["chains"][chain] = self.chains[chain].to_dict()

        return _dict
