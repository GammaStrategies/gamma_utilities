from argparse import Namespace
import os

from ..objects.general import config

# from ...errors.general import ConfigurationError
from ...general.command_line import parse_commandLine_args
from ...log import log_helper
from ...general.enums import text_to_chain, text_to_protocol
from .database import load_configuration_db
from .file import load_configuration_file


def merge_configuration_cml(cfg: config, cml_parameters: Namespace) -> config:
    """Modify configuration object using command line arguments

    Args:
        cfg (config): configuration object
        cml_parameters (Namespace): command line arguments

    Returns:
        config:  modified configuration object
    """

    # add cml_parameters into loaded config
    cfg.cml_parameters = cml_parameters

    # add log subfolder if set
    if cml_parameters.log_subfolder:
        cfg.logs.save_path = os.path.join(
            cfg.logs.save_path, cml_parameters.log_subfolder
        )

    # disable all CHAINS but defined
    if cml_parameters.networks:
        # define enabled chains
        enabled_chains = [text_to_chain(network) for network in cml_parameters.networks]
        # disable all networks not in enabled chains
        for chain in cfg.chains.keys():
            if chain not in enabled_chains:
                cfg.chains[chain].enabled = False

    # disable all PROTOCOLS but defined
    if cml_parameters.protocols:
        # define enabled protocols
        enabled_protocols = [
            text_to_protocol(protocol) for protocol in cml_parameters.protocols
        ]
        # disable all protocols not in enabled protocols
        for chain in cfg.chains.keys():
            for protocol in cfg.chains[chain].protocols.keys():
                if protocol not in enabled_protocols:
                    cfg.chains[chain].protocols[protocol].enabled = False

    # return modified configuration object
    return cfg


def merge_configuration_db(cfg: config, cfg_db: config) -> config:
    """Will use any configuration found in "chains" from database, if not specified in the file configuration

    Args:
        cfg (config): configuration
        cfg_db (config): database configuration

    Returns:
        config: modified configuration
    """

    # only chains
    if cfg_db.chains:
        for chain in cfg_db.chains.keys():
            # if chain is not defined in file configuration
            if chain not in cfg.chains.keys():
                # add chain to file configuration
                cfg.chains[chain] = cfg_db.chains[chain]
            else:
                pass

    # return modified configuration object
    return cfg


def load_configuration() -> config:
    cfg: config = None

    # 1) load configuration using local files
    # convert command line arguments to dict variables
    cml_parameters = parse_commandLine_args()

    # load configuration local files ( if any )
    cfg = (
        load_configuration_file(cfg_name=cml_parameters.config)
        if cml_parameters.config
        else load_configuration_file()
    )

    # 2) merge configuration using command line arguments
    cfg = merge_configuration_cml(cfg, cml_parameters)

    # 3) setup application logging
    log_helper.setup_logging_module(
        path=cfg.logs.path,
        save_path=cfg.logs.save_path,
        log_level=cfg.logs.level,
        telegram_enabled=cfg.logs.telegram.enabled,
        telegram_token=cfg.logs.telegram.token,
        telegram_chatid=cfg.logs.telegram.chat_id,
    )

    # 4) load configuration using database or defaults
    if cfg_db := load_configuration_db():
        # 4.1) merge configuration using database
        cfg = merge_configuration_db(cfg, cfg_db)

    return cfg
