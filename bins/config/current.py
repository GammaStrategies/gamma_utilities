import sys
from yaml.parser import ParserError
from ..errors.general import ConfigurationError
from .loader.all import load_configuration

# load current configuratoion
CFG = load_configuration()
