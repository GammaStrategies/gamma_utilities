import os
from bins.general.command_line import parse_commandLine_args
from bins.general.general_utilities import check_configuration_file, load_configuration
from bins.log import log_helper

# configuration file
CONFIGURATION = {}


# 1) load configuration using local files
# convert command line arguments to dict variables
cml_parameters = parse_commandLine_args()
# load configuration local files ( if any )
CONFIGURATION = (
    load_configuration(cfg_name=cml_parameters.config)
    if cml_parameters.config
    else load_configuration()
)


# 2) validate configuration file format
check_configuration_file(CONFIGURATION)


# 3) add cml_parameters into loaded config ( this is used later on to load again the config file to be able to update on-the-fly vars)
if "_custom_" not in CONFIGURATION.keys():
    CONFIGURATION["_custom_"] = {}
CONFIGURATION["_custom_"]["cml_parameters"] = cml_parameters

# add log subfolder if set
if CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder:
    CONFIGURATION["logs"]["save_path"] = os.path.join(
        CONFIGURATION["logs"]["save_path"],
        CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder,
    )


# 4) setup application logging
log_helper.setup_logging(customconf=CONFIGURATION)

# add temporal variables while the app is running so memory is kept
CONFIGURATION["_custom_"]["temporal_memory"] = {}


# 5) load configuration using database or defaults
