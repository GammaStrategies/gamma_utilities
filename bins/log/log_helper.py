import os
import yaml
import logging.config
import logging
from . import telegram_logger


def setup_logging(customconf, default_level=logging.INFO, env_key="LOG_CFG"):
    """Setup logging configuration

    Arguments:
       config {json} -- custom configuration json file

    Keyword Arguments:
       default_path {str} -- path to yaml config log file (default: {'logging.yaml'})
       default_level {int} --  (default: {logging.INFO})
       env_key {str} --     (default: {'LOG_CFG'})
    """

    # load telegram chatid n token
    telegram_logger.TELEGRAM_ENABLED = (
        customconf.get("logs", {}).get("telegram", {}).get("enabled", False)
    )
    telegram_logger.TELEGRAM_TOKEN = (
        customconf.get("logs", {}).get("telegram", {}).get("token", "")
    )
    telegram_logger.TELEGRAM_CHAT_ID = (
        customconf.get("logs", {}).get("telegram", {}).get("chat_id", "")
    )

    # create logs dir if not exists
    if not os.path.exists(customconf["logs"]["save_path"]):
        os.makedirs(name=customconf["logs"]["save_path"], exist_ok=True)

    # load log configuration file
    if os.path.exists(customconf["logs"]["path"]):
        with open(customconf["logs"]["path"], "rt", encoding="utf8") as f:
            try:
                # load yaml to var
                config = yaml.safe_load(f.read())

                # modify all filenames in handlers with the choosen path
                for k, v in config["handlers"].items():
                    try:
                        config["handlers"][k]["filename"] = os.path.join(
                            customconf["logs"]["save_path"], v["filename"]
                        )
                    except Exception:
                        # many handlers do not have filename key
                        pass

                # modify logging type if defined
                _log_level = customconf["logs"].get("level", "INFO").upper()
                if not _log_level in ["DEBUG", "INFO"]:
                    _log_level = "INFO"

                _log_level_donotmod = ["telegram"]
                for k, v in config["loggers"].items():
                    if k not in _log_level_donotmod:
                        v["level"] = _log_level

                logging.config.dictConfig(config)

                # setup color
                # coloredlogs.install()
            except Exception as e:
                print(e)
                print("Error in Logging Configuration. Using default configs")
                logging.basicConfig(level=default_level)
                # coloredlogs.install(level=default_level)
                logging.getLogger(__name__).error(
                    f"Failed to load Logging configuration file {customconf['logs']['path']} error: {e}. Using default configs"
                )

    else:
        logging.basicConfig(level=default_level)
        logging.getLogger(__name__).error(
            f"Failed to load Logging configuration file. {customconf['logs']['path']} does not exist. Using default configs"
        )
        # coloredlogs.install(level=default_level)
        print(
            f"Failed to load Logging configuration file. {customconf['logs']['path']} does not exist. Using default configs"
        )

    # setup custom duplicate filter
    logging.getLogger().addFilter(DuplicateFilter())  # add the filter to it


class infoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.INFO


class DuplicateFilter(logging.Filter):
    def filter(self, record):
        # add other fields if you need more granular comparison, depends on your app
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, "last_log", None):
            self.last_log = current_log
            return True
        return False
