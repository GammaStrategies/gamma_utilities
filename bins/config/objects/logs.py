from dataclasses import dataclass


@dataclass
class config_telegram:
    enabled: bool = False  # enable or disable telegram
    token: str = None
    chat_id: str = None


@dataclass
class config_logs:
    path: str = "bins/log/logging.yaml"  #  path to log yaml configuration file <relative to app>
    save_path: str = "logs/"  # log folder <relative to app> where to save log files
    level: str = "INFO"  # choose btween INFO and DEBUG
    log_execution_time: bool = (
        False  # debug.log file is populated with functions execution time
    )
    telegram: config_telegram = None

    def __post_init__(self):
        if isinstance(self.telegram, dict):
            self.telegram = config_telegram(**self.telegram)

        # init telegram
        if not self.telegram:
            self.telegram = config_telegram()
