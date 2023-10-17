from dataclasses import dataclass


@dataclass
class config_telegram:
    enabled: bool = False  # enable or disable telegram
    token: str = None
    chat_id: str = None

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        return self.__dict__.copy()


@dataclass
class config_logs:
    path: str = "bins/log/logging.yaml"  #  path to log yaml configuration file <relative to app>
    save_path: str = "logs/"  # log folder <relative to app> where to save log files
    level: str = "INFO"  # choose btween INFO and DEBUG

    # debug.log file is populated with functions execution time
    log_execution_time: bool = False
    telegram: config_telegram = None

    def __post_init__(self):
        if isinstance(self.telegram, dict):
            self.telegram = config_telegram(**self.telegram)

        # init telegram
        if not self.telegram:
            self.telegram = config_telegram()

    def to_dict(self):
        """convert object and subobjects to dictionary"""
        _dict = self.__dict__.copy()
        _dict["telegram"] = self.telegram.to_dict()
        return _dict
