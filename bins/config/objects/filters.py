from dataclasses import dataclass


@dataclass
class config_exclude:
    hypervisors: list[str]
    tokens: list[str]

    def __post_init__(self):
        # convert all strings are lowercase
        self.hypervisors = (
            [x.lower() for x in self.hypervisors] if self.hypervisors else []
        )
        self.tokens = [x.lower() for x in self.tokens] if self.tokens else []


@dataclass
class config_convert:
    tokens: dict[str, str]  # <token_address_from>: <token_address_to>

    def __post_init__(self):
        # convert all strings are lowercase
        self.tokens = (
            {k.lower(): v.lower() for k, v in self.tokens.items()}
            if self.tokens
            else {}
        )


@dataclass
class config_timeframe:
    start_time: str  # "2022-11-01T00:00:00"
    end_time: str  # 'now' can be used


@dataclass
class config_filters:
    exclude: config_exclude
    convert: config_convert
    force_timeframe: config_timeframe  # used for scraping decisions

    def __post_init__(self):
        if isinstance(self.exclude, dict):
            self.exclude = config_exclude(**self.exclude)
        if isinstance(self.convert, dict):
            self.convert = config_convert(**self.convert)
        if isinstance(self.force_timeframe, dict):
            self.force_timeframe = config_timeframe(**self.force_timeframe)
