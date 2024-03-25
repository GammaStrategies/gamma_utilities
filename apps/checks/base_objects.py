from dataclasses import dataclass


@dataclass
class analysis_item:
    # data
    name: str
    data: dict
    # logs
    log_message: str
    telegram_message: str


class base_analyzer_object:
    def __init__(self, items: list[analysis_item] = []):
        self.items = items
