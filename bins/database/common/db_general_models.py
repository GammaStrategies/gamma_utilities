from dataclasses import dataclass, field, asdict, InitVar


@dataclass
class tool_mongodb_general:
    def create_dbFilter(self) -> dict:
        return {}

    def asdict(self) -> dict:
        return asdict(self)


class tool_database_id:
    id: str = ""

    def __post_init__(self):
        if self.id == "":
            self.id = self.create_id()

    def create_id(self) -> str:
        return ""
