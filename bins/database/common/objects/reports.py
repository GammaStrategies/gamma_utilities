from dataclasses import dataclass
from bins.general.enums import Protocol, reportType
from .hypervisor import timeframe_object


@dataclass
class report_object:
    type: reportType
    protocol: Protocol | None = None
    timeframe: timeframe_object | None = None
    data: dict | None = None

    @property
    def id(self):
        return f"{self.type}_{self.protocol}_{self.timeframe.ini.timestamp}_{self.timeframe.end.timestamp}"

    def to_dict(self) -> dict:
        # create dict
        result = {
            "id": self.id,
            "type": self.type.value,
        }
        # add protocol
        result["protocol"] = self.protocol
        # add timeframe
        result["timeframe"] = self.timeframe.to_dict()
        # add data
        result["data"] = self.data

        # return dict
        return result
