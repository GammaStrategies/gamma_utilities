from dataclasses import dataclass
from decimal import Decimal

from bson import ObjectId


@dataclass
class token_group:
    token0: int | Decimal = None
    token1: int | Decimal = None

    def to_dict(self) -> dict:
        return {
            "token0": self.token0,
            "token1": self.token1,
        }


class dict_to_object_old(object):
    """Turns a dictionary into an object"""

    def __init__(self, dictionary: dict):
        # convert dictionary to object
        self._convert_to_object(obj=dictionary)
        # execute post init
        self.post_init()

    def post_init(self):
        pass

    # def __repr__(self):
    #     """"""
    #     attrs = str([x for x in self.__dict__])
    #     return "<dict_to_object: %s=" ">" % attrs

    def _convert_to_object(self, obj, key: str | None = None, filter: callable = None):
        """Converts a dictionary to an object if it is a dictionary"""

        if isinstance(obj, dict):
            data = {}
            for k, v in obj.items():
                data[k] = dict_to_object(dictionary=v)
            return data
        elif hasattr(obj, "__iter__") and not isinstance(obj, str):
            return [self._convert_to_object(obj=v, key=key, filter=filter) for v in obj]
        elif hasattr(obj, "__dict__"):
            data = dict(
                [
                    (key, self._convert_to_object(obj=value, key=key, filter=filter))
                    for key, value in obj.__dict__.items()
                    if not callable(value) and not key.startswith("_")
                ]
            )
            if key is not None and hasattr(obj, "__class__"):
                data[key] = obj.__class__.__name__
            return data
        elif isinstance(obj, ObjectId):
            # set attribute
            return str(obj)
        else:
            if filter:
                # apply filter
                return filter(obj, key)

            if key and obj:
                # set attribute
                setattr(self, key, obj[key])
            else:
                return obj


def filter_mongodb(obj, key: str | None = None):
    """Object to dict filter for mongodb objects"""

    # convert any int [not timestamp nor block] to avoid mongoDB 8bit errors
    if isinstance(obj, int) and key not in ["timestamp", "block"]:
        return str(obj)


def transformer_hypervisor_status(value, key: str):
    # convert bson objectID
    if isinstance(value, str):
        # check if string is float or int
        try:
            return int(value)
        except Exception:
            try:
                return float(value)
            except Exception:
                pass
        # check if string is objectID
        if key in ["qtty_token0", "qtty_token1"]:
            return float(value)
        if key in ["decimals", "block", "timestamp"]:
            return int(value)


class dict_to_object:
    def __init__(self, transformer: callable = None, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = dict_to_object(**value)
            elif hasattr(value, "__iter__") and not isinstance(value, str):
                self.__dict__[key] = []
                for v in value:
                    if isinstance(v, dict):
                        self.__dict__[key].append(dict_to_object(**v))
                    elif isinstance(v, list):
                        self.__dict__[key].append(dict_to_object(*v))
                    else:
                        self.__dict__[key].append(v)

            else:
                # check if value shall be converted
                if transformer:
                    value = transformer(value=value, key=key)
                self.__dict__[key] = value
