from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
import logging

from bson import ObjectId


@dataclass
class token_group_object:
    token0: int | Decimal = None
    token1: int | Decimal = None

    def to_dict(self) -> dict:
        return {
            "token0": self.token0,
            "token1": self.token1,
        }


def filter_mongodb(obj, key: str | None = None):
    """Object to dict filter for mongodb objects"""

    # convert any int [not timestamp nor block] to avoid mongoDB 8bit errors
    if isinstance(obj, int) and key not in ["timestamp", "block"]:
        return str(obj)


class dict_to_object:
    def __init__(self, transformer: callable = None, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = dict_to_object(transformer, **value)
            elif hasattr(value, "__iter__") and not isinstance(value, str):
                self.__dict__[key] = []
                for v in value:
                    if isinstance(v, dict):
                        self.__dict__[key].append(dict_to_object(transformer, **v))
                    elif isinstance(v, list):
                        self.__dict__[key].append(dict_to_object(transformer, *v))
                    else:
                        if transformer:
                            v = transformer(value=v, key=key)
                        self.__dict__[key].append(v)

            else:
                # check if value shall be converted
                if transformer:
                    value = transformer(value=value, key=key)
                self.__dict__[key] = value

    def pre_subtraction(self, key: str, value: any):
        """Called before the subtraction of two objects properties

        Args:
            key (str):
            value (any):

        Returns:
            any: a processed value or None
        """

        if key == "_id":
            return value
        elif key in ["fee", "decimals"]:
            return value

        if isinstance(value, str):
            # keep the value
            return value
        if isinstance(value, list):
            # keep the value
            return value

        return None

    def __sub__(self, other):
        # create new object
        result = deepcopy(self)
        # loop thru all properties and substract from other object properties
        for key, value in result.__dict__.items():
            # check if property exists in other object
            if key in other.__dict__:
                _value_processed = self.pre_subtraction(key=key, value=value)
                if _value_processed != None:
                    result.__dict__[key] = _value_processed
                    # if processed, continue
                    continue

                # substract properties and set result
                try:
                    result.__dict__[key] = value - other.__dict__[key]
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Error substracting {key}: {e}"
                    )
                    # this is a non numeric property, keep the value
                    result.__dict__[key] = value

            else:
                raise Exception(f"Property {key} not found in object to substract from")

        return result
