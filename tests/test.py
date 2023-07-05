from enum import Enum
from tests.protocols import test_protocols


class test_type(str, Enum):
    Protocols = "protocols"


def main(option):
    if option == test_type.Protocols:
        # test protocols
        test_protocols()
