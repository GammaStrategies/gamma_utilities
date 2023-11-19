from enum import Enum
from tests.hypervisors import test_hypervisors
from tests.protocols import test_protocols


class test_type(str, Enum):
    Protocols = "protocols"
    Hypervisors = "hypervisors"


def main(option):
    if option == test_type.Protocols:
        # test protocols
        test_protocols()
    elif option == test_type.Hypervisors:
        # test hypervisors
        test_hypervisors()
