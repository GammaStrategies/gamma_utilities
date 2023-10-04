from bins.checkers.general import check_dict_for_value
from bins.errors.general import CheckingError, ProcessingError
from bins.general.enums import error_identity


def check_hypervisor_is_valid(hypervisor: dict, quiet: bool = False):
    """Check if a dictionary hypervisor is well formed

    Args:
        hypervisor (dict):
        quiet (bool, optional): When set, will not raise error but return true/false Defaults to False.

    Raises:
        ValueError: with the description of the error
    """
    # search for none strings in hypervisor fields
    if result_none := check_dict_for_value(
        item=hypervisor, values=[None, "None", "none"]
    ):
        # result_none = list[ list[ str ] ]  -->  ".".joint(x) for x in i for i in result_none

        if quiet:
            return False
        # else
        raise CheckingError(
            item=hypervisor,
            identity=error_identity.RETURN_NONE,
            action="rescrape",
            message=f" Invalid hypervisor: none value found in hypervisor {result_none}",
        )

    return True
