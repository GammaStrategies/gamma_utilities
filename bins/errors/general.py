#
from bins.general.enums import Chain, error_identity


class ProcessingError(Exception):
    """Exception raised for errors during processing.

    Attributes:
        chain -- chain where the error occurred
        item -- error item
        identity -- error identification
        action -- conclusion action to be taken a posteriori
        message -- explanation of the error
    """

    def __init__(
        self,
        chain: Chain,
        item: dict,
        identity: error_identity = None,
        action: str = "none",
        message="Error during rewards processing",
    ):
        self.chain = chain
        self.item = item
        self.identity = identity
        self.action = action
        self.message = message
        super().__init__(self.message)
