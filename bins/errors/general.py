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


class CheckingError(Exception):
    """Exception raised for errors during checking.

    Attributes:
        item -- error item
        identity -- error identification
        action -- conclusion action to be taken a posteriori
        message -- explanation of the error
    """

    def __init__(
        self,
        item: dict,
        identity: error_identity = None,
        action: str = "none",
        message="Error during checking",
    ):
        self.item = item
        self.identity = identity
        self.action = action
        self.message = message
        super().__init__(self.message)


class ConfigurationError(Exception):
    def __init__(
        self,
        item: dict,
        action: str = "none",
        cascade: list[str] = None,
        message="Error loading configuration",
    ):
        self.item = item
        self.action = action
        self.cascade = cascade
        self.message = message
        super().__init__(self.message)
