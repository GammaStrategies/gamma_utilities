#
class ProcessingError(Exception):
    """Exception raised for errors during processing.

    Attributes:
        item -- error item
        action -- conclusion action to be taken a posteriori
        message -- explanation of the error
    """

    def __init__(
        self,
        item: dict,
        action: str = "none",
        message="Error during rewards processing",
    ):
        self.item = item
        self.action = action
        self.message = message
        super().__init__(self.message)
