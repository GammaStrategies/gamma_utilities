from multiprocessing import Process
from multiprocessing import Value


# custom process class
class CustomProcess(Process):
    # override the constructor
    def __init__(self, fn: callable, **kwargs):
        # execute the base constructor
        Process.__init__(self)
        # initialize
        self.processing_fn = fn
        self.kwargs = kwargs
        self.result_data = None

        self._processing_status = "stopped"

    # override the run function
    def run(self):
        # set processing status to running
        self._processing_status = "running"
        # execute the processing function, saving its result
        self.result_data = self.processing_fn(**self.kwargs)
        # set processing status
        self._processing_status = "ended"
