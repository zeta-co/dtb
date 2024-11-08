from .output_handler import OutputHandler
from .output_handler_delta_table import DeltaTableOutputHandler


class OutputHandlerFactory:
    @staticmethod
    def create_handler(type: str, *args, **kwargs) -> OutputHandler:
        handlers = {"delta_table": DeltaTableOutputHandler}
        return handlers[type](*args, **kwargs)
