from .output_handler import OutputHandler
from .delta_table_output_handler import DeltaTableOutputHandler


class OutputHandlerFactory:
    @staticmethod
    def create_handler(type: str, *args, **kwargs) -> OutputHandler:
        handlers = {"delta_table": DeltaTableOutputHandler}
        return handlers[type](*args, **kwargs)