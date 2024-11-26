from .registry import CheckLogEntryBuilderRegistry


class Check:
    """
    Main class that processes expectation results using appropriate calculators and loggers
    """

    def __init__(self, name: str):
        self.name = name
        # Initialise registries
        self.log_entry_builder_registry = CheckLogEntryBuilderRegistry()

        # Register handlers for different result types
        self.register_handlers()

    def register_handlers(self) -> None:
        # Register stats calculators
        self.stats_registry.register("dataframe", DataframeStatsCalculator())
        self.stats_registry.register("schema", SchemaStatsCalculator())

    def process_expectation_result(self, result: "ExpectationResult") -> None:
        # Get appropriate calculator and logger based on result type
        calculator = self.stats_registry.get_calculator(result.get_result_type())
        logger = self.logger_registry.get_logger(result.get_result_type())

        # Calculate stats and log results
        stats = calculator.calculate_stats(result)
        logger.log_validation_result(result, stats)
