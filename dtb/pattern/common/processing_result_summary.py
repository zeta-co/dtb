from typing import Any, Dict, List
from .processing_result import ProcessingResult


class ProcessingResultSummary:
    """Responsible for generating processing summaries in both dictionary and string formats."""

    def to_dict(self, results: List[ProcessingResult]) -> Dict[str, Any]:
        """Convert processing results to a dictionary format.

        Args:
            results: List of ProcessingResult objects to summarize

        Returns:
            Dictionary containing summary statistics and failure details
        """
        if not results:
            return {
                "total_batches": 0,
                "successful_batches": 0,
                "failed_batches": 0,
                "total_records_processed": 0,
                "success_rate": 0,
                "failures": [],
            }

        successful_batches = [r for r in results if r.success]
        failed_batches = [r for r in results if not r.success]

        return {
            "total_batches": len(results),
            "successful_batches": len(successful_batches),
            "failed_batches": len(failed_batches),
            "total_records_processed": sum(r.total_count for r in successful_batches),
            "success_rate": len(successful_batches) / len(results) if results else 0,
            "failures": [
                {
                    "files": list(
                        set(
                            f
                            for e in r.check_log_entries
                            for f in e._log_entry_dict.get("files", [])
                        )
                    ),
                    "checks": [
                        {
                            "description": e._log_entry_dict["check_description"],
                            "invalid_rows": e._log_entry_dict["invalid_row_count"],
                        }
                        for e in r.check_log_entries
                    ],
                    "error_message": r.error_message,
                    "error_traceback": r.error_traceback,
                }
                for r in failed_batches
            ],
        }

    def convert_to_str(self, results: List[ProcessingResult]) -> str:
        """Convert processing results to a formatted string representation.
        
        Args:
            results: List of ProcessingResult objects to summarize
            
        Returns:
            Formatted string containing the summary
        """
        summary_dict = self.to_dict(results)
        
        # Build the basic summary
        summary_lines = [
            f"Total batches: {summary_dict['total_batches']}",
            f"Successful batches: {summary_dict['successful_batches']}",
            f"Failed batches: {summary_dict['failed_batches']}",
            f"Total records processed: {summary_dict['total_records_processed']}",
            f"Success rate: {summary_dict['success_rate']:.1%}",
        ]

        # Add failure details if there are any
        if summary_dict["failures"]:
            summary_lines.append("\nThe following batches have failed:")
            
            for failure in summary_dict["failures"]:
                summary_lines.append("    Files:")
                for file in failure["files"]:
                    summary_lines.append(f"        {file}")
                
                if failure["error_message"]:
                    summary_lines.append(f"    Error message: {failure['error_message']}")
                
                if failure["checks"]:
                    summary_lines.append("    Checks:")
                    for check in failure["checks"]:
                        summary_lines.append(
                            f"        {check['invalid_rows']} records failed "
                            f"the check \"{check['description']}\""
                        )
                summary_lines.append("")  # Add blank line between failures

        return "\n".join(summary_lines).rstrip()
