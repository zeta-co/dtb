# Example of how the metadata dictionary should look
METADATA = {
    # Entity-level metadata
    "entity_id": "sales_transactions",
    "description": "Daily sales transactions data",
    "owner": "data_team",
    "domain": "sales",
    "tags": {"criticality": "high", "update_frequency": "daily"},
    "type": "delta",  # or "csv", "parquet", etc.
    "path": "catalog.sales.transactions",  # or file path pattern for raw files
    "format_options": {
        "header": "true",
        "inferSchema": "true",
        "mergeSchema": "true"
    },
    
    # Schema information
    "schema": {
        "columns": [
            {
                "name": "sale_id",
                "type": "string",
                "nullable": False,
                "description": "Unique identifier for each sale",
                "constraints": [
                    {"type": "primary_key"},
                    {"type": "pattern", "value": "^S[0-9]{10}$"}
                ]
            },
            {
                "name": "sale_date",
                "type": "date",
                "nullable": False,
                "partition_key": True,
                "constraints": [
                    {"type": "range", "min": "2020-01-01", "max": "2025-12-31"}
                ]
            }
        ],
        "constraints": [
            {"type": "unique", "columns": ["sale_id"]},
            {"type": "not_null", "columns": ["sale_id", "sale_date"]}
        ]
    },
    
    # Runtime behavior
    "runtime": {
        "filter_columns": ["sale_date"],
        "required_filters": ["sale_date"],
        "watermark_column": "sale_date",
        "batch_size": 10000,
        "timeout_seconds": 3600
    },
    
    # Data quality rules
    "quality": {
        "freshness_sla_minutes": 60,
        "completeness_threshold": 0.99,
        "custom_checks": [
            {
                "name": "daily_sales_volume",
                "type": "row_count",
                "min_value": 1000,
                "severity": "error"
            }
        ]
    },
    
    # Lineage information (useful for Purview)
    "lineage": {
        "upstream_entities": ["raw_sales.transactions"],
        "downstream_entities": ["sales.daily_summary"],
        "transformation_type": "clean_and_conform"
    },
    
    # Audit information
    "audit": {
        "created_by": "john.doe@company.com",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_by": "jane.smith@company.com",
        "updated_at": "2024-01-10T00:00:00Z",
        "version": "1.0"
    }
}
