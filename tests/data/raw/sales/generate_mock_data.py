import csv
from datetime import datetime, timedelta
import os
import random

# Schema versions with their date ranges and column data types
schema_versions = {
    'v1': {
        'date_range': ('2000-01-01', '2023-07-31'),
        'columns': {
            'timestamp': {'type': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'user_id': {'type': 'string', 'pattern': 'USER_\\d{4}'},
            'product_id': {'type': 'string', 'pattern': 'PROD_\\d{3}'},
            'quantity': {'type': 'integer', 'range': (1, 100)},
            'price': {'type': 'decimal', 'precision': 2, 'range': (10.0, 1000.0)}
        }
    },
    'v2': {
        'date_range': ('2023-08-01', '2023-12-31'),
        'columns': {
            'timestamp': {'type': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'user_id': {'type': 'string', 'pattern': 'USER_\\d{4}'},
            'quantity': {'type': 'integer', 'range': (1, 100)},
            'price': {'type': 'decimal', 'precision': 2, 'range': (10.0, 1000.0)}
        }
    },
    'v3': {
        'date_range': ('2024-01-01', '2024-06-30'),
        'columns': {
            'timestamp': {'type': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'user_id': {'type': 'string', 'pattern': 'USER_\\d{4}'},
            'product_id': {'type': 'string', 'pattern': 'PROD_\\d{3}'},
            'quantity': {'type': 'integer', 'range': (1, 100)},
            'price': {'type': 'decimal', 'precision': 2, 'range': (10.0, 1000.0)}
        }
    },
    'v4': {
        'date_range': ('2024-07-01', '2024-12-31'),
        'columns': {
            'timestamp': {'type': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'user_id': {'type': 'string', 'pattern': 'USER_\\d{4}'},
            'quantity': {'type': 'integer', 'range': (1, 100)},
            'price': {'type': 'decimal', 'precision': 2, 'range': (10.0, 1000.0)}
        }
    },
    'v5': {
        'date_range': ('2025-01-01', '9999-12-31'),
        'columns': {
            'timestamp': {'type': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'user_id': {'type': 'string', 'pattern': 'USER_\\d{4}'},
            'product_id': {'type': 'string', 'pattern': 'PROD_\\d{3}'},
            'quantity': {'type': 'integer', 'range': (1, 100)},
            'price': {'type': 'decimal', 'precision': 2, 'range': (10.0, 1000.0)},
            'category': {'type': 'string', 'enum': ['Electronics', 'Clothing', 'Food', 'Books', 'Home']}
        }
    }
}

def generate_mock_data(schema_version, num_rows=100):
    """Generate mock data based on schema version and data types"""
    data = []
    schema = schema_versions[schema_version]['columns']
    
    for _ in range(num_rows):
        row = {}
        for col_name, col_spec in schema.items():
            if col_spec['type'] == 'datetime':
                row[col_name] = datetime.now().strftime(col_spec['format'])
            elif col_spec['type'] == 'string':
                if 'enum' in col_spec:
                    row[col_name] = random.choice(col_spec['enum'])
                else:
                    # Generate string based on pattern (USER_#### or PROD_###)
                    if 'USER' in col_spec['pattern']:
                        row[col_name] = f'USER_{random.randint(1000, 9999)}'
                    else:
                        row[col_name] = f'PROD_{random.randint(100, 999)}'
            elif col_spec['type'] == 'integer':
                row[col_name] = random.randint(*col_spec['range'])
            elif col_spec['type'] == 'decimal':
                row[col_name] = round(random.uniform(*col_spec['range']), col_spec['precision'])
        
        data.append(row)
    
    return data

# Generate dates that will cover all schema versions
test_dates = [
    # v1
    datetime(2023, 7, 16),  # Sunday
    # v2
    datetime(2023, 8, 13),
    datetime(2023, 12, 17),
    # v3
    datetime(2024, 1, 14),
    datetime(2024, 6, 16),
    # v4
    datetime(2024, 7, 14),
    datetime(2024, 12, 15),
    # v5
    datetime(2025, 1, 12),
    datetime(2025, 2, 9),
    datetime(2025, 3, 9),
]

# Create directory if it doesn't exist
os.makedirs('mock_data', exist_ok=True)

# Generate files
for date in test_dates:
    date_str = date.strftime('%Y%m%d')
    # Find applicable schema version
    schema_version = None
    for version, info in schema_versions.items():
        start_date = datetime.strptime(info['date_range'][0], '%Y-%m-%d')
        end_date = datetime.strptime(info['date_range'][1], '%Y-%m-%d')
        if start_date <= date <= end_date:
            schema_version = version
            break
    
    data = generate_mock_data(schema_version)
    filename = f'mock_data/sales_data_{date_str}_schema_{schema_version}.csv'
    
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=schema_versions[schema_version]['columns'].keys())
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Generated {filename}")

print("\nSchema Version Mapping:")
for version, info in schema_versions.items():
    print(f"\n{version}: {info['date_range']}")
    print("Columns:")
    for col_name, col_spec in info['columns'].items():
        type_desc = f"{col_spec['type']}"
        if 'format' in col_spec:
            type_desc += f" (format: {col_spec['format']})"
        elif 'enum' in col_spec:
            type_desc += f" (values: {', '.join(col_spec['enum'])})"
        elif 'range' in col_spec:
            type_desc += f" (range: {col_spec['range']})"
        if 'precision' in col_spec:
            type_desc += f" (precision: {col_spec['precision']})"
        print(f"  - {col_name}: {type_desc}")
        