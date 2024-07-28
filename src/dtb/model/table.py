class Table:
    def __init__(self, *args, catalog="hive_metastore"):
        self.catalog = catalog
        if len(args) == 1:
            if '.' in args[0]:
                self.schema, self.name = args[0].split('.')
            else:
                raise ValueError('Invalid name pattern. Expected ("schema.table"), or ("schema", "table")!')
        elif len(args) == 2:
            self.schema, self.name = args
        else:
            raise ValueError("Invalid number of arguments. Expected 1 or 2!")
    
    @property
    def full_name(self):
        return f"{self.catalog}.{self.schema}.{self.name}"
    
    @property
    def schema_table(self):
        return f"{self.schema}.{self.name}"
