import csv
from bintrees import BinaryTree


class Database:
    def __init__(self):
        self.tables = {}
        self.indexing_structures = {}
        self.table_schemas = {}

    def create_table(self, table_definition):
        table_name = table_definition["name"]
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists!")

        # Create an empty table that will be populated when LOAD DATA is read
        self.tables[table_name] = []

        schema = self._create_schema(table_definition)
        print(f"Schema for {table_name}: {schema}")
        self.table_schemas[table_name] = schema
        print(f"Indexing structure: {self.indexing_structures}")

    def _create_schema(self, table_definition):
        schema = {}
        # First check if table has only one column
        if type(table_definition["columns"]) is dict:
            columns = [table_definition["columns"]]
        else:
            columns = table_definition["columns"]

        # Next, add columns to the schema
        for column in columns:
            schema[column["name"]] = {
                key: value for key, value in column.items() if key != "name"
            }

        # if no constraints are specified, return early
        if "constraint" not in table_definition:
            return schema

        # Next, add primary key and foreign key constraints
        constraints = table_definition["constraint"]
        # First, convert the constraints to a list if it is a dictionary
        if type(constraints) is dict:
            constraints = [constraints]

        for constraint in constraints:
            # traverse through the schema list to find the column that matches the primary key
            if "primary_key" in constraint:
                self._parse_key(schema, constraint, "primary_key")
            # traverse through the schema list to find the column that matches the foreign key
            if "foreign_key" in constraint:
                self._parse_key(schema, constraint, "foreign_key")

        # since inline primary key is not included in constraint, we need to check to ensure primary key exists
        primary_key_count = self._count_primary_key_(schema)
        if primary_key_count == 0:
            raise ValueError("No primary key specified!")
        # if key is a single attribute primary key, then add an entry to the indexing structure
        elif primary_key_count == 1:
            self.indexing_structures[table_definition["name"]] = BinaryTree()

        return schema

    # add primary key and foreign key constraints to the schema
    def _parse_key(self, schema, constraint, key_type):
        key_columns = constraint[key_type]["columns"]

        # if key_columns is a list, then there are multiple keys
        if type(key_columns) is list:
            for index, key in enumerate(key_columns):
                schema[key][key_type] = True
                # if key is a foreign key, then add the foreign table and column to the column
                if key_type == "foreign_key":
                    key_references = constraint[key_type]["references"]
                    schema[key]["foreign_references"] = {
                        "table": key_references["table"],
                        "column": key_references["columns"][index],
                    }

        # if key_columns is a string, then there is only one key
        else:
            schema[key_columns][key_type] = True
            # if key is a foreign key, then add the foreign table and column to the column
            if key_type == "foreign_key":
                key_references = constraint[key_type]["references"]
                schema[key_columns]["foreign_references"] = {
                    "table": key_references["table"],
                    "column": key_references["columns"],
                }

    def _count_primary_key_(self, schema):
        count = 0
        for column in schema:
            if "primary_key" in schema[column]:
                count += 1
        return count

    def populate_table_from_csv(self, table_name, csv_filename):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist!")
        if table_name not in self.table_schemas:
            raise ValueError(f"Schema for {table_name} does not exist!")

        with open(
            csv_filename, "r", encoding="utf-8"
        ) as file:  # Added encoding specification
            reader = csv.DictReader(file)
            expected_columns = set(self.table_schemas[table_name].keys())
            csv_columns = set(reader.fieldnames)

            if not csv_columns:
                print(f"CSV file seems to be empty or without headers: {csv_filename}")
                return

            if csv_columns != expected_columns:
                print(f"Expected columns: {expected_columns}")
                print(f"CSV columns: {csv_columns}")
                raise ValueError(
                    f"CSV columns do not match table columns for {table_name}!"
                )

            for row in reader:
                # Convert types as per the schema before appending
                converted_row = {
                    column: self._convert_type(
                        row[column], self.table_schemas[table_name][column]["data_type"]
                    )
                    for column in row
                }
                self.tables[table_name].append(converted_row)

    def _convert_type(self, value, data_type):
        if data_type == "INT":
            return int(value)
        elif data_type.startswith("FLOAT") or data_type.startswith("DECIMAL"):
            return float(value)
        elif data_type == "DATE":
            # Assuming you want to keep date as a string for now,
            # but you might want to convert it to a datetime object.
            return value
        else:
            return value  # For other data types that are strings
