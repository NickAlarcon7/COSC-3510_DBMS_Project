import csv
from bintrees import BinaryTree

{
    "create table": {
        "name": "sushi",
        "columns": [
            {"name": "id", "type": {"int": {}}, "nullable": False},
            {"name": "price", "type": {"decimal": [5, 2]}, "nullable": False},
        ],
        "constraint": {"primary_key": {"columns": "id"}},
    }
}

{
    "sushi": [
        {"id": {"type": "INT", "nullable": False, "primary_key": True}},
        {"price": {"type": "INT", "nullable": False}},
    ],
}

{
    "create table": {
        "name": "order_items",
        "columns": [
            {"name": "sushi_id", "type": {"int": {}}, "nullable": False},
            {"name": "order_id", "type": {"int": {}}, "nullable": False},
        ],
        "constraint": [
            {"primary_key": {"columns": ["sushi_id", "order_id"]}},
            {
                "foreign_key": {
                    "columns": "sushi_id",
                    "references": {"table": "sushi", "columns": "id"},
                }
            },
            {
                "foreign_key": {
                    "columns": "order_id",
                    "references": {"table": "orders", "columns": "id"},
                }
            },
        ],
    }
}

{
    "select": [
        {"value": "o.user_id"},
        {"value": "i.order_id"},
        {"value": "i.sushi_id"},
    ],
    "from": [{"value": "orders", "name": "o"}, {"value": "order_items", "name": "i"}],
    "where": {"eq": ["o.id", "i.order_id"]},
    "orderby": {"value": "i.sushi_id"},
}

{
    "create table": {
        "name": "orders",
        "columns": [
            {"name": "id", "type": {"int": {}}, "nullable": False},
            {"name": "user_id", "type": {"int": {}}, "nullable": False},
        ],
        "constraint": {"primary_key": {"columns": "id"}},
    }
}


class CreateTable:
    def __init__(self):
        self.tables = {}
        self.bplus_trees = {}
        self.table_schemas = {}

    def parse_create_command(self, table_definition):
        table_name = table_definition["name"]
        schema = []

        # TODO: handle when there is only one column
        for column in table_definition["columns"]:
            # create a temp entry to be added to the schema list
            # only add "nullable" property if it is in column
            temp = {column["name"]: {"type": column["type"]}}
            if "nullable" in column:
                temp[column["name"]]["nullable"] = column["nullable"]
            schema.append(temp)

        # Next, add primary key and foreign key constraints
        constraints = table_definition["constraint"]
        # First, check if "constraint"'s value is a list or a dictionary
        # if it is a list, then foreign key and primary key constraints are in the list
        if type(constraints) is list:
            for constraint in constraints:
                # traverse through the schema list to find the column that matches the primary key
                if "primary_key" in constraint:
                    self.parse_key(schema, constraint, "primary_key")
                # traverse through the schema list to find the column that matches the foreign key
                if "foreign_key" in constraint:
                    self.parse_key(schema, constraint, "foreign_key")
        # if it is a dictionary, then there is only primary key constraint
        elif type(constraints) is dict:
            # traverse through the schema list to find the column that matches the primary key
            self.parse_key(schema, constraints, "primary_key")

        print(f"Schema for {table_name}: {schema}")

        return table_name, schema

    # add primary key and foreign key constraints to the schema
    def parse_key(self, schema, constraint, key_type):
        key_columns = constraint[key_type]["columns"]

        # TODO: handle when foreign key is multiple attributes
        # traverse through the schema list to find the column that matches the primary key
        for column in schema:
            # check if there are multiple keys by checking the type
            if type(key_columns) is list:
                for key in key_columns:
                    if key in column:
                        column[key][key_type] = True
            else:
                # if there is only one key, then the type is a string
                if key_columns in column:
                    column[key_columns][key_type] = True
                    # if the key is a foreign key, then add the reference table and column
                    if key_type == "foreign_key":
                        column[key_columns]["foreign_references"] = constraint[
                            key_type
                        ]["references"]

    def initialize_table_structure(self, table_name, schema):
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists!")
        self.tables[table_name] = []
        self.table_schemas[table_name] = schema

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

    def create_btree_index(self, table_name):
        primary_key = self._find_primary_key(table_name)
        if primary_key is None:
            raise ValueError(f"No primary key found for table {table_name}")

        index = BinaryTree()
        for row in self.tables[table_name]:
            index.insert(row[primary_key], row)
        self.bplus_trees[table_name] = index

    def _find_primary_key(self, table_name):
        for column, column_def in self.table_schemas[table_name].items():
            if "PRIMARY KEY" in column_def["constraints"]:
                return column
        return None
