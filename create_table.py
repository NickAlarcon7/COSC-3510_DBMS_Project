import csv
from bintrees import BinaryTree

{
    "create table": {
        "name": "MyTable",
        "columns": {"name": "MyColumn", "type": {"int": {}}, "primary_key": True},
    }
}
{
    "create table": {
        "name": "Orders",
        "columns": [
            {"name": "OrderID", "type": {"int": {}}, "primary_key": True},
            {"name": "OrderDate", "type": {"date": {}}},
            {"name": "NewCustomerID", "type": {"int": {}}},
            {"name": "NewCustomerName", "type": {"varchar": 255}},
        ],
        "constraint": {
            "foreign_key": {
                "columns": ["NewCustomerID", "NewCustomerName"],
                "references": {
                    "table": "Customers",
                    "columns": ["CustomerID", "CustomerName"],
                },
            }
        },
    }
}
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
    "sushi": {
        "id": {"type": "INT", "nullable": False, "primary_key": True},
        "price": {"type": "INT", "nullable": False},
    },
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
            print(f"Schema for {table_name}: {schema}")
            return table_name, schema

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

        print(f"Schema for {table_name}: {schema}")
        return table_name, schema

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
