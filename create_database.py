import csv
from BTrees.OOBTree import OOBTree
from decimal import Decimal, getcontext
from prettytable import PrettyTable


class Database:
    def __init__(self):
        self.tables = {}
        self.indexing_structures = {}
        self.table_schemas = {}

    def create_table(self, table_definition) -> str:
        table_name = table_definition["name"]
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists!")

        # Create an empty table that will be populated when LOAD DATA is read
        self.tables[table_name] = []

        schema = self._create_schema(table_definition)
        print(f"Schema for {table_name}: {schema}")
        self.table_schemas[table_name] = schema

        return table_name

    def _create_schema(self, table_definition):
        schema = {}

        # First, add columns to the schema
        self._parse_columns(table_definition, schema)

        # if no constraints are specified, return early
        if "constraint" not in table_definition:
            return schema

        # Next, add primary key and foreign key constraints
        constraints = table_definition["constraint"]
        # First, convert the constraints to a list if it is a dictionary
        if isinstance(constraints, dict):
            constraints = [constraints]

        for constraint in constraints:
            # traverse through the schema list to find the column that matches the primary key
            if "primary_key" in constraint:
                self._parse_key(schema, constraint, "primary_key")
            # traverse through the schema list to find the column that matches the foreign key
            elif "foreign_key" in constraint:
                self._parse_key(schema, constraint, "foreign_key")
            else:
                raise ValueError(f"Invalid constraint: {constraint}")

        # since inline primary key is not included in constraint, we need to check to ensure primary key exists
        primary_key_count = self._count_primary_key(schema)
        if primary_key_count == 0:
            raise ValueError("No primary key specified!")
        # if key is a single attribute primary key, then add an entry to the indexing structure
        elif primary_key_count == 1:
            self.indexing_structures[table_definition["name"]] = OOBTree()

        return schema

    def _parse_columns(self, table_definition, schema):
        # First check if table has only one column
        if isinstance(table_definition["columns"], dict):
            columns = [table_definition["columns"]]
        else:
            columns = table_definition["columns"]

        # Next, add columns to the schema
        for column in columns:
            # throw error if no type is specified
            if "type" not in column:
                raise ValueError(f"Invalid column - no type is specified: {column}")

            schema[column["name"]] = {
                key: value for key, value in column.items() if key != "name"
            }

            # clean up data types - convert "type": {"int": {}} to "type": "int"
            data_type = schema[column["name"]]["type"]
            if "int" in data_type or "integer" in data_type:
                schema[column["name"]]["type"] = "int"
            elif "float" in data_type:
                schema[column["name"]]["type"] = "float"
            elif "boolean" in data_type:
                schema[column["name"]]["type"] = "boolean"
            elif "varchar" in data_type:
                schema[column["name"]]["type"] = "varchar"

    # add primary key and foreign key constraints to the schema
    def _parse_key(self, schema, constraint, key_type):
        key_columns = constraint[key_type]["columns"]

        # if key_columns is a list, then there are multiple keys
        if isinstance(key_columns, list):
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

    def _count_primary_key(self, schema):
        count = 0
        for column in schema:
            if "primary_key" in schema[column]:
                count += 1
        return count

    def load_from_csv(self, table_name, csv_filename):
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

            self._populate_table_from_csv(reader, table_name)

    def _populate_table_from_csv(self, reader, table_name):
        table = self.tables[table_name]
        schema = self.table_schemas[table_name]

        for row in reader:
            # Convert types as per the schema before appending
            try:
                converted_row = {
                    column: self._convert_type(
                        row[column],
                        schema[column]["type"],
                        schema[column].get("nullable", True),
                        schema[column].get("primary_key", False),
                    )
                    for column in row
                }
            # ignore the new row if it cannot be converted to the correct type
            except ValueError as e:
                print(f"Error converting row {row}. Skipping...: {e}")
                continue

            # ignore the new row if it already exists in the table
            if converted_row in table:
                print(f"Duplicate row: {converted_row}. Skipping...")
                continue
            # if indexing structure exists, then add the row to the indexing structure
            if table_name in self.indexing_structures:
                indexing_structure = self.indexing_structures[table_name]
                # Extract the primary key column and its value
                primary_key_column = next(
                    column
                    for column in converted_row
                    if "primary_key" in schema[column]
                )
                primary_key_value = converted_row[primary_key_column]
                # ignore the new row with a duplicate primary key
                if primary_key_value in indexing_structure:
                    print(f"Duplicate primary key: {primary_key_value}. Skipping...")
                    continue
                # Add converted_row to the indexing structure
                indexing_structure.insert(primary_key_value, converted_row)

            # Add converted_row to the table
            table.append(converted_row)

    def _convert_type(self, value, data_type, nullable, primary_key):
        # Handle null values
        if value == "" or (isinstance(value, str) and value.isspace()):
            # if NOT NULL is not set and column is not a primary key, then return None
            if nullable and not primary_key:
                return None
            else:
                raise ValueError(
                    f"Column cannot be null. Failed to convert to {data_type}"
                )
        # Handle integer type
        if data_type == "int":
            return int(value)
        # Handle floating point type
        elif data_type == "float":
            return float(value)
        # Handle boolean type
        elif data_type == "varchar":
            return str(value)
        elif data_type == "boolean":
            if value in ("true", "1", "t", "y", "yes"):
                return True
            elif value in ("false", "0", "f", "n", "no"):
                return False
            else:
                raise ValueError(f"Invalid boolean value: {value}")
        # Handle decimal type with precision and scale
        elif isinstance(data_type, dict) and "decimal" in data_type:
            precision, scale = data_type["decimal"]
            getcontext().prec = precision
            # Handle decimal type with precision and scale
            results = Decimal(value).quantize(Decimal("1." + "0" * scale))
            return float(results)
        # Handle other data types that are strings
        else:
            return str(value)

    def print_table(self, table_name):
        if table_name not in self.tables:
            print(f"Table {table_name} does not exist!")
            return

        table = PrettyTable()

        # Set the column names as the fields
        table.field_names = self.table_schemas[table_name].keys()

        # Set horizontal lines to separate rows
        table.hrules = 1

        # Add rows to the table
        for row in self.tables[table_name]:
            table.add_row([row[column] for column in table.field_names])

        # ANSI Blue color start and reset codes
        blue_start = "\033[94m"
        reset = "\033[0m"

        # Print the table in blue color
        print(blue_start + str(table) + reset)

    def insert(self, table_name, values):
        if "select" not in values:
            raise ValueError(
                f"Invalid values: {values}. Currently supported format: INSERT INTO EMPLOYEE VALUES (1, 'John', 150.00, 5)"
            )
        values = values["select"]

        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist!")
        table = self.tables[table_name]
        new_row = {}
        column_names = list(self.table_schemas[table_name].keys())

        # if number of values does not match number of columns, then raise error
        if len(values) != len(column_names):
            raise ValueError(
                f"Number of values does not match number of columns: {values}"
            )

        # iterate through values and match it with the schema to create a new row
        for index, value in enumerate(values):
            value = value["value"]
            # extract column name at index from schema
            column_name = column_names[index]

            column_schema = self.table_schemas[table_name][column_name]
            # before adding value to new_row, call _convert_type to convert value to the correct type
            new_row[column_name] = self._convert_type(
                value,
                column_schema["type"],
                column_schema.get("nullable", True),
                column_schema.get("primary_key", False),
            )

            # find the primary key value
            if "primary_key" in column_schema:
                primary_key_value = new_row[column_name]

        # if new_row already exists in the table, then raise error
        if new_row in table:
            raise ValueError(f"Duplicate row: {new_row}")

        # if indexing structure exists, then add the row to the indexing structure
        if table_name in self.indexing_structures:
            indexing_structure = self.indexing_structures[table_name]
            # ignore the new row with a duplicate primary key
            if primary_key_value in indexing_structure:
                raise ValueError(f"Duplicate primary key: {primary_key_value}")
            # Add new_row to the indexing structure
            indexing_structure.insert(primary_key_value, new_row)

        # add new_row to the table
        table.append(new_row)

    def delete(self, table_name, where_clause):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist!")
        table = self.tables[table_name]

        # if table is empty, then return early
        if not table:
            return "Table is empty!"
        # if where_clause is None, then delete all rows in the table
        if where_clause is None:
            self.tables[table_name] = []
            if table_name in self.indexing_structures:
                self.indexing_structures[table_name].clear()
            return "All rows successfully deleted!"

        if "eq" not in where_clause:
            raise ValueError(
                f"Invalid where clause: {where_clause}. Currently supported format: DELETE FROM EMPLOYEE WHERE dept = 5"
            )

        column_name, matching_value = self.parse_where(where_clause)

        # find row in tables[table_name] that matches the column name and matching value
        for row in table:
            if row[column_name] == matching_value:
                # if indexing structure exists, then remove the row from the indexing structure
                if table_name in self.indexing_structures:
                    # extract primary key column name and value
                    primary_key_column = next(
                        column
                        for column in row
                        if "primary_key" in self.table_schemas[table_name][column]
                    )
                    primary_key_value = row[primary_key_column]

                    self.indexing_structures[table_name].pop(primary_key_value)

                # remove the row from the indexing structure
                table.remove(row)

        return f"Row with {column_name} = {matching_value} successfully deleted!"

    def update(self, table_name, assignments, where_clause):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist!")
        table = self.tables[table_name]

        # if table is empty, then return early
        if not table:
            return "Table is empty!"

        # Extract column and matching value using items()
        for key, value in assignments.items():
            set_column = key
            # flatten and extract matching value
            if isinstance(value, dict):
                # if value does not contain "literal" key, then raise error
                if "literal" not in value:
                    raise ValueError(
                        f"Invalid assignments: {assignments}. Currently supported format: UPDATE EMPLOYEE SET salary = 150.00 WHERE name = 'John'"
                    )
                value = value["literal"]
            set_value = value

        # if where_clause is None, then update all rows in the table
        if where_clause is None:
            for row in table:
                row[set_column] = set_value
            return f"All rows successfully updated!"

        if "eq" not in where_clause:
            raise ValueError(
                f"Invalid where clause: {where_clause}. Currently supported format: UPDATE EMPLOYEE SET salary = 150.00 WHERE name = 'John'"
            )

        column_name, matching_value = self.parse_where(where_clause)

        # find row in tables[table_name] that matches the column name and matching value
        for row in table:
            if row[column_name] == matching_value:
                row[set_column] = set_value

                # if indexing structure exists, then update the row in the indexing structure
                if table_name in self.indexing_structures:
                    # extract primary key column name and value
                    primary_key_column = next(
                        column
                        for column in row
                        if "primary_key" in self.table_schemas[table_name][column]
                    )
                    primary_key_value = row[primary_key_column]

                    self.indexing_structures[table_name][primary_key_value][
                        set_column
                    ] = set_value

        return f"Successfully updated row with {set_column} = {set_value}"

    def parse_where(self, where_clause):
        # Only support equality condition for now
        equality_condition = where_clause["eq"]
        # extract column name from equality condition array
        column_name = equality_condition[0]
        # flatten and extract matching value from equality condition array
        if isinstance(equality_condition[1], dict):
            # if value does not contain "literal" key, then raise error
            if "literal" not in equality_condition[1]:
                raise ValueError(
                    f"Invalid where clause: {where_clause}. Currently supported type: string literal and number"
                )
            equality_condition[1] = equality_condition[1]["literal"]
        matching_value = equality_condition[1]

        return column_name, matching_value

    def drop_table(self, table_name):
        del self.tables[table_name]
        del self.table_schemas[table_name]
        if table_name in self.indexing_structures:
            del self.indexing_structures[table_name]
