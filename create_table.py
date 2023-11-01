import csv
import sqlparse


class CreateTable:
    def __init__(self):
        self.tables = {}

    def parse_create_command(self, command):
        # Parse the SQL command
        parsed = sqlparse.parse(command)[0]
        # print(f"Tokens: {[token for token in parsed.tokens if not token.is_whitespace]}")  # Debug print

        # Ensure it's a CREATE command
        if not parsed.get_type() == 'CREATE':
            raise ValueError("Only CREATE TABLE commands are supported.")

        # Initialize variables to hold the table name and columns
        table_name = None
        columns = []

        # Tokenize and extract table name and columns
        tokens = [token for token in parsed.tokens if not token.is_whitespace]
        for i, token in enumerate(tokens):
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'TABLE':
                table_name_token = tokens[i + 1]
                table_name = table_name_token.get_name()
            if isinstance(token, sqlparse.sql.Parenthesis):
                # Extract column names
                inner_tokens = token.tokens[1:-1]  # Remove the surrounding parentheses
                for inner_token in inner_tokens:
                    if isinstance(inner_token, sqlparse.sql.IdentifierList):
                        columns.extend(str(t.get_name()) for t in inner_token.get_identifiers())
                    elif isinstance(inner_token, sqlparse.sql.Identifier):
                        columns.append(str(inner_token.get_name()))

        # Check if table name and columns are found
        if table_name is None or not columns:
            raise ValueError("Table name or columns could not be parsed.")

        return table_name, columns

    def initialize_table_structure(self, table_name, columns):
        # Check if table already exists
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists!")

        # Create an empty list for the table, which will hold row data as dictionaries
        # where each dictionary has keys corresponding to the column names
        self.tables[table_name] = [dict.fromkeys(columns)]

    def populate_table_from_csv(self, table_name, csv_filename):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist!")

        with open(csv_filename, 'r') as file:
            reader = csv.DictReader(file)

            # Assuming the first row in self.tables[table_name] has all the columns as keys
            if self.tables[table_name]:  # Check if there's at least one row
                expected_columns = set(self.tables[table_name][0].keys())
            else:  # If no rows, assume the columns set is empty
                expected_columns = set()

            csv_columns = set(reader.fieldnames)

            if csv_columns != expected_columns:
                raise ValueError(f"CSV columns do not match table columns for {table_name}!")

            self.tables[table_name].clear()  # Clear the placeholder row if it exists

            for row in reader:
                self.tables[table_name].append(row)



