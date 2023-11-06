import csv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis, Function
from sqlparse import tokens
from sqlparse.tokens import Keyword, Token, Punctuation, DML, Whitespace, Literal, Name, Number
from bintrees import BinaryTree


class CreateTable:
    def __init__(self):
        self.tables = {}
        self.bplus_trees = {}
        self.table_schemas = {}

    def parse_create_command(self, command):
        parsed = sqlparse.parse(command)[0]
        table_name = None
        columns = {}
        primary_key = []

        # Tokenize and extract table name and columns
        tokens = [token for token in parsed.tokens if not token.is_whitespace]
        for i, token in enumerate(tokens):
            if token.ttype is Keyword and token.value.upper() == 'TABLE':
                table_name_token = tokens[i + 1]
                if isinstance(table_name_token, Identifier):
                    table_name = table_name_token.get_name()
                else:
                    raise TypeError(f"Unexpected token type {type(table_name_token)} for table name.")
            elif isinstance(token, Parenthesis):
                # Extract everything within the parenthesis
                column_tokens = [t for t in token.tokens if not t.is_whitespace and not t.ttype is Punctuation]
                # Handle column definitions and constraints within the parenthesis
                columns, primary_key = self._extract_columns_and_constraints(column_tokens)

        if not table_name or not columns:
            raise ValueError("Table name or columns could not be parsed.")

        # Add primary key constraints to the columns
        for pk in primary_key:
            if pk in columns:
                columns[pk]['constraints'].append('PRIMARY KEY')
            else:
                raise ValueError(f"Primary key column '{pk}' is not defined in the table.")

        self.table_schemas[table_name] = columns
        return table_name, columns

    def _extract_columns_and_constraints(self, tokens):
        columns = {}
        primary_key = []
        idx = 0

        print(f"Extracting columns from tokens: {tokens}")

        while idx < len(tokens):
            token = tokens[idx]
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    # Since get_identifiers splits by comma, the datatype and constraints are part of the identifier
                    # We need to collect all tokens related to one column before parsing
                    if isinstance(identifier, Identifier) or isinstance(identifier, Function):
                        column_tokens = [identifier] + tokens[idx + 1:]
                        col_name, col_def = self._parse_column(column_tokens)
                        columns[col_name] = col_def
                        break  # Break after finding a full column definition
            elif isinstance(token, Identifier) or isinstance(token, Function):
                # Collect all tokens for this column until the next comma or end of tokens
                column_tokens = []
                while idx < len(tokens) and not tokens[idx].match(Punctuation, ','):
                    column_tokens.append(tokens[idx])
                    idx += 1
                # Now parse the column using all its tokens
                idx -= 1  # Adjust idx to point to the comma after collecting tokens
                col_name, col_def = self._parse_column(column_tokens)
                columns[col_name] = col_def
            elif token.match(Keyword, "PRIMARY KEY"):
                idx += 1  # Skip PRIMARY KEY token
                pk_token = tokens[idx]
                if isinstance(pk_token, Parenthesis):
                    primary_key_tokens = pk_token.tokens[1:-1]  # Exclude the opening and closing parenthesis
                    primary_key = self._parse_primary_key(primary_key_tokens)
                    for pk_column in primary_key:
                        if pk_column in columns:
                            columns[pk_column]['constraints'].append('PRIMARY KEY')
                        else:
                            raise ValueError(f"Primary key column '{pk_column}' not found in columns list")
            idx += 1

        return columns, primary_key

    def _parse_column(self, tokens):
        # Initialize the column name, data type, and constraints
        col_name = None
        data_type = []
        constraints = []

        # Assuming the first token is the Identifier with the column name
        col_name_token = tokens[0]
        if isinstance(col_name_token, Identifier):
            col_name = col_name_token.get_name()
        else:
            raise ValueError("First token must be an Identifier with the column name")

        # Iterate over the rest of the tokens
        for token in tokens[1:]:
            if isinstance(token, Function):
                # Handle data type for DECIMAL(5, 2) as Function
                data_type = [
                    token.get_real_name() + ''.join(t.value for t in token.tokens if t.ttype is not Whitespace)]
            elif token.ttype in (Keyword, Name.Builtin) and not data_type:
                # The first Keyword or Builtin after the column name is assumed to be the data type
                data_type = [token.value]
            elif token.ttype in (Keyword, Name.Builtin) and data_type:
                # If we already have a data type, this must be a constraint
                constraints.append(token.value.upper())

        # Check if the data type was found
        if not data_type:
            raise ValueError(f"No data type found for column {col_name}")

        # Construct the column definition
        col_def = {
            'data_type': ' '.join(data_type).strip(),
            'constraints': constraints
        }

        return col_name, col_def


    def _parse_primary_key(self, tokens):
        # This method will parse the primary key column names from the tokens
        primary_key = []
        for token in tokens:
            if isinstance(token, Identifier):
                primary_key.append(token.get_name())
            elif token.ttype is Punctuation:
                continue  # Skip punctuation like commas
            else:
                raise ValueError(f"Unexpected token type in PRIMARY KEY definition: {token}")
        return primary_key

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

        with open(csv_filename, 'r', encoding='utf-8') as file:  # Added encoding specification
            reader = csv.DictReader(file)
            expected_columns = set(self.table_schemas[table_name].keys())
            csv_columns = set(reader.fieldnames)

            if not csv_columns:
                print(f"CSV file seems to be empty or without headers: {csv_filename}")
                return

            if csv_columns != expected_columns:
                print(f"Expected columns: {expected_columns}")
                print(f"CSV columns: {csv_columns}")
                raise ValueError(f"CSV columns do not match table columns for {table_name}!")

            for row in reader:
                # Convert types as per the schema before appending
                converted_row = {
                    column: self._convert_type(row[column], self.table_schemas[table_name][column]['data_type'])
                    for column in row}
                self.tables[table_name].append(converted_row)

    def _convert_type(self, value, data_type):
        if data_type == 'INT':
            return int(value)
        elif data_type.startswith('FLOAT') or data_type.startswith('DECIMAL'):
            return float(value)
        elif data_type == 'DATE':
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
            if 'PRIMARY KEY' in column_def['constraints']:
                return column
        return None
