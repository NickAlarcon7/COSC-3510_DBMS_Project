import cmd
import subprocess
from mo_sql_parsing import parse
from create_database import Database


class DatabaseCLI(cmd.Cmd):
    intro = 'Welcome to the SQL database CLI. Type help or ? to list commands.\n'
    prompt = '(database) '

    def __init__(self, database_instance):
        super().__init__()
        self.database = database_instance

    def do_create_table(self, arg):
        """Create a new table: CREATE TABLE ..."""
        if arg is None or arg == "":
            print("Enter your CREATE TABLE command:")
            arg = input()
        try:
            parsed_command = parse(arg)
            if "create table" in parsed_command:
                self.database.create_table(parsed_command["create table"])
                print(f"Table created successfully.")
            else:
                print(f"The command didn't specify a create table action.")
        except ValueError as e:
            print(f"An error occurred: {e}")

    def do_load_data(self, arg):
        """Load data into a table from a CSV file: LOAD DATA <table_name> <csv_file_path>"""
        if self.database.tables is None or len(self.database.tables) == 0:
            print("No tables in the database. Try creating one first genius")
            return
        if arg is None or arg == "":
            print("Enter your LOAD DATA command:")
            arg = input()
        try:
            parts = arg.split()
            if len(parts) == 4:
                part1, part2, table_name, csv_path = parts
                if part1.lower() + part2.lower() != "loaddata":
                    print(f"Invalid command: {arg}")
                    return
                if table_name not in self.database.tables:
                    print(f"Table {table_name} does not exist.")
                    return
                self.database.populate_table_from_csv(table_name, csv_path)
                print(f"Data loaded into table {table_name} from {csv_path}.")
            else:
                print(f"Invalid number of arguments.")
        except ValueError as e:
            print(f"An error occurred: {e}")

    def do_edit_table(self, line):
        """Edit an existing table: EDIT TABLE table_name;"""
        print("Edit table not implemented yet.")

    def do_delete_table(self, line):
        """Delete an existing table: DELETE TABLE table_name;"""
        if line is None or line == "":
            print("Enter your DELETE TABLE command:")
            line = input()
        try:
            # split line and extract table name
            line = line.split()[2]
            if line in self.database.tables:
                print(f"Are you sure you want to delete table {line}? (y/n)")
                answer = input()
                if answer == "y":
                    del self.database.tables[line]
                    print(f"Table {line} deleted successfully.")
                else:
                    print(f"Table {line} not deleted.")
            else:
                print(f"Table {line} does not exist.")
        except ValueError as e:
            print(f"An error occurred while trying delete_table: {e}")

    def do_run_query(self, line):
        """Run a query on the database: QUERY your_sql_query;"""
        print("Run query not implemented yet.")

    def do_print_tables(self, line):
        """Print all tables in the database or a specific table;"""
        try:
            if self.database.tables is None or len(self.database.tables) == 0:
                print("No tables in the database.")
                return
            if line is None or line == "":
                for table in self.database.tables:
                    print(f"Table for {table}:")
                    print(self.database.tables[table])
                    self.database.print_table(table)
                    print("\n" * 2)
            else:
                print(f"Table for {line}: \n")
                print(self.database.tables[line])
                self.database.print_table(line)
        except ValueError as e:
            print(f"An error occurred while trying to print tables: {e}")

    def do_print_schemas(self, line):
        """Print all schemas in the database or a specific schema;"""
        try:
            if self.database.table_schemas is None or len(self.database.table_schemas) == 0:
                print("No schemas in the database.")
                return
            if line is None or line == "":
                for schema in self.database.table_schemas:
                    print(f"Schema for {schema}:")
                    print(self.database.table_schemas[schema])
                    print("\n" * 2)
            else:
                print(f"Schema for {line}: \n")
                print(self.database.table_schemas[line])
        except ValueError as e:
            print(f"An error occurred while trying to print schemas: {e}")

    def do_exit(self, arg):
        """Exit the CLI"""
        print("Are you sure you want to exit? (y/n)")
        answer = input()
        if answer == "y":
            print("Exiting the SQL database CLI.")
            return True
        else:
            print("Exit aborted.")

    def default(self, line):
        if line.lower() == "clear":
            print("\n" * 100)
        else:
            print(f"Unknown command: {line}")

    def emptyline(self):
        pass  # Do nothing on an empty line input


