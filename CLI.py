import cmd
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
        print("Enter your CREATE TABLE command:")
        command = input()
        try:
            parsed_command = parse(command)
            if "create table" in parsed_command:
                self.database.create_table(parsed_command["create table"])
                print(f"Table created successfully.")
            else:
                print(f"The command didn't specify a create table action.")
        except ValueError as e:
            print(f"An error occurred: {e}")

    def do_load_data(self, arg):
        """Load data into a table from a CSV file: LOAD DATA <table_name> <csv_file_path>"""
        try:
            parts = arg.split()
            if len(parts) == 2:
                table_name, csv_path = parts
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
        print("Delete table not implemented yet.")

    def do_run_query(self, line):
        """Run a query on the database: QUERY your_sql_query;"""
        print("Run query not implemented yet.")

    def do_exit(self, arg):
        """Exit the CLI"""
        print("Exiting the SQL database CLI.")
        return True  # return True to exit the CLI

    def default(self, line):
        print(f"Unknown command: {line}")

    def emptyline(self):
        pass  # Do nothing on an empty line input


