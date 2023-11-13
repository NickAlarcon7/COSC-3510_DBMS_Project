import cmd
from mo_sql_parsing import parse
from executor import execute_query
from create_database import Database


class DatabaseCLI(cmd.Cmd):
    intro = "Welcome to the SQL database CLI. Type help or ? to list commands.\n"
    prompt = "(base-cli)$ "

    def __init__(self):
        super().__init__()
        self.databases = {}
        self.current_database = None  # Current database in use

    def databases_exist(self):
        return bool(self.databases)

    def onecmd(self, line):
        if not self.current_database and line.split()[0] not in (
            "help",
            "Create_Database",
            "List_Databases",
            "Exit",
            "Use_Database",
            "?",
        ):
            print(
                "No database is currently in use. Please create or use a database first."
            )
            return False
        else:
            return super().onecmd(line)

    def do_Create_Database(self, database_name):
        """Create a new database: CREATE database_name;"""
        if database_name is None or database_name == "":
            print("Enter your CREATE <database_name> command:")
            database_name = input()
        parts = database_name.split()
        if len(parts) == 2:
            part1, database_name = parts
            if part1.lower() != "create":
                print(f"Invalid command: {part1}")
                return
        else:
            print(f"Invalid number of arguments.")
            return
        if database_name in self.databases:
            print(f"A database with the name '{database_name}' already exists.")
        else:
            self.databases[database_name] = Database()
            self.current_database = self.databases[database_name]
            self.prompt = f"({database_name}-cli)> "
            print(f"Database '{database_name}' created successfully.")

    def do_Use_Database(self, database_name):
        """Switch to an existing database: USE database_name;"""
        if database_name is None or database_name == "":
            print("Enter your USE <database_name> command:")
            database_name = input()
        parts = database_name.split()
        if len(parts) == 2:
            part1, database_name = parts
            if part1.lower() != "use":
                print(f"Invalid command: {part1}")
                return
        else:
            print(f"Invalid number of arguments.")
            return
        if database_name not in self.databases:
            print(f"No database found with the name '{database_name}'.")
        else:
            self.current_database = self.databases[database_name]
            self.prompt = f"({database_name}-cli)> "
            print(f"Switched to database '{database_name}'.")

    def do_Create_Table(self, arg):
        """Create a new table: CREATE TABLE ..."""
        if self.current_database is None:
            print("No database in use. Try creating one first")
            return
        if arg is None or arg == "":
            print("Enter your CREATE TABLE command:")
            arg = input()
        try:
            parsed_command = parse(arg)
            if "create table" in parsed_command:
                self.current_database.create_table(parsed_command["create table"])
                print(f"Table created successfully.")
            else:
                print(f"The command didn't specify a create table action.")
        except ValueError as e:
            print(f"An error occurred: {e}")

    def do_Load_Data(self, arg):
        """Load data into a table from a CSV file: LOAD DATA <table_name> <csv_file_path>"""
        if self.current_database is None:
            print("No database in use. Try creating one first")
            return
        if (
            self.current_database.tables is None
            or len(self.current_database.tables) == 0
        ):
            print("No tables in the database. Try creating one first")
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
                if table_name not in self.current_database.tables:
                    print(f"Table {table_name} does not exist.")
                    return
                self.current_database.populate_table_from_csv(table_name, csv_path)
                print(f"Data loaded into table {table_name} from {csv_path}.")
            else:
                print(f"Invalid number of arguments.")
        except ValueError as e:
            print(f"An error occurred: {e}")

    def do_Edit_Table(self, line):
        """Edit an existing table: EDIT TABLE table_name;"""
        print("Edit table not implemented yet.")

    def do_Delete_Table(self, line):
        """Delete an existing table: DELETE TABLE table_name;"""
        if (
            self.current_database.tables is None
            or len(self.current_database.tables) == 0
        ):
            print("No tables in the database. Try creating one first")
            return
        if line is None or line == "":
            print("Enter your DELETE TABLE command:")
            line = input()
        try:
            # split line and extract table name
            line = line.split()[2]
            if line in self.current_database.tables:
                print(f"Are you sure you want to delete table {line}? (y/n)")
                answer = input()
                if answer == "y":
                    del self.current_database.tables[line]
                    print(f"Table {line} deleted successfully.")
                else:
                    print(f"Table {line} not deleted.")
            else:
                print(f"Table {line} does not exist.")
        except ValueError as e:
            print(f"An error occurred while trying delete_table: {e}")

    def do_Run_Query(self, line):
        """Run a query on the database: QUERY your_sql_query;"""
        if self.current_database is None:
            print("No database in use. Try creating one first")
            return
        if (
            self.current_database.tables is None
            or len(self.current_database.tables) == 0
        ):
            print("No tables in the database. Try creating one first")
            return
        if line is None or line == "":
            print("Enter your QUERY command:")
            line = input()
        try:
            results = execute_query(f"""{line}""", self.current_database)
            print("\nQuery Results:")
            print(results)
        except ValueError as e:
            print(f"An error occurred while trying to run query: {e}")

    def do_Print_Tables(self, line):
        """Print all tables in the database or a specific table;"""
        if self.current_database is None:
            print("No database in use. Try creating one first")
            return
        try:
            if (
                self.current_database.tables is None
                or len(self.current_database.tables) == 0
            ):
                print("No tables in the database.")
                return
            if line is None or line == "":
                for table in self.current_database.tables:
                    if (
                        self.current_database.tables[table] is None
                        or len(self.current_database.tables[table]) == 0
                    ):
                        print(f"Table for {table} is empty.")
                        continue
                    else:
                        print(f"Table for {table}:")
                        print(self.current_database.tables[table])
                        self.current_database.print_table(table)
                        print("\n" * 2)
            else:
                print(f"Table for {line}: \n")
                print(self.current_database.tables[line])
                self.current_database.print_table(line)
        except ValueError as e:
            print(f"An error occurred while trying to print tables: {e}")

    def do_Print_Schemas(self, line):
        """Print all schemas in the database or a specific schema;"""
        if self.current_database is None:
            print("No database in use. Try creating one first")
            return
        try:
            if (
                self.current_database.table_schemas is None
                or len(self.current_database.table_schemas) == 0
            ):
                print("No schemas in the database.")
                return
            if line is None or line == "":
                for schema in self.current_database.table_schemas:
                    print(f"Schema for {schema}:")
                    print(self.current_database.table_schemas[schema])
                    print("\n" * 2)
            else:
                print(f"Schema for {line}: \n")
                print(self.current_database.table_schemas[line])
        except ValueError as e:
            print(f"An error occurred while trying to print schemas: {e}")

    def do_List_Databases(self, line):
        """List all databases"""
        if not self.databases_exist():
            print("No databases have been created.")
        else:
            print("Databases:")
            for db_name in self.databases:
                print(f"- {db_name}")

    def do_Exit(self, arg):
        """Exit the CLI"""
        if self.current_database:
            print("Are you sure you want to exit current database session ? (y/n)")
            answer = input()
            if answer == "y":
                self.current_database = None
                self.prompt = "(base-cli)$ "
                print("Exiting the current database session.")
            else:
                print("Data base session exit aborted.")
        else:
            print("Are you sure you want to exit the SQL database CLI? (y/n)")
            answer = input()
            if answer == "y":
                print("Exiting the SQL database CLI.")
                return True
            else:
                print("Exit aborted.")

    def do_help(self, arg):
        # Filter out commands when no database is created or in use
        if not self.databases_exist():
            excluded_commands = [
                "Create_Table",
                "Load_Data",
                "Edit_Table",
                "Delete_Table",
                "Run_Query",
                "Print_Tables",
                "Print_Schemas",
                "Use_Database",
                "List_Databases",
            ]
        elif not self.current_database:
            excluded_commands = [
                "Create_Table",
                "Load_Data",
                "Edit_Table",
                "Delete_Table",
                "Run_Query",
                "Print_Tables",
                "Print_Schemas",
            ]
        else:
            excluded_commands = []

        if arg:
            # If specific help requested, and it's not an excluded command
            if arg in excluded_commands:
                print(f"The {arg} command is not available right now.")
                return
            else:
                # Call the base class method for specific command help
                super().do_help(arg)
        else:
            # Display help for all non-excluded commands
            cmds_doc = [cmd[3:] for cmd in self.get_names() if cmd.startswith("do_")]
            help_struct = {}
            for cmd in cmds_doc:
                if cmd not in excluded_commands:
                    help_struct[cmd] = 0
            self.print_topics(
                "Documented commands (type help <topic>):",
                sorted(help_struct.keys()),
                15,
                80,
            )

    def default(self, line):
        if line.lower() == "clear":
            print("\n" * 100)
        else:
            print(f"Unknown command: {line}")

    def emptyline(self):
        pass  # Do nothing on an empty line input
