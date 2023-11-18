import cmd
from CustomStyle import CustomStyle
from mo_sql_parsing import parse
from prompt_toolkit.lexers import PygmentsLexer
from executor import execute_query
from create_database import Database
from prettytable import PrettyTable
from prompt_toolkit import PromptSession, HTML
from prompt_toolkit.styles import Style, style_from_pygments_cls
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import InMemoryHistory
from pygments.lexers import SqlLexer
from rich.console import Console
from rich import print as rich_print
from rich.markup import escape

bright_green_style = "#00FF00"  # Bright green color
deep_red_style = "#FF0000"     # Deep red color


class DatabaseCLI(cmd.Cmd):

    prompt = "(base-cli)$ "

    def __init__(self):
        super().__init__()
        self.console = Console()
        self.commands = ["CREATE", "DATABASE", "USE", "TABLE", "LOAD", "DATA", "Exit", "INSERT", "INTO",
                         "SELECT", "Print_Tables", "Print_Schemas", "List_Databases", "SQL_command", "clear", "help",
                         "UPDATE", "SET", "DELETE", "FROM", "WHERE", "DROP", "GROUP", "BY"]
        self.completer = WordCompleter(self.commands, ignore_case=True, match_middle=False)
        self.session = PromptSession(completer=self.completer, history=InMemoryHistory(),
                                     lexer=PygmentsLexer(SqlLexer), style=style_from_pygments_cls(CustomStyle))
        self.prompt_style = Style.from_dict({
            "prompt": "ansiblue"})
        self.databases = {}
        self.current_database = None  # Current database in use

    def update_AutoComplete(self, table_nm):
        self.commands.append(table_nm)

        for column in self.current_database.table_schemas[table_nm]:
            if column not in self.commands:
                self.commands.append(column)

    # Remember to update the autocomplete list when a INSERT INTO command is executed

    def cmdloop(self, intro=None):
        """Override the cmdloop method to use Prompt Toolkit for input."""
        intro = """
        ╔─────────────────────────────────────────────────────────────╗
        │ ██████   █████           █████████     ██████    █████      │
        │  ██████   ███           ███     ███  ███    ███   ███       │
        │  ███ ███  ███  ███  ███ ███         ███      ███  ███       │
        │  ███  ███ ███   ██  ██   █████████  ███      ███  ███       │
        │  ███   ██████   ██  ██          ███ ███   ██ ███  ███       │
        │  ███    █████   ██  ██  ███     ███  ███   ████   ███      █│
        │ █████    █████   ████    █████████     ██████ ██ ███████████│
        ╚─────────────────────────────────────────────────────────────╝                               
            """
        self.console.print(f"[bold blue]{intro}[bold blue]\n")
        self.console.print("[bold blue]Welcome to the NuSQL database CLI ![/bold blue]")
        self.console.print("[yellow]Type help or ? to list commands.[/yellow]")
        self.console.print("[yellow]Type SQL_command to enter a SQL command.[/yellow]")

        stop = None
        while not stop:
            try:
                prompt_text = HTML(f'<ansiblue>{self.prompt}</ansiblue>')
                line = self.session.prompt(prompt_text)
                stop = self.onecmd(line)
            except KeyboardInterrupt:
                continue
            except EOFError:
                break

    def databases_exist(self):
        return bool(self.databases)

    def do_SQL_command(self, line):
        """Use this command to enter a SQL command. Example: SQL_command CREATE DATABASE mydb;"""
        if line is None or line.strip() == "":
            self.console.print("Enter your SQL command:", style=deep_red_style)
            line = input()

        command = line.strip().lower()

        try:
            # Handle SQL commands
            if command.startswith("create database"):
                database_name = line.split()[2]
                self.create_Database(database_name)
            elif command.startswith("use"):
                database_name = line.split()[1]
                self.use_Database(database_name)
            elif command.startswith("create table"):
                self.create_Table(line)
            elif command.startswith("load data"):
                self.load_Data(line)
            elif command.startswith("insert into"):
                parsed_command = parse(line)
                self.insert_into(parsed_command)
            elif command.startswith("delete from"):
                parsed_command = parse(line)
                self.delete_from(parsed_command)
            elif command.startswith("update"):
                parsed_command = parse(line)
                self.update(parsed_command)
            elif command.startswith("drop table"):
                parsed_command = parse(line)
                self.drop_Table(parsed_command)
            elif command.startswith("exit"):
                self.do_Exit()
            else:
                parsed_command = parse(line)
                if "select" in parsed_command:
                    self.run_Query(line)
                else:
                    self.console.print(f"Invalid command: {line}", style=deep_red_style)
        except Exception as e:
            self.console.print(f"An error occurred while parsing the command: {e}", style=deep_red_style)

    def onecmd(self, line):
        if not self.current_database and line.split()[0] not in (
            "help",
            "SQL_command",
            "List_Databases",
            "Exit",
            "?",
            "Print_Tables",
            "Print_Schemas",
            "clear",
            "exit",
            "Clear"
        ):
            print(
                f"Please use the 'SQL_command' command to create a database or use an existing one."
            )
            return False
        else:
            return super().onecmd(line)

    def insert_into(self, parsed_command):
        if self.current_database is None:
            self.console.print("No database selected.", style=deep_red_style)
            return

        try:
            table_name = parsed_command.get('insert')
            columns = parsed_command.get('columns', [])
            values = parsed_command.get('values', )
            # Assuming values is a list of tuples or a single tuple

            if not values:
                self.console.print("No values provided for insertion", style=deep_red_style)
                return

            # Call the insert method of the current database
            self.current_database.insert(table_name, columns, values)
            self.console.print(f"Data inserted into table {table_name} successfully.", style=bright_green_style)

        except Exception as e:
            self.console.print(f"An error occurred while trying to insert data: {e}", style=deep_red_style)

    def delete_from(self, parsed_command):
        if self.current_database is None:
            self.console.print("No database selected.", style=deep_red_style)
            return

        try:
            table_name = parsed_command.get("delete")
            where_clause = parsed_command.get("where")
            # The where clause is a dictionary representing the condition

            confirmation_msg = HTML(
                f"Are you sure you want to delete data from table <ansired>{table_name}</ansired>? (y/n)\n")
            answer = self.session.prompt(confirmation_msg)

            if answer.lower() == "y":
                # Call the delete method of the current database
                self.current_database.delete(table_name, where_clause)
                self.console.print(f"Data deleted from table {table_name} successfully.", style=bright_green_style)
            else:
                self.console.print(f"Data not deleted from table {table_name}.", style=deep_red_style)
        except Exception as e:
            self.console.print(f"An error occurred while trying to delete data: {e}", style=deep_red_style)

    def update(self, parsed_command):
        if self.current_database is None:
            self.console.print("No database selected.", style=deep_red_style)
            return

        try:
            table_name = parsed_command.get("update")
            assignments = parsed_command.get("set")
            where_clause = parsed_command.get("where")
            # The assignments are typically a dictionary of column-value pairs
            # The where_clause is a dictionary representing the condition

            # Call the update method of the current database
            self.current_database.update(table_name, assignments, where_clause)
            self.console.print(f"Data updated in table {table_name} successfully.", style=bright_green_style)
        except Exception as e:
            self.console.print(f"An error occurred while trying to update data: {e}", style=deep_red_style)

    def create_Database(self, database_name):
        """Create a new database: CREATE DATABASE database_name;"""

        if database_name is None or database_name == "":
            self.console.print("Enter your CREATE DATABASE <database_name> command:", style=deep_red_style)
            database_name = input()
        parts = database_name.split()
        if len(parts) == 3:
            part1, part2, database_name = parts
            if part1.lower() != "create" and part2.lower() != "database":
                self.console.print(f"Invalid command: {part1}", style=deep_red_style)
                return
        elif len(parts) == 1:
            database_name = parts[0]
        else:
            self.console.print(f"Invalid command: {parts}", style=deep_red_style)
            return
        if database_name in self.databases:
            self.console.print(f"A database with the name '{database_name}' already exists.", style=deep_red_style)
        else:
            self.databases[database_name] = Database()
            self.current_database = self.databases[database_name]
            self.prompt = f"({database_name}-cli)> "
            self.console.print(f"Database '{database_name}' created successfully.", style=bright_green_style)

    def use_Database(self, database_name):
        """Switch to an existing database: USE database_name;"""

        if database_name is None or database_name == "":
            self.console.print("Enter your USE <database_name> command:", style=deep_red_style)
            database_name = input()
        parts = database_name.split()
        if len(parts) == 2:
            part1, database_name = parts
            if part1.lower() != "use":
                self.console.print(f"Invalid command: {part1}", style=deep_red_style)
                return
        elif len(parts) == 1:
            database_name = parts[0]
        else:
            self.console.print(f"Invalid command: {parts}", style=deep_red_style)
            return
        if database_name not in self.databases:
            self.console.print(f"No database found with the name '{database_name}'.", style=deep_red_style)
        else:
            self.current_database = self.databases[database_name]
            self.prompt = f"({database_name}-cli)> "
            self.console.print(f"Switched to database '{database_name}'.", style=bright_green_style)

    def create_Table(self, arg):
        """Create a new table: CREATE TABLE ..."""
        if self.current_database is None:
            self.console.print("No database in use. Try creating one first", style=deep_red_style)
            return
        if arg is None or arg == "":
            self.console.print("Enter your CREATE TABLE command:", style=deep_red_style)
            arg = input()
        try:
            parsed_command = parse(arg)
            if "create table" in parsed_command:
                schema_to_autocomplete = self.current_database.create_table(parsed_command["create table"])
                self.update_AutoComplete(schema_to_autocomplete)
                self.console.print("Table created successfully.", style=bright_green_style)
            else:
                self.console.print("The command didn't specify a create table action.", style=deep_red_style)
        except Exception as e:
            self.console.print(f"An error occurred: {e}", style=deep_red_style)

    def load_Data(self, arg):
        """Load data into a table from a CSV file: LOAD DATA <table_name> <csv_file_path>"""
        if self.current_database is None:
            self.console.print("No database in use. Try creating one first", style=deep_red_style)
            return
        if not self.current_database.tables:
            self.console.print("No tables in the database. Try creating one first", style=deep_red_style)
            return
        if arg is None or arg == "":
            self.console.print("Enter your LOAD DATA command:", style=deep_red_style)
            arg = input()
        try:
            parts = arg.split()
            if len(parts) == 4:
                part1, part2, table_name, csv_path = parts
                if part1.lower() + part2.lower() != "loaddata":
                    self.console.print(f"Invalid command: {arg}", style=deep_red_style)
                    return
                if table_name not in self.current_database.tables:
                    self.console.print(f"Table {table_name} does not exist.", style=deep_red_style)
                    return
                self.current_database.populate_table_from_csv(table_name, csv_path)
                self.console.print(f"Data loaded into table {table_name} from {csv_path}.", style=bright_green_style)
            else:
                if len(parts) == 2:
                    table_name, csv_path = parts
                    if table_name not in self.current_database.tables:
                        self.console.print(f"Table {table_name} does not exist.", style=deep_red_style)
                        return
                    self.current_database.populate_table_from_csv(table_name, csv_path)
                    self.console.print(f"Data loaded into table {table_name} from {csv_path}.",
                                       style=bright_green_style)
        except ValueError as e:
            self.console.print(f"An error occurred: {e}", style=deep_red_style)

    def drop_Table(self, parsed_command):
        if self.current_database is None:
            self.console.print("No database in use.", style=deep_red_style)
            return

        try:
            table_name = parsed_command.get("drop")
            if table_name not in self.current_database.tables:
                self.console.print(f"Table {table_name} does not exist.", style=deep_red_style)
                return

            confirmation_msg = HTML(f"Are you sure you want to drop table <ansired>{table_name}</ansired>? (y/n)\n")
            answer = self.session.prompt(confirmation_msg)

            if answer.lower() == "y":
                # Call the drop_table method of the current database
                self.current_database.drop_table(table_name)
                self.console.print(f"Table {table_name} dropped successfully.", style=bright_green_style)
            else:
                self.console.print(f"Table {table_name} not dropped.", style=deep_red_style)

        except Exception as e:
            self.console.print(f"An error occurred while trying to drop table: {e}", style=deep_red_style)

    def run_Query(self, line):
        """Run a query on the database: QUERY your_sql_query;"""
        if self.current_database is None:
            self.console.print("No database in use. Try creating one first", style=deep_red_style)
            return
        if not self.current_database.tables:
            self.console.print("No tables in the database. Try creating one first", style=deep_red_style)
            return
        if line is None or line == "":
            self.console.print("Enter your QUERY command:", style=deep_red_style)
            line = input()
        try:
            results = execute_query(f"""{line}""", self.current_database)
            if results:
                table = PrettyTable()
                table.field_names = results.columns
                table.hrules = 1

                for row in results.rows:
                    table.add_row(row)

                # ANSI Blue color start code
                blue_start = "\033[94m"
                # ANSI color reset code
                reset = "\033[0m"

                # Print the table with blue color
                print(blue_start + str(table) + reset)
                self.console.print("\nQuery executed successfully.", style=bright_green_style)
            else:
                self.console.print("No data returned.", style=deep_red_style)

        except Exception as e:
            self.console.print(f"An error occurred while trying to run query: {e}", style=deep_red_style)

    def do_Print_Tables(self, line):
        """Print all tables in the database or a specific table;"""
        if self.current_database is None:
            self.console.print("No database in use. Try creating one first", style=deep_red_style)
            return
        try:
            if not self.current_database.tables:
                self.console.print("No tables in the database.", style=deep_red_style)
                return
            if line is None or line == "":
                for table in self.current_database.tables:
                    if not self.current_database.tables[table]:
                        self.console.print(f"Table for {escape(table)} is empty.", style=deep_red_style)
                    else:
                        rich_print(f"[blue]Table for {escape(table)}:[/blue]")
                        print(self.current_database.tables[table])
                        self.current_database.print_table(table)
                        print("\n" * 2)
            else:
                rich_print(f"[blue]Table for {escape(line)}:[/blue] \n")
                print(self.current_database.tables[line])
                self.current_database.print_table(line)
        except ValueError as e:
            self.console.print(f"An error occurred while trying to print tables: {e}", style=deep_red_style)

    def do_Print_Schemas(self, line):
        """Print all schemas in the database or a specific schema;"""
        if self.current_database is None:
            self.console.print("No database in use. Try creating one first", style=deep_red_style)
            return
        try:
            if not self.current_database.table_schemas:
                self.console.print("No schemas in the database.", style=deep_red_style)
                return
            if line is None or line == "":
                for schema in self.current_database.table_schemas:
                    rich_print(f"[blue]Schema for {escape(schema)}:[/blue]")
                    print(self.current_database.table_schemas[schema])
                    print("\n" * 2)
            else:
                rich_print(f"[blue]Schema for {escape(line)}:[/blue] \n")
                print(self.current_database.table_schemas[line])
        except ValueError as e:
            self.console.print(f"An error occurred while trying to print schemas: {e}", style=deep_red_style)

    def do_List_Databases(self, line):
        """List all databases"""
        if not self.databases_exist():
            self.console.print("No databases have been created.", style=deep_red_style)
        else:
            self.console.print("[blue]Databases:[/blue]")
            for db_name in self.databases:
                self.console.print(f"- {escape(db_name)}")

    def do_Exit(self, arg):
        """Exit the CLI"""
        exit_msg_style = "ansired"  # ANSI red for the prompt

        if arg:
            return True

        if self.current_database:
            exit_msg = HTML(
                f'<{exit_msg_style}>Are you sure you want to exit the current database session ? (y/n)</{exit_msg_style}>\n')
            answer = self.session.prompt(exit_msg)
            if answer.lower() == "y":
                self.current_database = None
                self.prompt = "(base-cli)$ "
                self.console.print("Exiting the current database session.", style=deep_red_style)
            else:
                self.console.print("Database session exit aborted.", style=deep_red_style)
        else:
            exit_msg = HTML(
                f'<{exit_msg_style}>Are you sure you want to exit the SQL database CLI? (y/n)</{exit_msg_style}>\n')
            answer = self.session.prompt(exit_msg)
            if answer.lower() == "y":
                self.console.print("Exiting the SQL database CLI.", style=deep_red_style)
                return True
            else:
                self.console.print("Exit aborted.", style=deep_red_style)

    def do_help(self, arg):
        # Filter out commands when no database is created or in use
        if not self.databases_exist():
            excluded_commands = ["Print_Tables", "Print_Schemas", "List_Databases"]
        elif not self.current_database:
            excluded_commands = [
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
        if line.lower() == "clear" or line.lower() == "clear ":
            print("\n" * 100)
            return
        if line.lower() == "exit":
            self.do_Exit(True)
        else:
            self.console.print(f"Unknown command: {line}", style=deep_red_style)

    def emptyline(self):
        pass  # Do nothing on an empty line input
