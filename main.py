from create_database import Database
from executor import execute
from mo_sql_parsing import parse
from CLI import DatabaseCLI


if __name__ == "__main__":
    # Create a new database instance
    DatabaseCLI().cmdloop()
