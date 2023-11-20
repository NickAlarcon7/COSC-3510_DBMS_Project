# Description: Main entry point for the application
from Program_files.CLI import DatabaseCLI

if __name__ == "__main__":
    # Create a new database instance
    cli = DatabaseCLI()
    cli.cmdloop()
