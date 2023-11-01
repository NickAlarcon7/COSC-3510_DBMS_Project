from create_table import CreateTable

basicTable = CreateTable()

# Parsing the SQL CREATE command and initializing the table structure
command = "CREATE TABLE users (id, name, age, city);"
table_name, columns = basicTable.parse_create_command(command)
basicTable.initialize_table_structure(table_name, columns)

# Populate the table with data from a CSV file
basicTable.populate_table_from_csv(table_name, 'test_data.csv')

# Print the table
print(basicTable.tables)
