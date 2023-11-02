from create_table import CreateTable
from sqlglot.executor import execute

basicTable = CreateTable()

# Parsing the SQL CREATE command and initializing the table structure
command = "CREATE TABLE sushi (id, price);"
command2 = "CREATE TABLE order_items (sushi_id, order_id);"
command3 = "CREATE TABLE orders (id, user_id);"

table_name, columns = basicTable.parse_create_command(command)
table_name2, columns2 = basicTable.parse_create_command(command2)
table_name3, columns3 = basicTable.parse_create_command(command3)

basicTable.initialize_table_structure(table_name, columns)
basicTable.initialize_table_structure(table_name2, columns2)
basicTable.initialize_table_structure(table_name3, columns3)

# Populate the table with data from a CSV file
basicTable.populate_table_from_csv(table_name, 'test_data.csv')
basicTable.populate_table_from_csv(table_name2, 'test_data2.csv')
basicTable.populate_table_from_csv(table_name3, 'test_data3.csv')


# Print the table
print(basicTable.tables)

table_schema = {
    'sushi': {
        'id': 'INT',
        'price': 'FLOAT'
    },
    'order_items': {
        'sushi_id': 'INT',
        'order_id': 'INT'
    },
    'orders': {
        'id': 'INT',
        'user_id': 'INT'
    }
}

# the_tables = basicTable.tables

for table_name, table_rows in basicTable.tables.items():
    for row in table_rows:
        for column, column_type in table_schema[table_name].items():
            if column_type == 'INT':
                row[column] = int(row[column])
            elif column_type == 'FLOAT':
                row[column] = float(row[column])

# Let's try to use SQLGlot


# Schema is optional but doesn't hurt to include, we can clean it up later (create structure in object)
try:
    result = execute(
    """
    SELECT
      o.user_id,
      SUM(s.price) AS price
    FROM orders o
    JOIN order_items i
      ON o.id = i.order_id
    JOIN sushi s
      ON i.sushi_id = s.id
    GROUP BY o.user_id
    """,
    schema=table_schema,
    tables=basicTable.tables,
    )
    print(result)
except Exception as e:
    print(f"An Error occurred: {e}")

