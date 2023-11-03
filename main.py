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

# Adjust types
basicTable.convert_data_type(table_schema)

user_index = basicTable.create_btree_index(basicTable.tables['sushi'], 'id')
# print(user_index)

user_with_id_1 = user_index.get(1)
temp_tables = {}
temp_tables['sushi'] = [user_with_id_1]

print(temp_tables)


# Print the table
print(basicTable.tables)

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
    tables=basicTable.tables
    )
    print(result)
except Exception as e:
    print(f"An Error occurred: {e}")

