from create_table import CreateTable
from sqlglot.executor import execute

basicTable = CreateTable()

# Parsing the SQL CREATE commands with constraints and initializing the table structure
command = """
CREATE TABLE sushi (
    id INT NOT NULL,
    price DECIMAL(5, 2) NOT NULL,
    PRIMARY KEY (id)
);
"""
command2 = """
CREATE TABLE order_items (
    sushi_id INT NOT NULL,
    order_id INT NOT NULL,
    PRIMARY KEY (sushi_id, order_id)
);
"""
command3 = """
CREATE TABLE orders (
    id INT NOT NULL,
    user_id INT NOT NULL,
    PRIMARY KEY (id)
);
"""

table_name, schema = basicTable.parse_create_command(command)
print(f"Parsed schema for {table_name}: {schema}")
basicTable.initialize_table_structure(table_name, schema)
table_name2, schema2 = basicTable.parse_create_command(command2)
print(f"Parsed schema for {table_name2}: {schema2}")
basicTable.initialize_table_structure(table_name2, schema2)
table_name3, schema3 = basicTable.parse_create_command(command3)
print(f"Parsed schema for {table_name3}: {schema3}")
basicTable.initialize_table_structure(table_name3, schema3)

# Populate the tables with data from CSV files

# ... [previous code]

# Populate the tables with data from CSV files
try:
    basicTable.populate_table_from_csv(table_name, '/Users/nickalarcon/Desktop/COS-3510_DMBS-Project/COSC-3510_DBMS_Project/test_data.csv')
except ValueError as e:
    print("Expected columns:", basicTable.table_schemas[table_name])
    print("CSV columns:", basicTable.tables[table_name])
    raise e

# ... [rest of the code]

basicTable.populate_table_from_csv(table_name2, '/Users/nickalarcon/Desktop/COS-3510_DMBS-Project/COSC-3510_DBMS_Project/test_data2.csv')
basicTable.populate_table_from_csv(table_name3, '/Users/nickalarcon/Desktop/COS-3510_DMBS-Project/COSC-3510_DBMS_Project/test_data3.csv')

# Create B-tree indexes for tables with primary keys
basicTable.create_btree_index(table_name)
basicTable.create_btree_index(table_name2)
basicTable.create_btree_index(table_name3)

# Print the structure of all tables and schemas
print("Tables:")
for table, rows in basicTable.tables.items():
    print(f"{table}: {rows}")
print("\nSchemas:")
for table, schema in basicTable.table_schemas.items():
    print(f"{table}: {schema}")

# Example query using sqlglot to execute and fetch results
try:
    result = execute(
    """
    SELECT
      o.user_id,
      SUM(s.price * i.quantity) AS total_price
    FROM orders o
    JOIN order_items i
      ON o.id = i.order_id
    JOIN sushi s
      ON i.sushi_id = s.id
    GROUP BY o.user_id
    """,
    tables=basicTable.tables
    )
    print("\nQuery Results:")
    print(result)
except Exception as e:
    print(f"\nAn Error occurred: {e}")


