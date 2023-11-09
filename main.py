from create_table import CreateTable
from executor import execute
from mo_sql_parsing import parse

basicTable = CreateTable()

# example commands
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
    PRIMARY KEY (sushi_id, order_id),
    FOREIGN KEY (sushi_id) REFERENCES sushi(id),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);
"""
command3 = """
CREATE TABLE orders (
    id INT NOT NULL,
    user_id INT NOT NULL,
    PRIMARY KEY (id)
);
"""
command4 = """
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    NewCustomerID INT,
    NewCustomerName VARCHAR(255),
    FOREIGN KEY (NewCustomerID, NewCustomerName) REFERENCES Customers(CustomerID, CustomerName)
);
"""
commands = [command, command2, command3, command4]
# command is parsed into parsed and stored in a dictionary
# parsed stores the names of the commands (e.g. "create table", "orderby")
for command in commands:
    parsed = parse(command)

    # parse the create table command and initialize the table and index structure
    if "create table" in parsed:
        table_name, schema = basicTable.parse_create_command(parsed["create table"])
        # basicTable.initialize_table_structure(table_name, schema)

# # temporarily set the tables
# basicTable.tables = {
#     "sushi": [
#         {"id": 1, "price": 1.0},
#         {"id": 2, "price": 2.0},
#         {"id": 3, "price": 3.0},
#     ],
#     "order_items": [
#         {"sushi_id": 1, "order_id": 1},
#         {"sushi_id": 1, "order_id": 1},
#         {"sushi_id": 2, "order_id": 1},
#         {"sushi_id": 3, "order_id": 2},
#     ],
#     "orders": [
#         {"id": 1, "user_id": 1},
#         {"id": 2, "user_id": 2},
#     ],
# }

# # Populate the tables with data from CSV files

# # ... [previous code]

# # # Populate the tables with data from CSV files
# # try:
# #     basicTable.populate_table_from_csv(
# #         table_name,
# #         "/Users/nickalarcon/Desktop/COS-3510_DMBS-Project/COSC-3510_DBMS_Project/test_data.csv",
# #     )
# # except ValueError as e:
# #     print("Expected columns:", basicTable.table_schemas[table_name])
# #     print("CSV columns:", basicTable.tables[table_name])
# #     raise e

# # # ... [rest of the code]

# # basicTable.populate_table_from_csv(
# #     table_name2,
# #     "/Users/nickalarcon/Desktop/COS-3510_DMBS-Project/COSC-3510_DBMS_Project/test_data2.csv",
# # )
# # basicTable.populate_table_from_csv(
# #     table_name3,
# #     "/Users/nickalarcon/Desktop/COS-3510_DMBS-Project/COSC-3510_DBMS_Project/test_data3.csv",
# # )

# # # Create B-tree indexes for tables with primary keys
# # basicTable.create_btree_index(table_name)
# # basicTable.create_btree_index(table_name2)
# # basicTable.create_btree_index(table_name3)

# # # Print the structure of all tables and schemas
# # print("Tables:")
# # for table, rows in basicTable.tables.items():
# #     print(f"{table}: {rows}")
# # print("\nSchemas:")
# # for table, schema in basicTable.table_schemas.items():
# #     print(f"{table}: {schema}")

# # Example query using sqlglot to execute and fetch results
# try:
#     result = execute(
#         """
#     SELECT
#       o.user_id, i.order_id, i.sushi_id
#     FROM orders o, order_items i
#     WHERE o.id = i.order_id
#     ORDER BY i.sushi_id
#     """,
#         tables=basicTable.tables,
#     )
#     print("\nQuery Results:")
#     print(result)
# except Exception as e:
#     print(f"\nAn Error occurred: {e}")
