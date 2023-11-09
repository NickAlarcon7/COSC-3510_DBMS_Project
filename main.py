from create_database import Database
from executor import execute
from mo_sql_parsing import parse
from CLI import DatabaseCLI


if __name__ == '__main__':
    # Here you create an instance of your Database class
    database_instance = Database()
    # Now pass this instance to the DatabaseCLI
    DatabaseCLI(database_instance).cmdloop()

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
