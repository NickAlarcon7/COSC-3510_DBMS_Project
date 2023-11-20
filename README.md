# **NuSQL** - _A Simple Relational Database Management System_

### COSC-3510_DBMS_Project

**Authors: Nick Alarcon, Tony Chen**

![](nuSQLlogo.png)

## Architecture:

- A SQL parser: use mo-sql to identify CREATE TABLE statements. For column data type, we explicitly support INT/INTEGER, FLOAT, DECIMAL, BOOLEAN, VARCHAR. Everything else is treated as string. We cannot enforce the length of VARCHAR since we treat it as string.
- An indexing structure: a BTrees.OOBTree with single attribute primary key as key and a tuple as value (row of data). We use the indexing structure to check for duplicates when inserting data. We mirror the IGNORE keyword behavior in MySQL by skipping duplicate rows. Since multi-attribute primary key is not indexed, we cannot check for primary key duplicates. However, we still check if a duplicate row exists in the table before inserting.
- A query optimizer: use sqlglot to optimize query
- An execution engine: wrap sqlglot executor with index support

**Storage Architecture:**

- Tables - dictionary (
  key: table name - string, value: tuples - dictionary
  )
  example tables:
  ```
  {
      "sushi": [
          {"id": 1, "price": 1.0},
          {"id": 2, "price": 2.0},
          {"id": 3, "price": 3.0},
      ],
      "order_items": [
          {"sushi_id": 1, "order_id": 1},
          {"sushi_id": 1, "order_id": 1},
          {"sushi_id": 2, "order_id": 1},
          {"sushi_id": 3, "order_id": 2},
      ],
      "orders": [
          {"id": 1, "user_id": 1},
          {"id": 2, "user_id": 2},
      ],
  }
  ```
- Schemas - dictionary (
  key: table name - string, value: schema definition - dictionary
  )
  example schemas:
  ```
  {
      "sushi": {
          "id": {"type": "int", "nullable": False, "primary_key": True},
          "price": {"type": {"decimal": [5, 2]}, "nullable": False},
      },
  }
  ```
- Index-tree - dictionary (
  key: table name - string, value: index tree - B tree
  )
  example index-tree:
  ```
  {
      "sushi": <BTrees.OOBTree>
  }
  ```

## SQL Commands:

- > **CREATE TABLE table_name (column_name data_type constraint, column_name data_type constraint, column_name data_type constraint, PRIMARY KEY (column_name))** - Create a entry in the tables dictionary with the table name as the key and an empty array as the value. Create a entry in the table_schemas dictionary with the table name as the key and the schema definition dictionary as the value. Raise error if column has no data type or no primary key is specified. Foreign key and reference are parsed and stored in the schema dictionary but not enforced. When creating with single attribute primary key, indexing happens under the hood. This app does not support CREATE INDEX statements.
- > **LOAD DATA table_name csv_file_path** - Check the schema table to convert the data type to match the schema. Input csv file must contain a header of column names. If input value is empty string or whitespace, and column is not constrained by NOT NULL or primary key, set the value to NONE and process as usual. However, skip rows where it contains null when it should not. We also check for single attribute primary key to insert into index strucuture.
- > **DROP TABLE table_name** - Drop table from tables, table_schemas, and indexing_structures (if exists)
- > **INSERT INTO table_name VALUES (value1, value2, value3)** - Insert row with values into table. Since column list is not specified, values must be listed in the order of their initial definition. Abort if duplicate row or duplicate primary key is found. Foreign key and reference are not enforced
- > **UPDATE table_name SET set_column = set_value WHERE match_column = match_value** - update row with match_value at match_column with set_value at set_column. If where clause is empty, update all rows in table. match_value and set_value must be either string or number. Foreign key and reference are not enforced
- > **DELETE FROM taable_name WHERE column_name = value** - Delete row from table with matching column_name and value. Delete entry from indexing strucuture if exists. If where clause is empty, delete all rows from table. If where clause does not match equal condition, raise error. Foreign key and reference are not enforced
- > **SELECT column_name FROM table_name WHERE column_name = value** -
  > INDEX SUPPORT: If selecting with a where clause from a single table that has a single attribute primary key, create a temp table with the tuple(s) from the indexing structure and feed it to query engine. Detect equality condition in WHERE clause and support multiple equality conditions connected with OR, AND. If one side of OR is indexed and the other side is not, the temp table is the original table because we cannot create a temp table with only the indexed tuple.
  > JOIN OPTIMIZER: If ordering by one of the joining condition, then use merge join. If the size of one table is less than 100, and the size of the other table is less than 10 times the size of the smaller table, then use nested loop join. Otherwise, defaults to hash join.

## TODO:

- [x] How to parse CREATE TABLE? - Use mo.sql to figure out the leading sql command (i.e. CREATE TABLE, SELECT, etc.)
- [x] Use mo-sql-parsing to parse the schema definition and extract type and primary key/foreign key
- [x] Populate table with entities using LOAD and check for duplicates
- [x] Create an index tree for each table in the index tree dictionary with name as key and primary key rows as value
- [x] Sqlglot defaults to hash join. In the case of a join on two tables with both primary keys, we can create a temp tables by inserting one by one from the index, and then use the temp tables in the join. This is more efficient than hash join. The drawback is temporarily we would have three copies of the same data in memory. Another approach would be to add an ORDER BY
- [x] Create simple structure in main.py to allow user to create tables, load data, and query.
- [x] Find a better way to parse CREATE TABLE statements (currently using a crappy method)
- [ ] Finish functions DROP, DELETE, UPDATE, INSERT in create_database.py
- [ ] Quality test for bugs and edge cases

## THINGS TO CONSIDER:

- [ ] Maintain the size of a table and expectation of sort status to be used in nested loop vs sorting join
- [ ] Strengthen the RDBMS by more error checking and exception handling
