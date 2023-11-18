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

## Notes:

- > A separate schemas table is used to store the schema of each table. We store data type, nullable, primary key, foreign key, and foreign reference in the schema table. When inserting data, we check the schema table to convert the data type to match the schema. We also check for single attribute primary key to insert into index strucuture.
- > When a table is created with single attribute primary key, indexing happens under the hood.
  > This app does not support CREATE INDEX statements
- > Since sqlglot does not support an alternate access path with index, this app detects when
  > using the indexing structure is more efficient and creates a temp table with the tuple(s) from the index. Then, instead of the original table, the temp table is used in the query. We currently detect equality condition in WHERE clause and support multiple equality conditions connected with OR, AND. If one side of OR is indexed and the other side is not, the temp table is the original table because we cannot create a temp table with only the indexed tuple.

## TODO:

- [x] When CREATE TABLE is called, create a table in the tables dictionary with the table name as the key and an empty dictionary as the value; create a schema in the schemas dictionary with the table name as the key and the schema definition dictionary as the value
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
