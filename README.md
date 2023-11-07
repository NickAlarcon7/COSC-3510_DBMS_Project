# COSC-3510_DBMS_Project

**Repo for DBMS Project**

## Architecture:

- A SQL parser: use mo-sql to identify CREATE TABLE statements
- An indexing structure: a binary tree with primary key as key and a tuple as value (row of data)
- A query optimizer: use sqlglot to optimize query **Need to work on this**
- An execution engine: wrap sqlglot executor with index support

Storage Architecture:
tables - dictionary (
key: table name - string, value: tuples - dictionary
return: Returns a valid table structure for use in sqlglot executor
)
schemas - dictionary (
key: table name - string, value: schema definition - dictionary
return: Returns schema (data type, primary key, constraint)
)
index-tree - dictionary (
key: table name - string, value: index tree - AVL tree
return: Returns a table with a single row 
) 

## Notes:

- A separate schemas table is used to store the schema of each table, and schema is a parameter into executor
- When a table is created with single attribute primary key, indexing happens under the hood.
  This app does not support CREATE INDEX statements
- Since sqlglot does not support an alternate access path with index, this app detects when
  using the indexing structure is more efficient and creates a temp table with the tuple from the index. Then, instead of the original table, the temp table is used in the query.

## TODO:

- [] When CREATE TABLE is called, create a table in the tables dictionary with the table name as the key and an empty dictionary as the value; create a schema in the schemas dictionary with the table name as the key and the schema definition dictionary as the value
- [] How to parse CREATE TABLE? - Use mo.sql to figure out the leading sql command (i.e. CREATE TABLE, SELECT, etc.)
- [] Use mo-sql-parsing to parse the schema definition and extract type and primary key/foreign key
- [] Populate table with entities using LOAD and check for duplicates
- [] Create an index tree for each table in the index tree dictionary with name as key and primary key rows as value (when called it returns a table with a single row)
- [] Sqlglot defaults to hash join. In the case of a join on two tables with both primary keys, we can create a temp tables by inserting one by one from the index, and then use the temp tables in the join. This is more efficient than hash join. The drawback is temporarily we would have three copies of the same data in memory. Another approach would be to add an ORDER BY
- [] Create simple structure in main.py to allow user to create tables, load data, and query.

## THINGS TO CONSIDER:
- [] Maintain the size of a table and expectation of sort status to be used in nested loop vs sorting join
- [] Use a B+ tree, instead of an AVL tree, for indexing
