# COSC-3510_DBMS_Project

Repo for DBMS Project

Architecture:

- A SQL parser: use sqlparse to identify CREATE TABLE statements
- An indexing structure: a binary tree with primary key as key and a tuple as value
- A query optimizer: use sqlglot to optimize query
- An execution engine: wrap sqlglot executor with index support

Storage Architecture:
tables - dictionary (
key: table name - string, value: tuples - dictionary
)
schemas - dictionary (
key: table name - string, value: schema definition - dictionary
)

Notes:

- A separate schemas table is used to store the schema of each table,
  and schema is a parameter into executor
- When a table is created with single attribute primary key, indexing happens under the hood.
  This app does not support CREATE INDEX statements
- Since sqlglot does not support an alternate access path with index, this app detects when
  using the indexing structure is more efficient and creats a temp table with the tuple from the index. Then, instead of the original table, the temp table is used in the query.

TODO:

- When CREATE TABLE is called, create a table in the tables dictionary with the table name as the key and an empty dictionary as the value;
  create a schema in the schemas dictionary with the table name as the key and the schema definition dictionary as the value
- how to parse CREATE TABLE? - Use sqlglot.parse_one().key to figure out the leading sql command (i.e. CREATE TABLE, SELECT, etc.)
- Use mo-sql-parsing to parse the schema definition and extract type and primary key/foreign key
- Populate table with entities using LOAD and check for duplicates
- Prove sqlglot optimizes nested loop vs sort-merge join. If sqlglot does not do nested loop, then we need to implement it ourselves.

THINGS TO CONSIDER:

- Maintain the size of a table and expectation of sort status to be used in nested loop vs sorting join
- Use a B+ tree, instead of an AVL tree, for indexing
