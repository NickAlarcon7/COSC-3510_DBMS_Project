from __future__ import annotations

import logging
import typing as t

from mo_sql_parsing import parse

from sqlglot.errors import ExecuteError
from sqlglot.executor.table import Table, ensure_tables
from sqlglot.helper import dict_depth
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan
from sqlglot.schema import ensure_schema, flatten_schema, nested_get, nested_set

from custom_python_executor import MergeJoinPythonExecutor, DefaultPythonExecutor

logger = logging.getLogger("sqlglot")

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.executor.table import Tables
    from sqlglot.expressions import Expression
    from sqlglot.schema import Schema


PYTHON_TYPE_TO_SQLGLOT = {
    "dict": "MAP",
}


def execute_query(query, database):
    parsed_query = parse(query)

    # identify available indexes
    tables = identify_available_indexes(parsed_query, database)

    # if query joins two tables and orders by one of the joining condiiton, then use merge join
    join_algorithm = identify_join_algorithm(parsed_query)

    result = sqlglot_execute(query, tables=tables, join_algorithm=join_algorithm)

    return result


def identify_join_algorithm(parsed_query):
    join_algorithm = "default"

    if "orderby" not in parsed_query or not isinstance(parsed_query.get("from"), list):
        return join_algorithm

    from_tables = parsed_query["from"]
    orderby_clause = parsed_query["orderby"]["value"]

    # traverse from_tables to find dictionary entries.
    for table in from_tables:
        # In a dictionary entry, find the dictionary value at the "on" key
        if not isinstance(table, dict) or "on" not in table:
            continue
        on_clause = table["on"]

        # don't use merge join if join type is left or right
        if "left join" in table or "right join" in table:
            return join_algorithm

        if not isinstance(on_clause, dict) or "eq" not in on_clause:
            continue
        eq_clause = on_clause["eq"]

        if orderby_clause in eq_clause:
            join_algorithm = "merge"
            return join_algorithm

    # inside where clause, find the array value at the "eq" key.
    if "where" in parsed_query:
        where_clause = parsed_query["where"]
        if not isinstance(where_clause, dict) or "eq" not in where_clause:
            return join_algorithm
        eq_clause = where_clause["eq"]

        # If orderby_clause is in the array, then set join_algorithm to "merge"
        if orderby_clause in eq_clause:
            join_algorithm = "merge"
            return join_algorithm

    return join_algorithm


def identify_available_indexes(parsed_query, database):
    tables = database.tables

    # extract table name from query
    from_table = parsed_query["from"]
    # extract table name from query and flatten it if it is a dictionary
    if isinstance(from_table, dict):
        table_name = from_table["value"]
    else:
        table_name = from_table

    # extract where clause from query if it exists
    where_clause = parsed_query.get("where")
    temp_table = {}

    # Use indexing structure to retrieve row for single table queries with where clause
    # e.x. SELECT * FROM sushi WHERE id = 1
    # e.x. SELECT * FROM sushi WHERE id = 1 OR id = 2
    # if query deals with a single table, extract where clause from query if it exists
    if (
        not isinstance(from_table, list)
        and where_clause is not None
        and table_name in database.indexing_structures
    ):
        selected_table = tables[table_name]
        selected_schema = database.table_schemas[table_name]
        selected_indexing_structure = database.indexing_structures[table_name]
        if "eq" in where_clause:
            temp_table = fetch_index(
                where_clause["eq"],
                selected_indexing_structure,
                selected_schema,
                table_name,
            )
        elif "or" in where_clause:
            conjunction_clause = where_clause["or"]
            temp_table = parse_conjunction_for_indexing(
                conjunction_clause,
                selected_indexing_structure,
                selected_schema,
                table_name,
            )
            # if only one side of "or" clause is in index, then set temp_table to selected_table
            if temp_table and len(temp_table[table_name]) == 1:
                temp_table[table_name] = selected_table

        elif "and" in where_clause:
            conjunction_clause = where_clause["and"]
            temp_table = parse_conjunction_for_indexing(
                conjunction_clause,
                selected_indexing_structure,
                selected_schema,
                table_name,
            )
        else:
            temp_table = {
                table_name: selected_table,
            }

    # if temp_table exists, then set tables to temp_table
    if temp_table:
        tables = temp_table
    return tables


def sqlglot_execute(
    sql: str | Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    read: DialectType = None,
    tables: t.Optional[t.Dict] = None,
    join_algorithm: str = "default",
) -> Table:
    """
    Run a sql query against data.

    Args:
        sql: a sql statement.
        schema: database schema.
            This can either be an instance of `Schema` or a mapping in one of the following forms:
            1. {table: {col: type}}
            2. {db: {table: {col: type}}}
            3. {catalog: {db: {table: {col: type}}}}
        read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql").
        tables: additional tables to register.

    Returns:
        Simple columnar data structure.
    """
    tables_ = ensure_tables(tables, dialect=read)

    if not schema:
        schema = {}
        flattened_tables = flatten_schema(
            tables_.mapping, depth=dict_depth(tables_.mapping)
        )

        for keys in flattened_tables:
            table = nested_get(tables_.mapping, *zip(keys, keys))
            assert table is not None

            for column in table.columns:
                py_type = type(table[0][column]).__name__
                nested_set(
                    schema,
                    [*keys, column],
                    PYTHON_TYPE_TO_SQLGLOT.get(py_type) or py_type,
                )

    schema = ensure_schema(schema, dialect=read)

    if (
        tables_.supported_table_args
        and tables_.supported_table_args != schema.supported_table_args
    ):
        raise ExecuteError("Tables must support the same table args as schema")

    expression = optimize(sql, schema, leave_tables_isolated=True, dialect=read)

    # logger.debug("Optimization finished: %f", time.time() - now)
    # logger.debug("Optimized SQL: %s", expression.sql(pretty=True))

    plan = Plan(expression)

    logger.debug("Logical Plan: %s", plan)

    # now = time.time()
    if join_algorithm == "merge":
        result = MergeJoinPythonExecutor(tables=tables_).execute(plan)
    else:
        result = DefaultPythonExecutor(tables=tables_).execute(plan)

    print()

    return result


def parse_conjunction_for_indexing(
    conjunction_clause, selected_indexing_structure, selected_schema, table_name
):
    temp_table = {}
    # conjunction_clause is a list with two elements; remove non-eq elements
    conjunction_clause = [eq for eq in conjunction_clause if "eq" in eq]
    # if there is one eq element in the list, then call fetch_index with that element
    if len(conjunction_clause) == 1:
        temp_table = fetch_index(
            conjunction_clause[0]["eq"],
            selected_indexing_structure,
            selected_schema,
            table_name,
        )
    # if there are two eq elements in the list, then call fetch_index on both elements and combine the results
    elif len(conjunction_clause) == 2:
        temp_table1 = fetch_index(
            conjunction_clause[0]["eq"],
            selected_indexing_structure,
            selected_schema,
            table_name,
        )
        temp_table2 = fetch_index(
            conjunction_clause[1]["eq"],
            selected_indexing_structure,
            selected_schema,
            table_name,
        )

        if temp_table1 and temp_table2:
            temp_table[table_name] = temp_table1[table_name] + temp_table2[table_name]
        elif temp_table1:
            temp_table = temp_table1
        elif temp_table2:
            temp_table = temp_table2

    return temp_table


def fetch_index(
    equality_condition, selected_indexing_structure, selected_schema, table_name
):
    tables = {}

    # extract column name from equality condition array
    column_name = equality_condition[0]
    # extract matching value from equality condition array
    if isinstance(equality_condition[1], dict):
        # if equality condition does not contain "literal" key, then return empty tables
        if "literal" not in equality_condition[1]:
            return tables
        equality_condition[1] = equality_condition[1]["literal"]
    matching_value = equality_condition[1]

    # find out if table has an index, column is a primary key, and matching value is in index
    if "primary_key" in selected_schema[
        column_name
    ] and selected_indexing_structure.has_key(matching_value):
        # if matching value is in index, then use index to retrieve row
        row = selected_indexing_structure.get(matching_value)
        # create a temp table with the row
        tables[table_name] = [row]
        return tables

    return tables
