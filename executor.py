from __future__ import annotations

import logging
import time
import typing as t

from mo_sql_parsing import parse

from sqlglot import maybe_parse
from sqlglot.errors import ExecuteError
from sqlglot.executor.python import PythonExecutor
from sqlglot.executor.table import Table, ensure_tables
from sqlglot.helper import dict_depth
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan
from sqlglot.schema import ensure_schema, flatten_schema, nested_get, nested_set


logger = logging.getLogger("sqlglot")

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.executor.table import Tables
    from sqlglot.expressions import Expression
    from sqlglot.schema import Schema


PYTHON_TYPE_TO_SQLGLOT = {
    "dict": "MAP",
}


class OptimizedPythonExecutor(PythonExecutor):
    def join(self, step, context):
        source = step.name

        source_table = context.tables[source]
        source_context = self.context({source: source_table})
        column_ranges = {source: range(0, len(source_table.columns))}

        for name, join in step.joins.items():
            table = context.tables[name]
            start = max(r.stop for r in column_ranges.values())
            column_ranges[name] = range(start, len(table.columns) + start)
            join_context = self.context({name: table})

            if join.get("source_key"):
                table = self.hash_join(join, source_context, join_context)
            else:
                table = self.nested_loop_join(join, source_context, join_context)

            source_context = self.context(
                {
                    name: Table(table.columns, table.rows, column_range)
                    for name, column_range in column_ranges.items()
                }
            )
            condition = self.generate(join["condition"])
            if condition:
                source_context.filter(condition)

        if not step.condition and not step.projections:
            return source_context

        sink = self._project_and_filter(
            source_context,
            step,
            (reader for reader, _ in iter(source_context)),
        )

        if step.projections:
            return self.context({step.name: sink})
        else:
            return self.context(
                {
                    name: Table(table.columns, sink.rows, table.column_range)
                    for name, table in source_context.tables.items()
                }
            )

    def merge_join(self, _join, source_context, join_context):
        table = Table(source_context.columns + join_context.columns)

        source_key = self.generate_tuple(_join["source_key"])
        join_key = self.generate_tuple(_join["join_key"])

        source_context.sort(source_key)
        join_context.sort(join_key)

        source_iterator = iter(source_context)
        join_iterator = iter(join_context)

        source_reader, source_ctx = next(source_iterator, (None, None))
        join_reader, join_ctx = next(join_iterator, (None, None))

        while source_reader and join_reader:
            source_key_value = source_ctx.eval_tuple(source_key)
            join_key_value = join_ctx.eval_tuple(join_key)

            if source_key_value == join_key_value:
                # Create lists to store matching rows from both contexts
                source_rows = []
                join_rows = []

                while (
                    source_reader
                    and source_ctx.eval_tuple(source_key) == source_key_value
                ):
                    source_rows.append(source_reader.row)
                    source_reader, source_ctx = next(source_iterator, (None, None))

                while join_reader and join_ctx.eval_tuple(join_key) == join_key_value:
                    join_rows.append(join_reader.row)
                    join_reader, join_ctx = next(join_iterator, (None, None))

                # Create a Cartesian product of matching rows
                for source_row in source_rows:
                    for join_row in join_rows:
                        table.append(source_row + join_row)
            elif source_key_value < join_key_value:
                source_reader, source_ctx = next(source_iterator, (None, None))
            else:
                join_reader, join_ctx = next(join_iterator, (None, None))

        return table


def execute_query(query, database):
    tables = database.tables

    parsed_query = parse(query)
    # extract table name from query
    from_tables = parsed_query["from"]
    # extract where clause from query if it exists
    if "where" in parsed_query:
        where_clause = parsed_query["where"]

    # if query deals with a single table, extract where clause from query if it exists
    if (
        type(from_tables) is not list
        and where_clause is not None
        and "eq" in where_clause
    ):
        # extract table name from query and flatten it if it is a dictionary
        if type(from_tables) is dict:
            table_name = from_tables["value"]
        else:
            table_name = from_tables

        # extract equality condition array from where clause if it exists
        equality_condition = where_clause["eq"]
        # extract column name from equality condition array
        column_name = equality_condition[0]
        # extract matching value from equality condition array
        if type(equality_condition[1]) is dict:
            equality_condition[1] = equality_condition[1]["literal"]
        matching_value = equality_condition[1]
        # find out if table has an index, column is a primary key, and matching value is in index
        if (
            table_name in database.indexing_structures
            and "primary_key" in database.table_schemas[table_name][column_name]
            and database.indexing_structures[table_name].has_key(matching_value)
        ):
            # if matching value is in index, then use index to retrieve row
            row = database.indexing_structures[table_name].get(matching_value)
            # create a temp table with the row
            tables = {}
            tables[table_name] = [row]

    return sqlglot_execute(query, tables=tables)


def sqlglot_execute(
    sql: str | Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    read: DialectType = None,
    tables: t.Optional[t.Dict] = None,
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

    now = time.time()
    expression = optimize(sql, schema, leave_tables_isolated=True, dialect=read)

    logger.debug("Optimization finished: %f", time.time() - now)
    logger.debug("Optimized SQL: %s", expression.sql(pretty=True))

    plan = Plan(expression)

    logger.debug("Logical Plan: %s", plan)

    now = time.time()
    result = OptimizedPythonExecutor(tables=tables_).execute(plan)

    print()
    print(f"Query finished: {time.time() - now:.5f}s")
    # print("Query finished: %fs", time.time() - now)

    return result
