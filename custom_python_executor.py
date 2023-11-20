from sqlglot.executor.python import PythonExecutor
from sqlglot.executor.table import Table, ensure_tables


class MergeJoinPythonExecutor(PythonExecutor):
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

            table = self.merge_join(join, source_context, join_context)

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


class DefaultPythonExecutor(PythonExecutor):
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

            source_size = len(list(source_context))
            join_size = len(list(join_context))

            smaller_size = min(source_size, join_size)
            larger_size = max(source_size, join_size)

            # if the size of one table is less than 100
            # and the size of the other table is less than 10 times the size of the smaller table,
            # then use nested loop join
            if (
                smaller_size < 100
                and larger_size < 10 * smaller_size
                and join.get("side") != "LEFT"
                and join.get("side") != "RIGHT"
            ):
                table = self.nested_loop_join(
                    join, source_context, join_context, source_size, join_size
                )
            # default to hash join
            else:
                table = self.hash_join(join, source_context, join_context)

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

    def nested_loop_join(
        self, _join, source_context, join_context, source_size, join_size
    ):
        table = Table(source_context.columns + join_context.columns)
        source_key = self.generate_tuple(_join["source_key"])
        join_key = self.generate_tuple(_join["join_key"])

        # if source table is smaller than join table, then use source table as outer table
        outer_context, inner_context, outer_key, inner_key = (
            (source_context, join_context, source_key, join_key)
            if source_size < join_size
            else (join_context, source_context, join_key, source_key)
        )

        # The smaller number of relevant tuples should be the outer relation
        for outer_reader, outer_ctx in outer_context:
            for inner_reader, inner_ctx in inner_context:
                if outer_ctx.eval_tuple(outer_key) == inner_ctx.eval_tuple(inner_key):
                    table.append(
                        (outer_reader.row + inner_reader.row)
                        if source_size < join_size
                        else (inner_reader.row + outer_reader.row)
                    )

        return table
