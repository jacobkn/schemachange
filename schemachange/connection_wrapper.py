from typing import Callable

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


class SnowflakeParallelConnection(SnowflakeConnection):
    def async_execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        cursor_class: SnowflakeCursor = SnowflakeCursor,
        **kwargs,
    ) -> Iterable[Callable[[], SnowflakeCursor]]:
        """Executes a SQL text including multiple statements. This is a non-standard convenience method."""
        kwargs["_no_results"] = True
        stream = StringIO(sql_text)
        stream_generator = self.execute_stream(
            stream, remove_comments=remove_comments, cursor_class=cursor_class, **kwargs
        )
        ret = list(stream_generator)
        return ret

    def async_execute_stream(
        self,
        stream: StringIO,
        remove_comments: bool = False,
        cursor_class: SnowflakeCursor = SnowflakeCursor,
        **kwargs,
    ) -> Generator[SnowflakeCursor, None, None]:
        """Executes a stream of SQL statements. This is a non-standard convenient method."""
        split_statements_list = split_statements(
            stream, remove_comments=remove_comments
        )
        kwargs["_no_results"] = True
        # Note: split_statements_list is a list of tuples of sql statements and whether they are put/get
        non_empty_statements = [e for e in split_statements_list if e[0]]
        for sql, is_put_or_get in non_empty_statements:
            yield lambda: self.cursor(cursor_class=cursor_class).execute(sql, _is_put_get=is_put_or_get, **kwargs)

    @staticmethod
    def execute_async_lambda_cursor(lambda_cursor: Callable[[], SnowflakeCursor]) -> str:
        return lambda_cursor().sfqid
