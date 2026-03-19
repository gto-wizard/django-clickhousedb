from django.db import connections


DEFAULT_DB_ALIAS = "default"


def execute_raw_sql(query, params=None, using=DEFAULT_DB_ALIAS):
    """Execute raw SQL and return results as a list of dicts."""
    connection = connections[using]
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def compile_queryset_to_sql(queryset):
    """Extract the compiled SQL and params from a queryset without executing it."""
    connection = connections[queryset.db]
    compiler = queryset.query.get_compiler(connection=connection)
    return compiler.as_sql()


def extract_where_clause_from_qs(queryset):
    """Extract only the WHERE clause SQL and params from a queryset."""
    connection = connections[queryset.db]
    compiler = queryset.query.get_compiler(connection=connection)
    where_node = queryset.query.where

    if where_node:
        where_sql, where_params = compiler.compile(where_node)
        return where_sql, list(where_params) if where_params else []
    return "", []
