from django.test import TestCase

from clickhouse_backend.utils.sql import (
    compile_queryset_to_sql,
    execute_raw_sql,
    extract_where_clause_from_qs,
)

from ..models import Person


class ExecuteRawSqlTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        Person.objects.create(first_name="Alice", last_name="Smith")
        Person.objects.create(first_name="Bob", last_name="Jones")

    def test_returns_dicts(self):
        results = execute_raw_sql(
            "SELECT first_name, last_name FROM backends_person ORDER BY first_name"
        )
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["first_name"], "Alice")
        self.assertEqual(results[0]["last_name"], "Smith")
        self.assertEqual(results[1]["first_name"], "Bob")

    def test_with_params(self):
        results = execute_raw_sql(
            "SELECT first_name FROM backends_person WHERE last_name = %s",
            ["Jones"],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["first_name"], "Bob")

    def test_empty_result(self):
        results = execute_raw_sql(
            "SELECT first_name FROM backends_person WHERE first_name = %s",
            ["Nobody"],
        )
        self.assertEqual(results, [])

    def test_custom_using(self):
        results = execute_raw_sql(
            "SELECT first_name FROM backends_person ORDER BY first_name",
            using="default",
        )
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["first_name"], "Alice")


class CompileQuerysetToSqlTests(TestCase):
    def test_compile_queryset_to_sql(self):
        qs = Person.objects.filter(first_name="Alice")
        sql, params = compile_queryset_to_sql(qs)
        self.assertIn("backends_person", sql)
        self.assertIn("WHERE", sql)

    def test_params(self):
        qs = Person.objects.filter(first_name="Alice", last_name="Smith")
        sql, params = compile_queryset_to_sql(qs)
        self.assertIn("Alice", params)
        self.assertIn("Smith", params)


class ExtractWhereClauseTests(TestCase):
    def test_extract_where_clause(self):
        qs = Person.objects.filter(first_name="Alice")
        where_sql, where_params = extract_where_clause_from_qs(qs)
        self.assertIn("first_name", where_sql)
        self.assertEqual(where_params, ["Alice"])

    def test_empty(self):
        qs = Person.objects.all()
        where_sql, where_params = extract_where_clause_from_qs(qs)
        self.assertEqual(where_sql, "")
        self.assertEqual(where_params, [])

    def test_multiple_conditions(self):
        qs = Person.objects.filter(first_name="Alice").filter(last_name="Smith")
        where_sql, where_params = extract_where_clause_from_qs(qs)
        self.assertIn("AND", where_sql)
        self.assertIn("Alice", where_params)
        self.assertIn("Smith", where_params)
