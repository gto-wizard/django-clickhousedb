from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from .models import (
    LightweightDistributed,
    LightweightEvent,
    LightweightUpdateEvent,
)


class LightweightDeleteTests(TestCase):
    def test_lightweight_delete_sql(self):
        """On CH >= 23.3, DELETE should use lightweight syntax."""
        LightweightEvent.objects.create(name="a", value=1)
        with CaptureQueriesContext(connection) as ctx:
            LightweightEvent.objects.filter(value=1).delete()
        delete_sql = [q["sql"] for q in ctx if q["sql"].startswith("DELETE")]
        self.assertTrue(delete_sql, "Expected a DELETE FROM query")
        self.assertTrue(delete_sql[0].startswith("DELETE FROM"))
        self.assertNotIn("ALTER TABLE", delete_sql[0])

    def test_mutation_delete_on_opt_out_per_query(self):
        """Per-query opt-out falls back to mutation syntax."""
        LightweightEvent.objects.create(name="a", value=1)
        qs = LightweightEvent.objects.filter(value=1).compile_with(
            lightweight_delete=False
        )
        with CaptureQueriesContext(connection) as ctx:
            qs.delete()
        delete_sql = [q["sql"] for q in ctx if "DELETE" in q["sql"]]
        self.assertTrue(delete_sql)
        self.assertTrue(delete_sql[0].startswith("ALTER TABLE"))

    def test_mutation_delete_on_opt_out_database(self):
        """Database-level opt-out falls back to mutation syntax."""
        LightweightEvent.objects.create(name="a", value=1)
        try:
            connection.settings_dict.setdefault("COMPILER_OPTIONS", {})
            connection.settings_dict["COMPILER_OPTIONS"]["lightweight_delete"] = False
            with CaptureQueriesContext(connection) as ctx:
                LightweightEvent.objects.filter(value=1).delete()
            delete_sql = [q["sql"] for q in ctx if "DELETE" in q["sql"]]
            self.assertTrue(delete_sql)
            self.assertTrue(delete_sql[0].startswith("ALTER TABLE"))
        finally:
            connection.settings_dict["COMPILER_OPTIONS"].pop("lightweight_delete", None)

    def test_lightweight_delete_distributed(self):
        """Distributed model should produce DELETE FROM local ON CLUSTER."""
        LightweightDistributed.objects.bulk_create(
            [LightweightDistributed(name=f"s{i}", score=i) for i in range(3)]
        )
        with CaptureQueriesContext(connection) as ctx:
            LightweightDistributed.objects.filter(score=0).delete()
        delete_sql = [q["sql"] for q in ctx if q["sql"].startswith("DELETE")]
        self.assertTrue(delete_sql, "Expected a DELETE FROM query")
        self.assertIn("ON CLUSTER", delete_sql[0])
        # Should reference the local (underlying) table, not the distributed table
        self.assertNotIn(
            LightweightDistributed._meta.db_table,
            delete_sql[0].split("ON CLUSTER")[0],
        )

    def test_lightweight_delete_with_settings(self):
        """SETTINGS should be appended after WHERE clause."""
        LightweightEvent.objects.create(name="a", value=1)
        qs = LightweightEvent.objects.filter(value=1).settings(mutations_sync=2)
        with CaptureQueriesContext(connection) as ctx:
            qs.delete()
        delete_sql = [q["sql"] for q in ctx if q["sql"].startswith("DELETE")]
        self.assertTrue(delete_sql)
        self.assertIn("SETTINGS", delete_sql[0])

    def test_lightweight_delete_no_where_falls_back_to_mutation(self):
        """Full-table delete without WHERE falls back to mutation syntax."""
        LightweightEvent.objects.create(name="a", value=1)
        with CaptureQueriesContext(connection) as ctx:
            LightweightEvent.objects.all().delete()
        delete_sql = [q["sql"] for q in ctx if "DELETE" in q["sql"]]
        self.assertTrue(delete_sql)
        self.assertTrue(delete_sql[0].startswith("ALTER TABLE"))

    def test_compile_with_does_not_leak_to_settings(self):
        """compile_with flags should not appear in the SETTINGS clause."""
        LightweightEvent.objects.create(name="a", value=1)
        qs = (
            LightweightEvent.objects.filter(value=1)
            .compile_with(lightweight_delete=True)
            .settings(mutations_sync=2)
        )
        with CaptureQueriesContext(connection) as ctx:
            qs.delete()
        delete_sql = [q["sql"] for q in ctx if q["sql"].startswith("DELETE")]
        self.assertTrue(delete_sql)
        # Check SETTINGS clause specifically — table name contains "lightweight_delete"
        settings_part = (
            delete_sql[0].split("SETTINGS")[-1] if "SETTINGS" in delete_sql[0] else ""
        )
        self.assertNotIn("lightweight_delete", settings_part)

    def test_actual_lightweight_delete(self):
        """Integration: insert, delete, verify rows are gone."""
        LightweightEvent.objects.create(name="keep", value=1)
        LightweightEvent.objects.create(name="remove", value=2)
        LightweightEvent.objects.create(name="remove", value=3)
        self.assertEqual(LightweightEvent.objects.count(), 3)
        LightweightEvent.objects.filter(value__gte=2).delete()
        self.assertEqual(LightweightEvent.objects.count(), 1)
        self.assertEqual(LightweightEvent.objects.first().name, "keep")

    def test_db_opt_out_per_query_override_in(self):
        """DB-level opt-out can be overridden to opt-in per-query."""
        LightweightEvent.objects.create(name="a", value=1)
        try:
            connection.settings_dict.setdefault("COMPILER_OPTIONS", {})
            connection.settings_dict["COMPILER_OPTIONS"]["lightweight_delete"] = False
            qs = LightweightEvent.objects.filter(value=1).compile_with(
                lightweight_delete=True
            )
            with CaptureQueriesContext(connection) as ctx:
                qs.delete()
            delete_sql = [q["sql"] for q in ctx if q["sql"].startswith("DELETE")]
            self.assertTrue(delete_sql, "Expected a DELETE FROM query")
            self.assertNotIn("ALTER TABLE", delete_sql[0])
        finally:
            connection.settings_dict["COMPILER_OPTIONS"].pop("lightweight_delete", None)


class LightweightUpdateTests(TestCase):
    """Lightweight UPDATE is opt-in (requires table-level block settings)."""

    def _ch_version_gte_25_7(self):
        return connection.get_database_version() >= (25, 7)

    def test_default_update_uses_mutation(self):
        """Without opt-in, UPDATE should use mutation syntax even on CH >= 25.7."""
        LightweightEvent.objects.create(name="a", value=1)
        with CaptureQueriesContext(connection) as ctx:
            LightweightEvent.objects.filter(value=1).update(name="b")
        update_sql = [q["sql"] for q in ctx if "UPDATE" in q["sql"]]
        self.assertTrue(update_sql)
        self.assertTrue(update_sql[0].startswith("ALTER TABLE"))

    def test_lightweight_update_per_query_opt_in(self):
        """Per-query opt-in produces lightweight UPDATE syntax."""
        if not self._ch_version_gte_25_7():
            self.skipTest("Requires ClickHouse >= 25.7")
        LightweightUpdateEvent.objects.create(name="a", value=1)
        qs = LightweightUpdateEvent.objects.filter(value=1).compile_with(
            lightweight_update=True
        )
        with CaptureQueriesContext(connection) as ctx:
            qs.update(name="b")
        update_sql = [q["sql"] for q in ctx if q["sql"].startswith("UPDATE")]
        self.assertTrue(update_sql, "Expected an UPDATE query")
        self.assertNotIn("ALTER TABLE", update_sql[0])
        self.assertIn("SET", update_sql[0])

    def test_lightweight_update_database_opt_in(self):
        """Database-level opt-in produces lightweight UPDATE syntax."""
        if not self._ch_version_gte_25_7():
            self.skipTest("Requires ClickHouse >= 25.7")
        LightweightUpdateEvent.objects.create(name="a", value=1)
        try:
            connection.settings_dict.setdefault("COMPILER_OPTIONS", {})
            connection.settings_dict["COMPILER_OPTIONS"]["lightweight_update"] = True
            with CaptureQueriesContext(connection) as ctx:
                LightweightUpdateEvent.objects.filter(value=1).update(name="b")
            update_sql = [q["sql"] for q in ctx if q["sql"].startswith("UPDATE")]
            self.assertTrue(update_sql, "Expected an UPDATE query")
            self.assertNotIn("ALTER TABLE", update_sql[0])
        finally:
            connection.settings_dict["COMPILER_OPTIONS"].pop("lightweight_update", None)

    def test_database_opt_in_per_query_opt_out(self):
        """Database-level opt-in can be overridden per-query."""
        if not self._ch_version_gte_25_7():
            self.skipTest("Requires ClickHouse >= 25.7")
        LightweightUpdateEvent.objects.create(name="a", value=1)
        try:
            connection.settings_dict.setdefault("COMPILER_OPTIONS", {})
            connection.settings_dict["COMPILER_OPTIONS"]["lightweight_update"] = True
            qs = LightweightUpdateEvent.objects.filter(value=1).compile_with(
                lightweight_update=False
            )
            with CaptureQueriesContext(connection) as ctx:
                qs.update(name="b")
            update_sql = [q["sql"] for q in ctx if "UPDATE" in q["sql"]]
            self.assertTrue(update_sql)
            self.assertTrue(update_sql[0].startswith("ALTER TABLE"))
        finally:
            connection.settings_dict["COMPILER_OPTIONS"].pop("lightweight_update", None)

    def test_lightweight_update_with_settings(self):
        """SETTINGS should be appended after WHERE clause."""
        if not self._ch_version_gte_25_7():
            self.skipTest("Requires ClickHouse >= 25.7")
        LightweightUpdateEvent.objects.create(name="a", value=1)
        qs = (
            LightweightUpdateEvent.objects.filter(value=1)
            .compile_with(lightweight_update=True)
            .settings(mutations_sync=2)
        )
        with CaptureQueriesContext(connection) as ctx:
            qs.update(name="b")
        update_sql = [q["sql"] for q in ctx if q["sql"].startswith("UPDATE")]
        self.assertTrue(update_sql)
        self.assertIn("SETTINGS", update_sql[0])

    def test_compile_with_does_not_leak_to_settings(self):
        """compile_with flags should not appear in the SETTINGS clause."""
        if not self._ch_version_gte_25_7():
            self.skipTest("Requires ClickHouse >= 25.7")
        LightweightUpdateEvent.objects.create(name="a", value=1)
        qs = (
            LightweightUpdateEvent.objects.filter(value=1)
            .compile_with(lightweight_update=True)
            .settings(mutations_sync=2)
        )
        with CaptureQueriesContext(connection) as ctx:
            qs.update(name="b")
        update_sql = [q["sql"] for q in ctx if q["sql"].startswith("UPDATE")]
        self.assertTrue(update_sql)
        settings_part = (
            update_sql[0].split("SETTINGS")[-1] if "SETTINGS" in update_sql[0] else ""
        )
        self.assertNotIn("lightweight_update", settings_part)

    def test_actual_lightweight_update(self):
        """Integration: insert, update with opt-in, verify values changed."""
        if not self._ch_version_gte_25_7():
            self.skipTest("Requires ClickHouse >= 25.7")
        LightweightUpdateEvent.objects.create(name="original", value=1)
        LightweightUpdateEvent.objects.create(name="original", value=2)
        LightweightUpdateEvent.objects.filter(value=1).compile_with(
            lightweight_update=True
        ).update(name="updated")
        self.assertEqual(
            LightweightUpdateEvent.objects.filter(name="updated").count(), 1
        )
        self.assertEqual(
            LightweightUpdateEvent.objects.filter(name="original").count(), 1
        )

    def test_actual_mutation_update(self):
        """Integration: mutation update works (default path)."""
        LightweightEvent.objects.create(name="original", value=10)
        LightweightEvent.objects.filter(value=10).update(name="mutated")
        self.assertEqual(LightweightEvent.objects.filter(name="mutated").count(), 1)


class FeatureTests(TestCase):
    def test_supports_lightweight_delete(self):
        self.assertTrue(connection.features.supports_lightweight_delete)

    def test_supports_lightweight_update(self):
        version = connection.get_database_version()
        if version >= (25, 7):
            self.assertTrue(connection.features.supports_lightweight_update)
        else:
            self.assertFalse(connection.features.supports_lightweight_update)
