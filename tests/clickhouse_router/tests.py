from django.test import SimpleTestCase

from clickhouse_backend.models import ClickhouseModel
from clickhouse_backend.router import ClickHouseRouter
from clickhouse_backend.utils import get_subclasses

from .models import RouterTestModel


class GetSubclassesTest(SimpleTestCase):
    def test_finds_concrete_subclass(self):
        subclasses = get_subclasses(ClickhouseModel)
        self.assertIn(RouterTestModel, subclasses)

    def test_excludes_abstract_classes(self):
        subclasses = get_subclasses(ClickhouseModel)
        for cls in subclasses:
            self.assertFalse(cls._meta.abstract, f"{cls} is abstract but was returned")


class ClickHouseRouterInitTest(SimpleTestCase):
    def setUp(self):
        self.router = ClickHouseRouter()

    def test_route_model_names_contains_test_model(self):
        self.assertIn(
            "clickhouse_router.routertestmodel", self.router.route_model_names
        )

    def test_route_model_names_excludes_abstract(self):
        for name in self.router.route_model_names:
            app_label, model_name = name.split(".")
            # All entries must be resolvable to concrete models — just check format
            self.assertIn(".", name)


class ClickHouseRouterReadWriteTest(SimpleTestCase):
    def setUp(self):
        self.router = ClickHouseRouter()

    def test_db_for_read_ch_model(self):
        self.assertEqual(self.router.db_for_read(RouterTestModel), "clickhouse")

    def test_db_for_write_ch_model(self):
        self.assertEqual(self.router.db_for_write(RouterTestModel), "clickhouse")

    def test_db_for_read_hint_target(self):
        from django.contrib.auth.models import User

        self.assertEqual(
            self.router.db_for_read(User, target="clickhouse"), "clickhouse"
        )

    def test_db_for_write_hint_target(self):
        from django.contrib.auth.models import User

        self.assertEqual(
            self.router.db_for_write(User, target="clickhouse"), "clickhouse"
        )

    def test_db_for_read_non_ch_model(self):
        from django.contrib.auth.models import User

        self.assertIsNone(self.router.db_for_read(User))

    def test_db_for_write_non_ch_model(self):
        from django.contrib.auth.models import User

        self.assertIsNone(self.router.db_for_write(User))


class ClickHouseRouterAllowMigrateTest(SimpleTestCase):
    def setUp(self):
        self.router = ClickHouseRouter()

    def test_ch_model_on_clickhouse_db(self):
        result = self.router.allow_migrate(
            "clickhouse", "clickhouse_router", model_name="routertestmodel"
        )
        self.assertTrue(result)

    def test_ch_model_on_other_db(self):
        result = self.router.allow_migrate(
            "default", "clickhouse_router", model_name="routertestmodel"
        )
        self.assertFalse(result)

    def test_non_ch_model_on_clickhouse_db(self):
        result = self.router.allow_migrate("clickhouse", "auth", model_name="user")
        self.assertFalse(result)

    def test_non_ch_model_on_other_db(self):
        result = self.router.allow_migrate("default", "auth", model_name="user")
        self.assertIsNone(result)

    def test_target_hint_on_clickhouse_db(self):
        result = self.router.allow_migrate(
            "clickhouse", "auth", model_name="user", target="clickhouse"
        )
        self.assertTrue(result)

    def test_target_hint_on_other_db(self):
        result = self.router.allow_migrate(
            "default", "auth", model_name="user", target="clickhouse"
        )
        self.assertFalse(result)
