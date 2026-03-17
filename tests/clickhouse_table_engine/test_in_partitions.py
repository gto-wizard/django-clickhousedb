from datetime import datetime

from django.test import TestCase

from . import models


class InPartitionsDeleteTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.e1 = models.Event.objects.create(
            ip="1.2.3.4", port=80, protocol="tcp",
            content="jan", timestamp=datetime(2024, 1, 15),
        )
        cls.e2 = models.Event.objects.create(
            ip="5.6.7.8", port=443, protocol="tcp",
            content="feb", timestamp=datetime(2024, 2, 15),
        )
        cls.e3 = models.Event.objects.create(
            ip="9.10.11.12", port=8080, protocol="udp",
            content="jan2", timestamp=datetime(2024, 1, 20),
        )

    def test_delete_single_partition(self):
        """Delete scoped to one partition only affects rows in that partition."""
        models.Event.objects.filter(protocol="tcp").in_partitions(
            "20240115"
        ).delete()
        remaining_ids = set(models.Event.objects.values_list("id", flat=True))
        # e1 (jan, tcp) should be deleted
        self.assertNotIn(self.e1.id, remaining_ids)
        # e2 (feb, tcp) untouched — different partition
        self.assertIn(self.e2.id, remaining_ids)
        # e3 (jan, udp) untouched — different protocol
        self.assertIn(self.e3.id, remaining_ids)

    def test_delete_multiple_partitions(self):
        """Delete across multiple explicit partitions."""
        models.Event.objects.filter(protocol="tcp").in_partitions(
            "20240115", "20240215"
        ).delete()
        remaining_ids = set(models.Event.objects.values_list("id", flat=True))
        self.assertNotIn(self.e1.id, remaining_ids)
        self.assertNotIn(self.e2.id, remaining_ids)
        self.assertIn(self.e3.id, remaining_ids)

    def test_delete_nonexistent_partition(self):
        """Delete in a partition with no matching rows deletes nothing."""
        models.Event.objects.filter(protocol="tcp").in_partitions(
            "20230101"
        ).delete()
        self.assertEqual(models.Event.objects.count(), 3)


class InPartitionsUpdateTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.e1 = models.Event.objects.create(
            ip="1.2.3.4", port=80, protocol="tcp",
            content="jan", timestamp=datetime(2024, 1, 15),
        )
        cls.e2 = models.Event.objects.create(
            ip="5.6.7.8", port=443, protocol="tcp",
            content="feb", timestamp=datetime(2024, 2, 15),
        )

    def test_update_single_partition(self):
        """Update scoped to one partition only affects rows in that partition."""
        models.Event.objects.filter(protocol="tcp").in_partitions(
            "20240115"
        ).update(port=9999)
        self.e1.refresh_from_db()
        self.e2.refresh_from_db()
        self.assertEqual(self.e1.port, 9999)
        # e2 is in a different partition — should be untouched
        self.assertEqual(self.e2.port, 443)

    def test_update_multiple_partitions(self):
        """Update across multiple explicit partitions."""
        models.Event.objects.filter(protocol="tcp").in_partitions(
            "20240115", "20240215"
        ).update(port=1111)
        self.e1.refresh_from_db()
        self.e2.refresh_from_db()
        self.assertEqual(self.e1.port, 1111)
        self.assertEqual(self.e2.port, 1111)


class InPartitionsSQLTests(TestCase):
    """Test generated SQL without hitting the database."""

    def test_single_partition_delete_sql(self):
        qs = models.Event.objects.filter(protocol="tcp").in_partitions("20240115")
        self.assertEqual(qs.query.partition_ids, ("20240115",))
        self.assertFalse(qs.query.partition_id_mode)

    def test_partition_id_mode(self):
        qs = models.Event.objects.in_partitions("abc123", partition_id=True)
        self.assertEqual(qs.query.partition_ids, ("abc123",))
        self.assertTrue(qs.query.partition_id_mode)

    def test_empty_partition_ids_raises(self):
        with self.assertRaises(ValueError):
            models.Event.objects.in_partitions()

    def test_chaining_with_settings(self):
        qs = (
            models.Event.objects.filter(protocol="tcp")
            .settings(mutations_sync=2)
            .in_partitions("20240115")
        )
        self.assertEqual(qs.query.partition_ids, ("20240115",))
        self.assertEqual(qs.query.setting_info, {"mutations_sync": 2})

    def test_in_partitions_via_manager(self):
        qs = models.Event.objects.in_partitions("20240115")
        self.assertEqual(qs.query.partition_ids, ("20240115",))


class InPartitionsCloneTests(TestCase):
    """Verify partition_ids survive query cloning."""

    def test_queryset_clone(self):
        qs1 = models.Event.objects.in_partitions("20240115")
        qs2 = qs1.filter(protocol="tcp")
        self.assertEqual(qs2.query.partition_ids, ("20240115",))
        self.assertFalse(qs2.query.partition_id_mode)

    def test_queryset_clone_isolation(self):
        """Cloned querysets don't share partition state."""
        qs1 = models.Event.objects.in_partitions("20240115")
        qs2 = qs1.in_partitions("20240215")
        self.assertEqual(qs1.query.partition_ids, ("20240115",))
        self.assertEqual(qs2.query.partition_ids, ("20240215",))
