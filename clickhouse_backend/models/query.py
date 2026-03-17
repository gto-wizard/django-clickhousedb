from django.db.models import F, Q, query
from django.db.models.functions import Trunc

from clickhouse_backend.models import fields, sql


class QuerySet(query.QuerySet):
    def explain(self, *, format=None, type=None, **settings):
        """
        Runs an EXPLAIN on the SQL query this QuerySet would perform, and
        returns the results.
        https://clickhouse.com/docs/en/sql-reference/statements/explain/
        """
        return self.query.explain(using=self.db, format=format, type=type, **settings)

    def settings(self, **kwargs):
        clone = self._chain()
        if isinstance(clone.query, sql.Query):
            clone.query.setting_info.update(kwargs)
        return clone

    def prewhere(self, *args, **kwargs):
        """
        Return a new QuerySet instance with the args ANDed to the existing
        prewhere set.
        """
        self._not_support_combined_queries("prewhere")
        if (args or kwargs) and self.query.is_sliced:
            raise TypeError("Cannot prewhere a query once a slice has been taken.")
        clone = self._chain()
        clone._query.add_prewhere(Q(*args, **kwargs))
        return clone

    def in_partitions(self, *partition_ids, partition_id=False):
        """Scope subsequent mutations to specific ClickHouse partitions.

        Args:
            *partition_ids: One or more partition expression values
                (e.g. '202401') or internal partition IDs from system.parts.
            partition_id: If True, values are treated as internal partition IDs
                and ``IN PARTITION ID`` syntax is used instead of
                ``IN PARTITION``.

        Returns:
            A new QuerySet scoped to the given partitions.

        Example::

            # Delete only in January 2024 partition
            Event.objects.filter(ip='1.2.3.4').in_partitions('20240101').delete()

            # Update across two partitions
            Event.objects.in_partitions('20240101', '20240201').update(port=80)

            # Using internal partition IDs
            Event.objects.in_partitions('abc123', partition_id=True).delete()
        """
        if not partition_ids:
            raise ValueError("At least one partition ID is required.")
        clone = self._chain()
        if isinstance(clone.query, sql.Query):
            clone.query.partition_ids = tuple(partition_ids)
            clone.query.partition_id_mode = partition_id
        return clone

    def delete(self):
        partition_ids = getattr(self.query, "partition_ids", ())
        if len(partition_ids) > 1:
            total_deleted = 0
            all_counts = {}
            for pid in partition_ids:
                clone = self.all()
                clone.query.partition_ids = (pid,)
                deleted, counts = clone.delete()
                total_deleted += deleted
                for key, val in counts.items():
                    all_counts[key] = all_counts.get(key, 0) + val
            return total_deleted, all_counts
        return super().delete()

    def update(self, **kwargs):
        partition_ids = getattr(self.query, "partition_ids", ())
        if len(partition_ids) > 1:
            total = 0
            for pid in partition_ids:
                clone = self.all()
                clone.query.partition_ids = (pid,)
                total += clone.update(**kwargs)
            return total
        return super().update(**kwargs)

    def datetimes(self, field_name, kind, order="ASC", tzinfo=None):
        """
        Return a list of datetime objects representing all available
        datetimes for the given field_name, scoped to 'kind'.
        """
        if kind not in ("year", "month", "week", "day", "hour", "minute", "second"):
            raise ValueError(
                "'kind' must be one of 'year', 'month', 'week', 'day', "
                "'hour', 'minute', or 'second'."
            )
        if order not in ("ASC", "DESC"):
            raise ValueError("'order' must be either 'ASC' or 'DESC'.")

        if kind in ("year", "month", "week"):
            output_field = fields.DateField()
        else:
            output_field = fields.DateTimeField()
        return (
            self.annotate(
                datetimefield=Trunc(
                    field_name,
                    kind,
                    output_field=output_field,
                    tzinfo=tzinfo,
                ),
                plain_field=F(field_name),
            )
            .values_list("datetimefield", flat=True)
            .distinct()
            .filter(plain_field__isnull=False)
            .order_by(("-" if order == "DESC" else "") + "datetimefield")
        )
