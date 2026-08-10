from django.db import models
from django.db.migrations import state
from django.db.models import options

from .query import QuerySet
from .sql import Query
from clickhouse_backend import compat

# WHY THIS PATCH EXISTS:
# Django's ModelState.from_model() iterates DEFAULT_NAMES to capture Meta options
# into migration files (state.py line ~835). Without "engine" and "cluster" in this
# tuple, makemigrations silently drops these options — migrations would create tables
# with wrong engines. Django provides no backend-specific hook for custom Meta options.
# Both options.DEFAULT_NAMES and state.DEFAULT_NAMES must be patched because:
# - options.DEFAULT_NAMES: used by Options.__init__() to populate original_attrs
# - state.DEFAULT_NAMES: used by ModelState.from_model() to serialize into migrations
_CLICKHOUSE_META_OPTIONS = ("engine", "cluster")
if "engine" not in options.DEFAULT_NAMES:
    options.DEFAULT_NAMES = (*options.DEFAULT_NAMES, *_CLICKHOUSE_META_OPTIONS)
    state.DEFAULT_NAMES = options.DEFAULT_NAMES


class ClickhouseManager(models.Manager):
    _queryset_class = QuerySet

    def get_queryset(self):
        return self._queryset_class(
            model=self.model, query=Query(self.model), using=self._db, hints=self._hints
        )

    def settings(self, **kwargs):
        return self.get_queryset().settings(**kwargs)

    def prewhere(self, *args, **kwargs):
        return self.get_queryset().prewhere(*args, **kwargs)

    def datetimes(self, *args, **kwargs):
        return self.get_queryset().datetimes(*args, **kwargs)


class ClickhouseModel(models.Model):
    objects = ClickhouseManager()
    _overwrite_base_manager = ClickhouseManager()

    class Meta:
        abstract = True
        base_manager_name = "_overwrite_base_manager"

    # Both branches are required while the package supports Django 5.0+ (see the
    # classifiers in pyproject.toml and the tox matrix). Django 6.0 changed the
    # contract Model._save_table relies on: _do_update gained returning_fields and
    # returns the returned rows, while <6.0 passes six arguments and expects a bool.
    # QuerySet._update follows the same split, so one signature cannot serve both.
    if compat.dj_ge6:

        def _do_update(
            self,
            base_qs,
            using,
            pk_val,
            values,
            update_fields,
            forced_update,
            returning_fields,
        ):
            filtered = base_qs.filter(pk=pk_val)
            if not values:
                if update_fields is not None or filtered.exists():
                    return [()]
                return []
            return filtered._update(values, returning_fields)

    else:

        def _do_update(
            self, base_qs, using, pk_val, values, update_fields, forced_update
        ):
            filtered = base_qs.filter(pk=pk_val)
            if not values:
                # Saving a model in an inheritance chain where update_fields targets
                # no field of this model, or a PK-only model: the update succeeded as
                # long as the row still exists.
                return update_fields is not None or filtered.exists()
            return filtered._update(values) > 0
