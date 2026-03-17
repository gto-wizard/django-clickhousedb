from clickhouse_backend.models import ClickhouseModel
from clickhouse_backend.utils import get_subclasses


class ClickHouseRouter:
    def __init__(self):
        self.route_model_names = set()
        for model in get_subclasses(ClickhouseModel):
            if model._meta.abstract:
                continue
            self.route_model_names.add(model._meta.label_lower)

    def _is_clickhouse(self, model, hints):
        return (
            model._meta.label_lower in self.route_model_names
            or hints.get("target") == "clickhouse"
        )

    def db_for_read(self, model, **hints):
        return "clickhouse" if self._is_clickhouse(model, hints) else None

    def db_for_write(self, model, **hints):
        return "clickhouse" if self._is_clickhouse(model, hints) else None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if (
            f"{app_label}.{model_name}" in self.route_model_names
            or hints.get("target") == "clickhouse"
        ):
            return db == "clickhouse"
        elif db == "clickhouse":
            return False
        return None
