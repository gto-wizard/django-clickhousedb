from clickhouse_backend import models


class RouterTestModel(models.ClickhouseModel):
    name = models.StringField(default="")

    class Meta:
        app_label = "clickhouse_router"
        engine = models.MergeTree(order_by="id")
