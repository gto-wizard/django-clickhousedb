from clickhouse_backend import models


class LightweightEvent(models.ClickhouseModel):
    name = models.StringField(default="")
    value = models.Int32Field(default=0)

    class Meta:
        engine = models.MergeTree(order_by="id")


class LightweightUpdateEvent(models.ClickhouseModel):
    """Model with block_number/block_offset settings for lightweight UPDATE."""

    name = models.StringField(default="")
    value = models.Int32Field(default=0)

    class Meta:
        engine = models.MergeTree(
            order_by="id",
            enable_block_number_column=1,
            enable_block_offset_column=1,
        )


class LightweightStudent(models.ClickhouseModel):
    name = models.StringField()
    score = models.Int8Field()

    class Meta:
        engine = models.ReplicatedMergeTree(order_by="id")
        cluster = "cluster"


class LightweightDistributed(models.ClickhouseModel):
    name = models.StringField()
    score = models.Int8Field()

    class Meta:
        engine = models.Distributed(
            "cluster",
            models.currentDatabase(),
            LightweightStudent._meta.db_table,
            models.Rand(),
        )
        cluster = "cluster"
