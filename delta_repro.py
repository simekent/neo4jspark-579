import argparse
import os
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.readwriter import DataStreamWriter


def read_stream(
    spark: SparkSession,
    table_dir: str,
):
    return (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 1)
        .option("maxBytesPerTrigger", 1)
        .load(table_dir)
    )


def stream_writer(stream_df: DataFrame, checkpoint_location: str):
    return stream_df.writeStream.option(
        "checkpointLocation", checkpoint_location
    ).format("console")


def try_delete_checkpoint(checkpoint_location: str, checkpoint: int):
    result = False
    for dirs in ("commits", "offsets"):
        dir_path = Path(checkpoint_location, dirs)
        for files in (
            dir_path.glob(str(checkpoint)),
            dir_path.glob(f".{checkpoint}.crc*"),
        ):
            for f in files:
                os.unlink(f)
                result = True

    return result


def repro(
    writer: DataStreamWriter,
    processing_time: str = "500 milliseconds",
    checkpoint_location: str | None = None,
    delete_checkpoints_from: int | None = None,
):
    print("RUN 1")
    streaming_query = writer.trigger(processingTime=processing_time).start()
    streaming_query.awaitTermination(5)
    streaming_query.stop()

    if checkpoint_location is not None and delete_checkpoints_from is not None:
        while try_delete_checkpoint(checkpoint_location, delete_checkpoints_from):
            delete_checkpoints_from = delete_checkpoints_from + 1

    # Stop the stream and resume for one trigger simulating failure after restart.
    print("RUN 2")
    streaming_query = writer.trigger(processingTime=processing_time).start()
    streaming_query.awaitTermination(5)
    streaming_query.stop()


def seed_data(spark: SparkSession, table_dir: str):
    if not DeltaTable.isDeltaTable(spark, table_dir):
        spark.createDataFrame([[v] for v in range(0, 50)], ["value"]).orderBy(
            "value"
        ).write.format("delta").option("maxRecordsPerFile", 1).option(
            "path", args.table_dir
        ).save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table-dir", type=str, required=True)
    parser.add_argument("-c", "--checkpointLocation", type=str, required=True)
    parser.add_argument("-d", "--delete-checkpoints-from", type=int)
    args = parser.parse_args()

    spark = (
        SparkSession.Builder()
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("FATAL")
    seed_data(spark, args.table_dir)
    stream_df = read_stream(spark, args.table_dir)
    writer = stream_writer(stream_df, args.checkpointLocation)
    repro(
        writer,
        checkpoint_location=args.checkpointLocation,
        delete_checkpoints_from=args.delete_checkpoints_from,
    )
