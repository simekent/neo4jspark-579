import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.readwriter import DataStreamWriter


def read_stream(
    spark: SparkSession,
    bootstrap_server: str,
    topic: str,
):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("max.poll.records", 1)
        .option("maxOffsetsPerTrigger", 1)
        .option("kafka.group.id", "mik")
        .load()
        .selectExpr("CAST(value AS STRING)", "offset")
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
    streaming_query.awaitTermination(10)
    streaming_query.stop()

    if checkpoint_location is not None and delete_checkpoints_from is not None:
        while try_delete_checkpoint(checkpoint_location, delete_checkpoints_from):
            delete_checkpoints_from = delete_checkpoints_from + 1

    # Stop the stream and resume for one trigger simulating failure after restart.
    print("RUN 2")
    streaming_query = writer.trigger(once=True).start()
    streaming_query.awaitTermination()
    streaming_query.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-b", "--bootstrap-server", type=str, default="http://localhost:9092"
    )
    parser.add_argument("-t", "--topic", type=str, required=True)
    parser.add_argument("-c", "--checkpointLocation", type=str, required=True)
    parser.add_argument("-d", "--delete-checkpoints-from", type=int)
    args = parser.parse_args()
    spark = (
        SparkSession.Builder()
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("FATAL")

    stream_df = read_stream(spark, args.bootstrap_server, args.topic)
    writer = stream_writer(stream_df, args.checkpointLocation)
    repro(
        writer,
        checkpoint_location=args.checkpointLocation,
        delete_checkpoints_from=args.delete_checkpoints_from,
    )
