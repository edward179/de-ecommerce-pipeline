"""M7: dlt loads order events from the lake (Parquet micro-batches) into DuckDB (schema raw_events)."""
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

# Files written by m7-streaming/consumer.py
EVENTS_GLOB = "events/order_events/dt=*/*.parquet"


def order_events():
    files = filesystem(file_glob=EVENTS_GLOB)
    # Read only files added since the last run (cursor kept in the pipeline state)
    files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    resource = (files | read_parquet(use_pyarrow=True, chunksize=50_000)).with_name("order_events")
    # The consumer is at-least-once: a re-read event keeps its event_id, so merge keeps one row
    resource.apply_hints(primary_key="event_id", write_disposition="merge")
    return resource


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="olist_events_to_duckdb",
        destination=dlt.destinations.duckdb("warehouse/olist.duckdb"),
        dataset_name="raw_events",
    )
    info = pipeline.run(order_events())
    print(info)
    print(pipeline.last_trace.last_normalize_info)   # rows per table in this run


if __name__ == "__main__":
    main()
