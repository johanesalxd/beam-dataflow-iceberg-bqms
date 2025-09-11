# Streaming Aggregation Pipeline

This document details the optional streaming aggregation branch of the Pub/Sub to BigQuery pipeline. This branch performs a stateful, windowed aggregation on the taxi ride data to provide near real-time insights.

## Overview

This optional pipeline branch:
- Performs a real-time aggregation of passenger counts grouped by ride status.
- Operates on one-hour tumbling windows.
- Emits updated results every 5 minutes.
- Handles late-arriving data and guarantees exactly-once processing by deduplicating messages.

## Aggregation Logic Explained

The core of this feature is a sophisticated windowing and triggering strategy designed for accuracy and low latency.

### 1. Windowing

Events are grouped into fixed, non-overlapping **one-hour windows** based on the `event_time` from the message data. This is known as a "tumbling window."

### 2. Handling Late Data

Real-world data is often delayed. This pipeline is configured to handle this gracefully:
- **Allowed Lateness**: The window is configured with an `allowed_lateness` of **10 minutes**.
- If an event that belongs in a closed window arrives within this 10-minute grace period, it is still included in the calculation, and the pipeline emits a new, corrected aggregation for that window.

### 3. Triggering

Within each one-hour window, the pipeline needs to know *when* to emit the aggregated results. We use a repeating trigger:
- **Trigger**: Every 5 minutes of processing time, the pipeline calculates and emits the current sum of passengers for each ride status within the window.
- **Accumulation Mode**: The pipeline uses `ACCUMULATING` mode, meaning each time it fires, it emits the total accumulated value for the window so far.

### 4. Deduplication (Exactly-Once Processing)

To prevent double-counting from Pub/Sub's "at-least-once" delivery, the pipeline implements stateful deduplication.
- Each message is keyed by its unique `ride_id`.
- A custom stateful DoFn maintains a set of seen ride_ids per window, ensuring exactly-once processing even with frequent triggers.
- State is automatically garbage collected when windows expire (window_end + allowed_lateness + buffer).

## Output Table and Schema

The aggregated data is written to a separate BigQuery table specified by the `--output_agg_table` parameter. This table is automatically partitioned by the hour on the `window_start` field to optimize queries.

**Schema (`schemas/agg_table.json`):**
```json
[
  {
    "name": "window_start",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "window_end",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "ride_status",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "passenger_count",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
```

## How to Run

This streaming aggregation feature is designed for and supported on the **Dataflow runner only**, due to the high volume of the public data source.

The `run_dataflow.sh` script is pre-configured to automatically:
1.  Enable the aggregation pipeline branch.
2.  Create the necessary BigQuery view (`taxirides_hourly_latest`) for querying the results.

Simply execute the script to deploy the pipeline:
```bash
./run_dataflow.sh
```

## Querying the Aggregated Data

The `run_dataflow.sh` script automatically creates a BigQuery view named `taxirides_hourly_latest` in your dataset. This view always shows the most recent, up-to-date aggregation for each window.

You should always query the **view**, not the raw output table.

### Querying the View

Your users can now query the `hourly_rides_latest` view to get a clean, simple, and up-to-date view of the data:

```sql
-- Get the latest passenger count for all ride statuses in the last 3 hours
SELECT
  window_start,
  ride_status,
  passenger_count
FROM
  `your-project.your_dataset.hourly_rides_latest`
WHERE
  window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
ORDER BY
  window_start DESC,
  passenger_count DESC;
```

## Deduplication Deep Dive

This section provides an in-depth analysis of different deduplication approaches in Apache Beam streaming pipelines, their trade-offs, and implementation considerations.

### The Challenge

Streaming deduplication in Apache Beam is complex due to the interaction between windowing, triggers, and accumulation modes. The goal is to ensure exactly-once processing per window while maintaining real-time data flow.

### Current Implementation: Stateful DoFn

```python
class StatefulDeduplication(beam.DoFn):
    """
    Stateful deduplication that maintains state across triggers within windows.
    Relies on window expiration for state cleanup.
    """
    SEEN_IDS_STATE = BagStateSpec(
        'seen_ids', beam.coders.StrUtf8Coder())

    def process(
            self,
            element,
            seen_ids=beam.DoFn.StateParam(SEEN_IDS_STATE)):
        """Process elements and deduplicate based on ride_id."""
        ride_id, message = element
        current_seen = set(seen_ids.read())

        if ride_id not in current_seen:
            seen_ids.add(ride_id)
            yield message
```

And here is how it's used in the pipeline:

```python
# Apply windowing and deduplication
(
    parsed_messages
    | 'ApplyWindow' >> beam.WindowInto(
        FixedWindows(3600),
        trigger=trigger.Repeatedly(trigger.AfterProcessingTime(300)),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        allowed_lateness=600
    )
    | 'ExtractRideIdAsKey' >> beam.Map(lambda x: (x['ride_id'], x))
    | 'StatefulDeduplication' >> beam.ParDo(StatefulDeduplication())
    ...
)
```

**How it works:**
- Maintains per-window state of seen ride_ids
- State persists across all trigger firings within the window
- Uses ACCUMULATING mode throughout for real-time updates

### State Lifecycle

**State Duration:** Window start → Window end + allowed_lateness + buffer
- Window 14:00-15:00: State active from 14:00:00 to ~15:10:30
- Automatic cleanup prevents memory leaks
- Fault-tolerant with checkpointing and recovery

**Timeline Example:**
```
14:00:00 - Window [14:00-15:00] opens, state created
14:00:05 - First trigger fires, processes ride_123 (new), adds to state
14:00:10 - Second trigger fires, processes ride_123 (duplicate), ignores
14:00:15 - Third trigger fires, processes ride_456 (new), adds to state
...
15:00:00 - Window closes, but state remains for late data
15:05:00 - Late data arrives for ride_789, processed and added to state
15:10:00 - Allowed lateness expires, final trigger fires
15:10:30 - State garbage collected automatically
```

### A Note on Beam Schemas and SqlTransform

A key learning from this project was the importance of ensuring that `PCollection`s have a well-defined schema before being passed to a `SqlTransform`.

The `SqlTransform` relies on Beam's schema-aware PCollections to understand the data structure. If the schema is not correctly inferred, the pipeline will fail with a cryptic `pythonsdk_any:v1` error.

To solve this, we implemented the following pattern in our `JsonToRow` transform:

```python
class JsonToRow(beam.DoFn):
    def process(self, element):
        yield beam.Row(
            event_time=str(element.get('event_time')),
            processing_time=str(element.get('processing_time')),
            ride_id=str(element.get('ride_id')),
            ride_status=str(element.get('ride_status')),
            passenger_count=int(element.get('passenger_count')),
            payload=str(element.get('payload'))
        )
```

By explicitly casting all fields to their expected types (e.g., `str()`, `int()`), we provide Beam with the necessary information to correctly infer the schema, allowing the `SqlTransform` to work as expected. This is a crucial pattern to follow when working with `SqlTransform` in Beam.

## Troubleshooting Common Errors

### `KeyError: 'after_synchronized_processing_time'`

This error typically indicates a version mismatch between the Apache Beam SDK and the runner environment (e.g., Dataflow). The `after_synchronized_processing_time` trigger was part of an older API and has since been deprecated or changed.

**Solution:**

-   **Upgrade `apache-beam`**: Ensure you are using a recent version of the `apache-beam` library. This error is often resolved by simply upgrading to the latest stable version.
-   **Check Runner Compatibility**: Verify that the Beam SDK version is compatible with the runner you are using. Refer to the official Beam documentation for the compatibility matrix.
-   **Update Trigger Logic**: If upgrading is not an option, you may need to update your trigger logic to use the current API. For example, `AfterProcessingTime` is the recommended trigger for processing-time-based operations.
