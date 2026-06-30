# dataflow_pipeline.py
# Streaming Dataflow job: Pub/Sub -> BigQuery with data validation

import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam import DoFn, ParDo
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms.window import TimestampedValue


def parse_iso8601(ts_str):
    """Convert ISO8601 string to Unix timestamp (seconds)."""
    return datetime.fromisoformat(ts_str.rstrip('Z')).timestamp()

class ParseAndValidateEvent(DoFn):
    """Parses Pub/Sub messages into validated dictionaries."""
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            # Basic type casting
            parsed = {
                'user_id':      str(record['user_id']),
                'game_id':      str(record['game_id']),
                'event_type':   str(record['event_type']),
                'score_delta':  int(record.get('score_delta', 0)),
                'wager_amount': float(record.get('wager_amount', 0.0)),
                'ts':           record['ts']  # ISO8601 timestamp
            }
            yield parsed
        except Exception as e:
            logging.warning(f"Failed to parse record, skipping: {e} | {element}")


def is_valid(row):
    """Filter out invalid or malformed records."""
    # Required fields
    if not row['user_id'] or not row['game_id']:
        return False
    # Event type must be one of expected
    if row['event_type'] not in ('play_round', 'wager', 'login'):
        return False
    # Numeric validations
    if row['score_delta'] < -1000 or row['score_delta'] > 10000:
        return False
    if row['wager_amount'] < 0 or row['wager_amount'] > 100000:
        return False
    # Timestamp parse
    try:
        _ = parse_iso8601(row['ts'])
    except Exception:
        return False
    return True


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project',          required=True)
    parser.add_argument('--region',           required=True)
    parser.add_argument('--temp_location',    required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--pubsub_topic',     default='gaming-events')
    parser.add_argument('--bq_dataset',       default='gaming')
    parser.add_argument('--bq_raw_table',     default='raw_events')
    parser.add_argument('--runner',           default='DataflowRunner')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        runner=known_args.runner,
        save_main_session=True,
    )
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    # BigQuery table spec
    bq_table = f"{known_args.project}:{known_args.bq_dataset}.{known_args.bq_raw_table}"

    with beam.Pipeline(options=options) as p:
        # 1) Read raw bytes and parse JSON into dicts
        parsed = (
            p
            | 'ReadPubSub' >> ReadFromPubSub(topic=f"projects/{known_args.project}/topics/{known_args.pubsub_topic}")
            | 'ParseJSON'  >> ParDo(ParseAndValidateEvent())
        )

        # 2) Attach event-time timestamps
        with_ts = (
            parsed
            | 'AddTimestamps' >> beam.Map(lambda row: TimestampedValue(row, parse_iso8601(row['ts'])))
        )

        # 3) Data validations
        valid = (
            with_ts
            | 'ValidateRecords' >> beam.Filter(is_valid)
        )

        # 4) Write validated records to BigQuery
        valid | 'WriteToBigQuery' >> WriteToBigQuery(
            table=bq_table,
            schema='user_id:STRING,game_id:STRING,event_type:STRING,score_delta:INTEGER,wager_amount:FLOAT,ts:TIMESTAMP',
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=known_args.temp_location
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
