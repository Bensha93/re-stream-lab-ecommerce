"""
Apache Beam Pipeline: Pub/Sub → BigQuery + GCS
-----------------------------------------------
Reads JSON events from Pub/Sub, routes by type, writes to BigQuery tables and GCS backup
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import json
from datetime import datetime
import logging

# ---------------------------------------------------------------------------
# CONFIGURATION - GCP project details and resource names (Objects/Topics/Tables)
# ---------------------------------------------------------------------------
PROJECT_ID = "re-stream-lab-ecommerce"
SUBSCRIPTION = "projects/re-stream-lab-ecommerce/subscriptions/backend-events-topic-sub"
TOPIC = "projects/re-stream-lab-ecommerce/topics/backend-events-topic"
DATASET = "backend_events"
GCS_BUCKET = "gs://re_ecommerce_bucket"


# ----------------------------------------------------------------------------
# 1.0 : Parse incoming JSON messages from Pub/Sub for processing
# ---------------------------------------------------------------------------
class ParseJson(beam.DoFn):
    """
    Converts raw Pub/Sub bytes to Python dictionary
    Adds 'created_at' timestamp for when record was processed
    """
    def process(self, element):
        try:
            # Pub/Sub messages come as bytes, decode to string then parse JSON
            data = json.loads(element.decode('utf-8'))
            
            # Add processing timestamp
            data['created_at'] = datetime.utcnow().isoformat()
            
            yield data
        except Exception as e:
            logging.error(f"Parse error: {e}")


# ---------------------------------------------------------------------------
# 2.0 : Loading into GCS as backup (organized by date/time) --- RAW STAGE
# ---------------------------------------------------------------------------
class WriteToGCS(beam.DoFn):
    """
    Organizes files in GCS by: event_type/year/month/day/hour/minute/
    Example: gs://bucket/order/2025/11/04/14/30/order_20251104143000.json
    """
    def process(self, element):
        event_type = element.get('event_type')
        
        # Get timestamp from event (different field names per event type)
        if event_type == 'order':
            ts_str = element.get('order_date')
        else:
            ts_str = element.get('timestamp')
        
        # Parse timestamp or use current time if invalid
        try:
            ts = datetime.fromisoformat(ts_str.replace('Z', ''))
        except:
            ts = datetime.utcnow()
        
        # Create hierarchical path: event_type/YYYY/MM/DD/HH/mm/
        path = f"{event_type}/{ts.year:04d}/{ts.month:02d}/{ts.day:02d}/{ts.hour:02d}/{ts.minute:02d}/"
        filename = f"{event_type}_{ts.strftime('%Y%m%d%H%M%S')}.json"
        
        yield beam.pvalue.TaggedOutput('gcs', (path + filename, json.dumps(element)))


# ---------------------------------------------------------------------------
# MAIN PIPELINE and RUNNER CONFIGURATION (Dataflow: Pub/Sub -> BigQuery + GCS)
# ---------------------------------------------------------------------------
def run():
    # Configure Dataflow options
    options = PipelineOptions(
        project=PROJECT_ID,
        runner='DataflowRunner',      # using 'DirectRunner' for local testing
        region='us-central1',
        temp_location=f'{GCS_BUCKET}/temp',
        staging_location=f'{GCS_BUCKET}/staging',
        streaming=True,                # To Enable streaming mode for real-time processing
        save_main_session=True,        # To save main session for Dataflow workers
        job_name='backend-events-pipeline'
    )
    
    with beam.Pipeline(options=options) as p:
        
        # ---------------------------------------------------------------------
        # 1.0 : Reading from Pub/Sub and parse JSON Messages -- LOAD STAGE
        # ---------------------------------------------------------------------
        events = (
            p 
            | 'Read Pub/Sub' >> ReadFromPubSub(subscription=SUBSCRIPTION)
            | 'Parse JSON' >> beam.ParDo(ParseJson())
        )
        
        # ---------------------------------------------------------------------
        # 2.0 : Route and write ORDERS to BigQuery -- TRANSFORM STAGE
        # ---------------------------------------------------------------------
        orders = (
            events 
            | 'Filter Orders' >> beam.Filter(lambda x: x.get('event_type') == 'order')
        )
        orders | 'Write Orders to BigQuery' >> WriteToBigQuery(
            're-stream-lab-ecommerce.backend_events.orders',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
        # ---------------------------------------------------------------------
        # 3.0 : Route and write INVENTORY to BigQuery -- TRANSFORM STAGE
        # ---------------------------------------------------------------------
        inventory = (
            events 
            | 'Filter Inventory' >> beam.Filter(lambda x: x.get('event_type') == 'inventory')
        )
        inventory | 'Write Inventory to BigQuery' >> WriteToBigQuery(
            're-stream-lab-ecommerce.backend_events.inventory',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
        # ---------------------------------------------------------------------
        # 4.0: Route and write USER ACTIVITY to BigQuery -- TRANSFORM STAGE
        # ---------------------------------------------------------------------
        activity = (
            events 
            | 'Filter Activity' >> beam.Filter(lambda x: x.get('event_type') == 'user_activity')
        )
        activity | 'Write Activity to BigQuery' >> WriteToBigQuery(
            're-stream-lab-ecommerce.backend_events.user_activity',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()