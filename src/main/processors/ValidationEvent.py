import apache_beam as beam
import argparse
import logging
import json
from apache_beam import pvalue
import logging
class ValidationEventArrival(beam.DoFn):
    def process(self, element,logging_mode):
        #adding logs
        logging.getLogger().setLevel(logging.getLevelName(logging_mode))
        #json row
        file_row=json.loads(element)
        #getting key values from json row
        schema=file_row.get('id').get('payload').get('schema')
        id_row=file_row.get('id')
        #pass only alerted events
        if 'alert' in file_row.keys() and file_row["alert"] == True and schema["alert_id"] is not None and schema['alert_id'].strip() != '':

            logging.info('creating data for pubsub')
            event_arrival={}

            event_arrival["source"]="UK_Feedzai"
            event_arrival["alert_id"]=schema['alert_id']
            event_arrival["alert_type"]=id_row['channelId']
            event_arrival["customer_id"]=schema['customer_id']
            event_arrival["alert_entity_value"]=id_row['identifier']
            event_arrival["customer_portfolio_region"]=schema['customer_portfolio_region']
            event_arrival["customer_portfolio_product"]=schema['customer_portfolio_product']
            event_arrival["customer_portfolio_channel"]=schema['customer_portfolio_channel']
            event_arrival["customer_portfolio_country"]=schema['customer_portfolio_country']
            event_arrival["customer_portfolio_type"]=schema['customer_portfolio_type']
            event_arrival["customer_portfolio_class"]=schema['customer_portfolio_class']
            try:
                event_arrival["alert_create_timestamp"]=file_row['versionTimestamp']
            except Exception:
                event_arrival["alert_create_timestamp"]=None

            event_arrival_pubsub={"event_arrival":event_arrival}
            
            #tagging for pubsub and sql db upsert

            yield pvalue.TaggedOutput('pubsub',json.dumps(event_arrival_pubsub,default=str))
            yield pvalue.TaggedOutput('upsert',json.dumps(event_arrival,default=str))
        else:
            #not alerted events are not loaded
            logging.info('record not valid for event arrival upsert')
