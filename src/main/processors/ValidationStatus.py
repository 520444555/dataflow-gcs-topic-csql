import apache_beam as beam
import argparse
import logging
import json
from apache_beam import pvalue
import logging
class ValidationStatusUpdate(beam.DoFn):
    def process(self,element,logging_mode):
        #adding logs
        logging.getLogger().setLevel(logging.getLevelName(logging_mode))
        #json row for event status
        file_row=json.loads(element)
        #extracting key value from json data
        if file_row['stateMachineId'] == 'status':
            if len(file_row.get('reasons'))==0:
                reasons_name=None
            else:
                reasons_name=file_row.get('reasons')[0].get('name',None)
            for id in file_row.get('ids'):
                schema=id.get('payload').get('schema')
                #only alerted records are loaded into sql db and pubsub
                if schema['alert_id'] is not None and schema['alert_id'].strip() != '':
                    logging.info('creating data for pubsub event_status_transfers')
                    event_status_transfers={}

                    event_status_transfers["source"]="UK_Feedzai"
                    event_status_transfers["alert_id"]=schema['alert_id']
                    event_status_transfers["alert_type"]=id['channelId']
                    event_status_transfers["customer_id"]=schema['customer_id']
                    event_status_transfers["alert_entity_value"]=id['identifier']
                    event_status_transfers["alert_fraud_state"]=file_row['state']['id']
                    event_status_transfers["alert_state_reason"]=reasons_name
                    event_status_transfers["alert_update_timestamp"]=file_row['updatedAt']
                    
                    #tagging json for pubsub
              
                    event_status_transfers_pubsub={"event_state_updates_status_transfers":event_status_transfers}
                    logging.info('Event State Transfers JSON: %s',json.dumps(event_status_transfers_pubsub,default=str))
                    yield pvalue.TaggedOutput('pubsub',json.dumps(event_status_transfers_pubsub,default=str))
                    
                    #tagging json for sql sb

                    event_status_transfers['type']="event_status_transfers"
                    yield pvalue.TaggedOutput('upsert',json.dumps(event_status_transfers,default=str))

                else:
                    #not alerted records are discared
                    logging.info("alert_id is null--- record not processed")

            
        elif file_row['stateMachineId'] == 'operational_status':
            #extracting key value from json data
            if len(file_row.get('reasons'))==0:
                reasons_name=None
            else:
                reasons_name=file_row.get('reasons')[0].get('name',None)
            for id in file_row.get('ids'):
                schema=id.get('payload').get('schema')
                #only alerted records are loaded into sql db and pubsub
                if schema['alert_id'] is not None and schema['alert_id'].strip() != '':
                    logging.info('creating data for pubsub operational_status')
                    event_operational_status={}

                    event_operational_status["source"]="UK_Feedzai"
                    event_operational_status["alert_id"]=schema['alert_id']
                    event_operational_status["alert_type"]=id['channelId']
                    event_operational_status["customer_id"]=schema['customer_id']
                    event_operational_status["alert_entity_value"]=id['identifier']
                    event_operational_status["customer_portfolio_region"]=schema['customer_portfolio_region']
                    event_operational_status["customer_portfolio_product"]=schema['customer_portfolio_product']
                    event_operational_status["customer_portfolio_channel"]=schema['customer_portfolio_channel']
                    event_operational_status["customer_portfolio_country"]=schema['customer_portfolio_country']
                    event_operational_status_pubsub={"event_state_updates_operational_status":event_operational_status}
                    logging.info('Event operational status JSON: %s',json.dumps(event_operational_status_pubsub,default=str))
                    yield pvalue.TaggedOutput('pubsub',json.dumps(event_operational_status_pubsub,default=str))
                    
                    
                    
                    event_operational_status['type']="event_operational_status"
                    #tagging json for sql sb
                    
                    

                    yield pvalue.TaggedOutput('upsert',json.dumps(event_operational_status,default=str))
                else:
                    #not alerted records are discared
                    logging.info("alert_id is null--- record not processed")
        else:
            #record not contains required statemachineid
            logging.info('record not valid for upsert')
