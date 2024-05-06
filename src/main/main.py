import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json
from options import MyPipelineOptions
from processors import UpsertEventArrival
from processors import UpsertStatusUpdate
from processors import Validation
from processors import getData
from processors import getFile
from processors import ValidationEvent
from processors import ValidationStatus
from processors import sqlconnection
import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Test'))
import test_main
import unittest
#only calling statement should be here
if __name__ == '__main__':
    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions.MyPipelineOptions)
    logging.getLogger().setLevel(logging.INFO)
    suite = unittest.TestLoader().loadTestsFromModule(test_main)
    result_unittest = unittest.TextTestRunner().run(suite)
    
    
    if result_unittest.wasSuccessful():
        #starting pipeline
        p = beam.Pipeline(options=PipelineOptions())
        #list of files receveing
        file_list=['CM_UPDATES_EVENTS_EVENT','CM_UPDATES_EVENTS_STATUS','file_not_found']
        #getting pubsub message
        notification=(p | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription=my_pipeline_options.subscription))
        file_name = notification | 'Get file name' >> beam.Map(getFile.getfile)
        #sepearating the type of file
        file_type = file_name | 'Get file type' >> beam.ParDo(getData.getdata(),my_pipeline_options.logging_mode).with_outputs(*file_list)
        
        #Case for Events Event
       
        #Case for Events Event
        cm_updates_event = (file_type['CM_UPDATES_EVENTS_EVENT'] | 'read cm_updates_event' >> beam.io.ReadAllFromText()
        | 'validating event' >> beam.ParDo(Validation.Validate(),my_pipeline_options.logging_mode,"cm_event_arrival",my_pipeline_options.ivr_table_name,my_pipeline_options.ivr_error_table_name).with_outputs('exception_row','validated_row'))
        validaton_event_arrival=cm_updates_event['validated_row'] | beam.ParDo(ValidationEvent.ValidationEventArrival(),my_pipeline_options.logging_mode).with_outputs('pubsub','upsert')
        load_cm_updates_event=validaton_event_arrival['upsert'] | beam.ParDo(UpsertEventArrival.UpsertEventArrival(my_pipeline_options.logging_mode,my_pipeline_options.ivr_table_name,my_pipeline_options.sql_instance,my_pipeline_options.username,my_pipeline_options.db_name),my_pipeline_options.logging_mode,my_pipeline_options.ivr_table_name,my_pipeline_options.sql_instance,my_pipeline_options.username,my_pipeline_options.db_name)
        load_cm_updates_event_error=cm_updates_event['exception_row'] | 'loadtoBQerror cm_updates_event' >> beam.io.WriteToBigQuery(my_pipeline_options.ivr_error_table_name,ignore_unknown_columns=True,create_disposition='CREATE_NEVER',write_disposition='WRITE_APPEND',method='STREAMING_INSERTS')
        
        
        #publishing events event alerted into pubsub
        publish_to_pubsub_event=(validaton_event_arrival['pubsub'] | 'UTF-8-event' >> beam.Map(lambda x: x.encode('utf-8'))
        | 'PublishtoPubsub cm_updates_event' >>  beam.io.WriteToPubSub(my_pipeline_options.topic_name))
        
        #Case for Events status
        cm_updates_status = (file_type['CM_UPDATES_EVENTS_STATUS'] | 'read cm_updates_status' >> beam.io.ReadAllFromText()
        | 'validating status' >> beam.ParDo(Validation.Validate(),my_pipeline_options.logging_mode,"cm_event_state_updates",my_pipeline_options.ivr_table_name,my_pipeline_options.ivr_error_table_name).with_outputs('exception_row','validated_row'))

        validaton_event_status=cm_updates_status['validated_row'] | beam.ParDo(ValidationStatus.ValidationStatusUpdate(),my_pipeline_options.logging_mode).with_outputs('pubsub','upsert')
        load_cm_updates_status= validaton_event_status['upsert'] | beam.ParDo(UpsertStatusUpdate.UpsertStatusUpdate(my_pipeline_options.logging_mode,my_pipeline_options.ivr_table_name,my_pipeline_options.sql_instance,my_pipeline_options.username,my_pipeline_options.db_name),my_pipeline_options.logging_mode,my_pipeline_options.ivr_table_name,my_pipeline_options.sql_instance,my_pipeline_options.username,my_pipeline_options.db_name)
        load_cm_updates_status_error=cm_updates_status['exception_row'] | 'loadtoBQerror cm_updates_status' >> beam.io.WriteToBigQuery(my_pipeline_options.ivr_error_table_name,ignore_unknown_columns=True,create_disposition='CREATE_NEVER',write_disposition='WRITE_APPEND',method='STREAMING_INSERTS')
        #publising event status alerted into pubsub
        publish_to_pubsub_status=(validaton_event_status['pubsub'] | 'UTF-8-status' >> beam.Map(lambda x: x.encode('utf-8'))
        | 'PublishtoPubsub cm_updates_status' >>  beam.io.WriteToPubSub(my_pipeline_options.topic_name))

        result = p.run()
        result.wait_until_finish()
