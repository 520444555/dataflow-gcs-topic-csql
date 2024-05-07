import logging
import apache_beam as beam
from apache_beam import pvalue

class getdata(beam.DoFn):
        def process(self,element,logging_mode):
                logging.getLogger().setLevel(logging.getLevelName(logging_mode))
                
                full_name=str(element)
                #getting full name of file with gcs path
                logging.info('file name pattern matching ------------------: %s ', full_name)
                #getting only file name
                file_name=full_name.rsplit("/",1)[-1]
                #tag file with file name
                if "CM_UPDATES_EVENTS-EVENT" in file_name :
                        #tag correct file name
                        yield pvalue.TaggedOutput("CM_UPDATES_EVENTS_EVENT",full_name)
                elif "CM_UPDATES_EVENTS-STATUS" in file_name:
                        yield pvalue.TaggedOutput("CM_UPDATES_EVENTS_STATUS",full_name)
                else:
                        #incorrect file tag as incorrect
                        incorrect_file={}
                        incorrect_file['input_file_name']=full_name
                        incorrect_file['target_table_name']="ext_fraud_alert_schema.ext_fraud_alert"
                        incorrect_file['error_message']="FILE NAME NOT MATCHING PATTERN"
                        incorrect_file['event_data']="FILE NAME NOT MATCHING PATTERN"
                        #tag incorrect file
                        yield  pvalue.TaggedOutput('file_not_found',incorrect_file)
