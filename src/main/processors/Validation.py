import logging
import apache_beam as beam
import json
from modules import tables_schema
from apache_beam import pvalue
import jsonschema
import fastjsonschema
#validation of data with schema
class Validate(beam.DoFn):
    def __init__(self):
        super(self.__class__, self).__init__()
    def process(self,element,logging_mode,table_name,target_table,project):
        logging.getLogger().setLevel(logging.getLevelName(logging_mode))
        if not hasattr(self,'validate_cm_event_arrival'):
            #initialise the json schema for events event
            self.validate_cm_event_arrival=fastjsonschema.compile(eval('tables_schema.{0}'.format(table_name)))
        if not hasattr(self,'validate_cm_event_state_updates'):
            #initialise the json schema for events event
            self.validate_cm_event_state_updates=fastjsonschema.compile(eval('tables_schema.{0}'.format(table_name)))
        logging.info('Validation filename-----%s',table_name)
        try:
            #json data row
            row=json.loads(element)
            #validation of schema
            validation_name=eval("self.validate_"+table_name)
            validation_name(row)
            logging.info("---------Validation successful for %s",table_name)
            #validation success tag as valid row
            yield pvalue.TaggedOutput('validated_row',element)
        except Exception as e:
            logging.error("ALERT for PROJECT ID : %s  JOB NAME: gcs-pubsub-sql-fdr-ivr %s",project.rsplit(".")[0],str(e))
            exception={}
            exception['input_file_name']=table_name
            exception['target_table_name']=target_table
            exception['error_message']=str(e)
            exception['event_data']=str(row)
            #validation error tag as exception row
            yield  pvalue.TaggedOutput('exception_row',exception)
