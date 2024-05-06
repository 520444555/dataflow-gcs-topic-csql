from processors import sqlconnection
import apache_beam as beam
import argparse
import logging
import sqlalchemy
import json
from apache_beam import pvalue
class UpsertEventArrival(beam.DoFn):
    def __init__(self,logging_mode,table_name,sql_instance,username,db_name):
        #intializing sql db instance
        self.logging_mode=logging_mode
        self.table_name=table_name
        self.username=username
        self.sql_instance=sql_instance
        self.db_name=db_name
        super(self.__class__, self).__init__()
    def start_bundle(self):
        if not hasattr(self, 'mypool'):
            logging.getLogger().setLevel(logging.getLevelName(self.logging_mode))
            #creating sql connection pool
            self.mypool=sqlconnection.getpool(self.sql_instance,self.username,self.db_name)
            logging.info('created pool for arrival')
    def process(self, element,logging_mode,table_name,sql_instance,username,db_name):
        logging.getLogger().setLevel(logging.getLevelName(logging_mode))
        #json for events event 
        event_arrival=json.loads(element)
        #adding logs
        logging.info(" starting connection : ---------")
        
        #preparing upsert statement

        upsert_statement="""insert into $table_name (source,alert_id,alert_type,customer_id,alert_entity_value,customer_portfolio_region,customer_portfolio_product,\
        customer_portfolio_channel,customer_portfolio_country,customer_portfolio_type,customer_portfolio_class,alert_create_timestamp) \
        values (:source,:alert_id,:alert_type,:customer_id,:alert_entity_value,:customer_portfolio_region,:customer_portfolio_product,\
        :customer_portfolio_channel,:customer_portfolio_country,:customer_portfolio_type,:customer_portfolio_class,:alert_create_timestamp) \
        ON CONFLICT \
        (alert_entity_value) do update set \
        source=EXCLUDED.source,\
        alert_id=EXCLUDED.alert_id,\
        alert_type=EXCLUDED.alert_type,\
        customer_id=EXCLUDED.customer_id,\
        alert_entity_value=EXCLUDED.alert_entity_value,\
        customer_portfolio_region=EXCLUDED.customer_portfolio_region,\
        customer_portfolio_product=EXCLUDED.customer_portfolio_product,\
        customer_portfolio_channel=EXCLUDED.customer_portfolio_channel,\
        customer_portfolio_country=EXCLUDED.customer_portfolio_country,\
        customer_portfolio_type=EXCLUDED.customer_portfolio_type,\
        customer_portfolio_class=EXCLUDED.customer_portfolio_class,\
        alert_create_timestamp=EXCLUDED.alert_create_timestamp ,\
        updatedtimestamp=EXCLUDED.updatedtimestamp;""".replace('$table_name',table_name)
        
        #making connection with pool and replacing variables for upsert
        with self.mypool.connect() as db_conn:
            db_conn.execute(sqlalchemy.text(upsert_statement),parameters={
            "source":event_arrival["source"],
            "alert_id":event_arrival['alert_id'],
            "alert_type":event_arrival["alert_type"],
            "customer_id":event_arrival["customer_id"],
            "alert_entity_value":event_arrival["alert_entity_value"],
            "customer_portfolio_region":event_arrival["customer_portfolio_region"],
            "customer_portfolio_product":event_arrival['customer_portfolio_product'],
            "customer_portfolio_channel":event_arrival['customer_portfolio_channel'],
            "customer_portfolio_country":event_arrival['customer_portfolio_country'],
            "customer_portfolio_type":event_arrival['customer_portfolio_type'],
            "customer_portfolio_class":event_arrival['customer_portfolio_class'],
            "alert_create_timestamp":event_arrival["alert_create_timestamp"]})
            
            #commiting to db after all processing

            db_conn.commit()
            logging.info(" connection closing for event: ---------")
    def finish_bundle(self):
        #flushing pool after bundle processing
        if self.mypool:
            self.mypool.dispose()
