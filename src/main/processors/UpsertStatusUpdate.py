from processors import sqlconnection
import apache_beam as beam
import argparse
import logging
import sqlalchemy
import json
from apache_beam import pvalue
class UpsertStatusUpdate(beam.DoFn):
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
            logging.info('created pool for updates')
    def process(self,element,logging_mode,table_name,sql_instance,username,db_name):
        #adding logs
        logging.getLogger().setLevel(logging.getLevelName(self.logging_mode))
        #json row for events status
        row=json.loads(element)
        #upsert basis on the statemachineid
        if row['type'] == 'event_status_transfers':
            event_status_transfers=row
            logging.info(" starting connection : ---------")
            #comparing identifier to upsert only latest record
            select_stmt="""select case when alert_update_timestamp > :alert_update_timestamp then 'exist' else 'no_exist' end as result \
            from (select '1' as com) common \
            left join \
            (select '1' as com, alert_update_timestamp from $table_name where alert_entity_value = :alert_entity_value) ivr \
            on common.com = ivr.com;""".replace('$table_name',self.table_name)
            
            #preparing upsert statement

            upsert_statement="""insert into $table_name (source,alert_id,alert_type,customer_id,alert_entity_value,customer_portfolio_region,customer_portfolio_product,\
                customer_portfolio_channel,customer_portfolio_country,customer_portfolio_type,customer_portfolio_class,alert_fraud_state,\
               alert_state_reason,alert_update_timestamp) \
                values(:source,:alert_id,:alert_type,:customer_id,:alert_entity_value,:customer_portfolio_region,:customer_portfolio_product,\
                :customer_portfolio_channel,:customer_portfolio_country,:customer_portfolio_type,:customer_portfolio_class,:alert_fraud_state,\
                :alert_state_reason,:alert_update_timestamp)\
                ON CONFLICT \
                (alert_entity_value) do update set\
                source=EXCLUDED.source,\
                alert_id=EXCLUDED.alert_id,\
                alert_type=EXCLUDED.alert_type,\
                customer_id=EXCLUDED.customer_id,\
                alert_entity_value=EXCLUDED.alert_entity_value,\
                alert_fraud_state=EXCLUDED.alert_fraud_state,\
                alert_state_reason=EXCLUDED.alert_state_reason,\
                alert_update_timestamp=EXCLUDED.alert_update_timestamp, \
                updatedtimestamp=EXCLUDED.updatedtimestamp ;""".replace('$table_name',self.table_name)
            #making connection with pool and replacing variables for upser
            with self.mypool.connect() as db_conn:
                results=db_conn.execute(sqlalchemy.text(select_stmt),parameters={"alert_update_timestamp":event_status_transfers['alert_update_timestamp'],"alert_entity_value":event_status_transfers['alert_entity_value']}).fetchall()
                logging.info(' fetched data type ----------%s',type(results))
                logging.info(' fetched data len ----------%s',len(results))

                if results[0][0]=='no_exist':
                    logging.info("No macthed Id --insert direct----------")
                    db_conn.execute(sqlalchemy.text(upsert_statement),parameters={
                    "source":event_status_transfers["source"],
                    "alert_id":event_status_transfers['alert_id'],
                    "alert_type":event_status_transfers['alert_type'],
                    "customer_id":event_status_transfers['customer_id'],
                    "alert_entity_value":event_status_transfers['alert_entity_value'],
                    "customer_portfolio_region":event_status_transfers['customer_portfolio_region'],
                    "customer_portfolio_product":event_status_transfers['customer_portfolio_product'],
                    "customer_portfolio_channel":event_status_transfers['customer_portfolio_channel'],
                    "customer_portfolio_country":event_status_transfers['customer_portfolio_country'],
                    "customer_portfolio_type":event_status_transfers['customer_portfolio_type'],
                    "customer_portfolio_class":event_status_transfers['customer_portfolio_class'],
                    "alert_fraud_state":event_status_transfers['alert_fraud_state'],
                    "alert_state_reason":event_status_transfers['alert_state_reason'],
                    "alert_update_timestamp":event_status_transfers['alert_update_timestamp']})
                    #commiting to db after all processing
                    db_conn.commit()
                    logging.info(" connection closing for updates: ---------")
                    #db_conn.close()
                else:
                    logging.info("not required --- record with greater timestamp already present")
                    #db_conn.close()

            
        elif row['type'] == 'event_operational_status':

            event_operational_status=row
            logging.info(" starting connection : ---------")
            #comparing identifier to upsert only latest record
            select_stmt="""select case when alert_update_timestamp > :alert_update_timestamp then 'exist' else 'no_exist' end as result \
            from (select '1' as com) common \
            left join \
            (select '1' as com, alert_update_timestamp from $table_name where alert_entity_value = :alert_entity_value) ivr \
            on common.com = ivr.com;""".replace('$table_name',self.table_name)
            
            #preparing upsert statement
            upsert_statement="""insert into $table_name (source,alert_id,alert_type,customer_id,alert_entity_value,customer_portfolio_region,customer_portfolio_product,\
                customer_portfolio_channel,customer_portfolio_country,customer_portfolio_type,customer_portfolio_class,alert_operational_state,\
                alert_state_reason,alert_update_timestamp) \
                values(:source,:alert_id,:alert_type,:customer_id,:alert_entity_value,:customer_portfolio_region,:customer_portfolio_product,\
                :customer_portfolio_channel,:customer_portfolio_country,:customer_portfolio_type,:customer_portfolio_class,:alert_operational_state,\
                :alert_state_reason,:alert_update_timestamp)\
                ON CONFLICT \
                (alert_entity_value) do update set\
                source=EXCLUDED.source,\
               alert_operational_state=EXCLUDED.alert_operational_state,\
                alert_state_reason=EXCLUDED.alert_state_reason,\
                alert_update_timestamp=EXCLUDED.alert_update_timestamp, \
                updatedtimestamp=EXCLUDED.updatedtimestamp ;""".replace('$table_name',self.table_name)
            #making connection with pool and replacing variables for upsert
            with self.mypool.connect() as db_conn:
                results=db_conn.execute(sqlalchemy.text(select_stmt),parameters={"alert_update_timestamp":event_operational_status['alert_update_timestamp'],"alert_entity_value":event_operational_status['alert_entity_value']}).fetchall()
                logging.info(' fetched data type ----------%s',type(results))
                logging.info(' fetched data len ----------%s',len(results))

                if results[0][0]=='no_exist':
                    logging.info("No macthed Id --insert direct----------")
                    db_conn.execute(sqlalchemy.text(upsert_statement),parameters={
                    "source":event_operational_status["source"],
                    "alert_id":event_operational_status['alert_id'],
                    "alert_type":event_operational_status['alert_type'],
                    "customer_id":event_operational_status['customer_id'],
                    "customer_portfolio_class":event_operational_status['customer_portfolio_class'],
                    "alert_operational_state":event_operational_status['alert_operational_state'],
                    "alert_state_reason":event_operational_status['alert_state_reason'],
                    "alert_update_timestamp":event_operational_status['alert_update_timestamp']})
                    
                    #commiting to db after all processing
                    db_conn.commit()
                    logging.info(" connection closing for updates: ---------")
                    #db_conn.close()
                else:
                    logging.info("not required --- record with greater timestamp already present")
        else:
            logging.info('record not valid for upsert')
    def finish_bundle(self):
        #flushing pool after bundle processing
        if self.mypool:
            self.mypool.dispose()
