from google.cloud.sql.connector import Connector,IPTypes
import sqlalchemy
from sqlalchemy.pool import QueuePool
connector=Connector()

def getpool(sql_instance,username,db_name):
    #connecting with iam enabled
    def getconn():
        conn=connector.connect(
            sql_instance,
            "pg8000",
            user=username,
            db=db_name,
            enable_iam_auth=True,
            ip_type=IPTypes.PRIVATE
        )
        return conn
    pool=sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
    pool_size=1,
    max_overflow=10,
    pool_timeout=30,
    poolclass=QueuePool
    )
    return pool
#parameters passed for pool connection -timeout,max size of pool,class
