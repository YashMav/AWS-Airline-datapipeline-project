import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
# import re

args = getResolvedOptions(sys.argv, ['JOB_NAME','file_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
current_date = datetime.date.today()

# Script generated for node s3_input_airlines_data
s3_input_airlines_data_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://"+args['file_name']]}, transformation_ctx="s3_input_airlines_data_node")

# Script generated for node Airports_dim_origin
Airports_dim_origin_node = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-058264222641-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.airports_dim", "connectionName": "Redshift connection 2"}, transformation_ctx="Airports_dim_origin_node")

# Script generated for node Airports_dim_dest
Airports_dim_dest_node = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-058264222641-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.airports_dim", "connectionName": "Redshift connection 2"}, transformation_ctx="Airports_dim_dest_node")

#to add a new column for load_date in the dynamic df
def add_date_col(r):
    r["load_date"] = current_date
    return r

s3_input_airlines_data_node = Map.apply(frame = s3_input_airlines_data_node , f=add_date_col)

#convert dynamicframe to dataframe

s3_df =s3_input_airlines_data_node.toDF()
# s3_df.show()

s3_df = s3_df.select(to_date('Rep_date','yyyyMMdd').alias('Rep_Date'),'Carrier','OriginAirportID','DestAirportID','DepDelay','ArrDelay','load_date')

# convert the DF again to dynamic frame
s3_input_airlines_data_node = DynamicFrame.fromDF(s3_df,glueContext)

# s3_input_airlines_data_node.show()


# Script generated for node Change Schema
ChangeSchema_node = ApplyMapping.apply(frame=s3_input_airlines_data_node, mappings=[("Rep_date","date","Rep_date","date"),("Carrier", "string", "Carrier", "string"), ("OriginAirportID", "string", "OriginAirportID", "int"), ("DestAirportID", "string", "DestAirportID", "int"), ("DepDelay", "string", "DepDelay", "int"), ("ArrDelay", "string", "ArrDelay", "int") ,("load_date","date","load_date","date")], transformation_ctx="ChangeSchema_node")

# Script generated for node Filter
Filter_node = Filter.apply(frame=ChangeSchema_node, f=lambda row: (row["DepDelay"] > 60), transformation_ctx="Filter_node")

# Script generated for node Join_origin
Filter_node_DF = Filter_node.toDF()
Airports_dim_origin_DF = Airports_dim_origin_node.toDF()
Join_origin_node = DynamicFrame.fromDF(Filter_node_DF.join(Airports_dim_origin_DF, (Filter_node_DF['OriginAirportID'] == Airports_dim_origin_DF['airport_id']), "left"), glueContext, "Join_origin_node")

# Script generated for node Change Schema origin
ChangeSchemaorigin_node = ApplyMapping.apply(frame=Join_origin_node, mappings=[("Rep_date","date","Rep_date","date"),("Carrier", "string", "carrier", "string"), ("DestAirportID", "int", "DestAirportID", "int"), ("DepDelay", "int", "dep_Delay", "int"), ("ArrDelay", "int", "arr_Delay", "int"), ("city", "string", "dep_city", "string"), ("state", "string", "dep_state", "string"), ("name", "string", "dep_airport", "string"),("load_date","date","load_date","date")], transformation_ctx="ChangeSchemaorigin_node")

# Script generated for node Join_dest
ChangeSchemaorigin_DF = ChangeSchemaorigin_node.toDF()
Airports_dim_dest_DF = Airports_dim_dest_node.toDF()
Join_dest_node = DynamicFrame.fromDF(ChangeSchemaorigin_DF.join(Airports_dim_dest_DF, (ChangeSchemaorigin_DF['DestAirportID'] == Airports_dim_dest_DF['airport_id']), "left"), glueContext, "Join_dest_node")

# Script generated for node Change Schema dest
ChangeSchemadest_node = ApplyMapping.apply(frame=Join_dest_node, mappings=[("Rep_date","date","Rep_date","date"),("carrier", "string", "carrier", "string"), ("dep_Delay", "int", "dep_delay", "bigint"), ("arr_Delay", "int", "arr_delay", "bigint"), ("dep_city", "string", "dep_city", "string"), ("dep_state", "string", "dep_state", "string"), ("dep_airport", "string", "dep_airport", "string"), ("city", "string", "arr_city", "string"), ("state", "string", "arr_state", "string"), ("name", "string", "arr_airport", "string"),("load_date","date","load_date","date")], transformation_ctx="ChangeSchemadest_node")

# Script generated for node daily_flights_fact
daily_flights_fact_node = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemadest_node, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-058264222641-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.daily_flights_fact", "connectionName": "Redshift connection 2", "preactions": "CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (rep_date date,carrier varchar(10),dep_airport VARCHAR(200),arr_airport VARCHAR(200),dep_city VARCHAR(100),arr_city VARCHAR(100),dep_state VARCHAR(100),arr_state VARCHAR(100),dep_delay BIGINT,arr_delay BIGINT,load_date date) DISTSTYLE KEY DISTKEY (rep_date) sortkey(carrier);"}, transformation_ctx="daily_flights_fact_node")

job.commit()
