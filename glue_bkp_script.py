import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME','file_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node s3_input_airlines_data
s3_input_airlines_data_node1720529582718 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://"+args['file_name']]}, transformation_ctx="s3_input_airlines_data_node1720529582718")

# Script generated for node Airports_dim_origin
Airports_dim_origin_node1720532675223 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-058264222641-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.airports_dim", "connectionName": "Redshift connection 2"}, transformation_ctx="Airports_dim_origin_node1720532675223")

# Script generated for node Airports_dim_dest
Airports_dim_dest_node1720538085850 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-058264222641-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.airports_dim", "connectionName": "Redshift connection 2"}, transformation_ctx="Airports_dim_dest_node1720538085850")

# Script generated for node Change Schema
ChangeSchema_node1720530250873 = ApplyMapping.apply(frame=s3_input_airlines_data_node1720529582718, mappings=[("Carrier", "string", "Carrier", "string"), ("OriginAirportID", "string", "OriginAirportID", "int"), ("DestAirportID", "string", "DestAirportID", "int"), ("DepDelay", "string", "DepDelay", "int"), ("ArrDelay", "string", "ArrDelay", "int")], transformation_ctx="ChangeSchema_node1720530250873")

# Script generated for node Filter
Filter_node1720530204051 = Filter.apply(frame=ChangeSchema_node1720530250873, f=lambda row: (row["DepDelay"] > 60), transformation_ctx="Filter_node1720530204051")

# Script generated for node Join_origin
Filter_node1720530204051DF = Filter_node1720530204051.toDF()
Airports_dim_origin_node1720532675223DF = Airports_dim_origin_node1720532675223.toDF()
Join_origin_node1720532712778 = DynamicFrame.fromDF(Filter_node1720530204051DF.join(Airports_dim_origin_node1720532675223DF, (Filter_node1720530204051DF['OriginAirportID'] == Airports_dim_origin_node1720532675223DF['airport_id']), "left"), glueContext, "Join_origin_node1720532712778")

# Script generated for node Change Schema origin
ChangeSchemaorigin_node1720533711171 = ApplyMapping.apply(frame=Join_origin_node1720532712778, mappings=[("Carrier", "string", "carrier", "string"), ("DestAirportID", "int", "DestAirportID", "int"), ("DepDelay", "int", "dep_Delay", "int"), ("ArrDelay", "int", "arr_Delay", "int"), ("city", "string", "dep_city", "string"), ("state", "string", "dep_state", "string"), ("name", "string", "dep_airport", "string")], transformation_ctx="ChangeSchemaorigin_node1720533711171")

# Script generated for node Join_dest
ChangeSchemaorigin_node1720533711171DF = ChangeSchemaorigin_node1720533711171.toDF()
Airports_dim_dest_node1720538085850DF = Airports_dim_dest_node1720538085850.toDF()
Join_dest_node1720538127505 = DynamicFrame.fromDF(ChangeSchemaorigin_node1720533711171DF.join(Airports_dim_dest_node1720538085850DF, (ChangeSchemaorigin_node1720533711171DF['DestAirportID'] == Airports_dim_dest_node1720538085850DF['airport_id']), "left"), glueContext, "Join_dest_node1720538127505")

# Script generated for node Change Schema dest
ChangeSchemadest_node1720538173775 = ApplyMapping.apply(frame=Join_dest_node1720538127505, mappings=[("carrier", "string", "carrier", "string"), ("dep_Delay", "int", "dep_delay", "bigint"), ("arr_Delay", "int", "arr_delay", "bigint"), ("dep_city", "string", "dep_city", "string"), ("dep_state", "string", "dep_state", "string"), ("dep_airport", "string", "dep_airport", "string"), ("city", "string", "arr_city", "string"), ("state", "string", "arr_state", "string"), ("name", "string", "arr_airport", "string")], transformation_ctx="ChangeSchemadest_node1720538173775")

# Script generated for node daily_flights_fact
daily_flights_fact_node1720538432811 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemadest_node1720538173775, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-058264222641-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.daily_flights_fact", "connectionName": "Redshift connection 2", "preactions": "CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (carrier VARCHAR, dep_delay VARCHAR, arr_delay VARCHAR, dep_city VARCHAR, dep_state VARCHAR, dep_airport VARCHAR, arr_city VARCHAR, arr_state VARCHAR, arr_airport VARCHAR);"}, transformation_ctx="daily_flights_fact_node1720538432811")

job.commit()
