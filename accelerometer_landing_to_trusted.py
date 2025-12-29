import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1766880474061 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1766880474061")

# Script generated for node Customer Trusted
CustomerTrusted_node1766880433558 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1766880433558")

# Script generated for node Change Schema
ChangeSchema_node1766960197315 = ApplyMapping.apply(frame=CustomerTrusted_node1766880433558, mappings=[("customername", "string", "customerName", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthDay", "string"), ("serialnumber", "string", "serialNumber", "string"), ("registrationdate", "long", "registrationDate", "bigint"), ("lastupdatedate", "long", "lastUpdateDate", "bigint"), ("sharewithresearchasofdate", "long", "shareWithResearchAsOfDate", "bigint"), ("sharewithpublicasofdate", "long", "shareWithPublicAsOfDate", "bigint"), ("sharewithfriendsasofdate", "long", "shareWithFriendsAsOfDate", "bigint")], transformation_ctx="ChangeSchema_node1766960197315")

# Script generated for node Join
Join_node1766958554174 = Join.apply(frame1=AccelerometerLanding_node1766880474061, frame2=ChangeSchema_node1766960197315, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1766958554174")

# Script generated for node Drop Fields
DropFields_node1766958607211 = DropFields.apply(frame=Join_node1766958554174, paths=["sharewithfriendsasofdate", "sharewithpublicasofdate", "sharewithresearchasofdate", "registrationdate", "lastupdatedate", "serialnumber", "birthday", "phone", "email", "customername", "birthDay", "serialNumber", "customerName", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1766958607211")

# Script generated for node Change Schema
ChangeSchema_node1766960586222 = ApplyMapping.apply(frame=DropFields_node1766958607211, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "bigint"), ("x", "double", "x", "float"), ("y", "double", "y", "float"), ("z", "double", "z", "float")], transformation_ctx="ChangeSchema_node1766960586222")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1766960586222, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766958210074", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1766958674630 = glueContext.getSink(path="s3://spark-877687662547-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1766958674630")
AccelerometerTrusted_node1766958674630.setCatalogInfo(catalogDatabase="lakehouse_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1766958674630.setFormat("json")
AccelerometerTrusted_node1766958674630.writeFrame(ChangeSchema_node1766960586222)
job.commit()