import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer Curated
CustomerCurated_node1766968490341 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1766968490341")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1766968488707 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1766968488707")

# Script generated for node SQL Query
SqlQuery1467 = '''
select step_trainer_landing.serialnumber, distancefromobject, sensorreadingtime  
from step_trainer_landing
inner join customer_curated on customer_curated.serialnumber = step_trainer_landing.serialnumber
'''
SQLQuery_node1766986439311 = sparkSqlQuery(glueContext, query = SqlQuery1467, mapping = {"customer_curated":CustomerCurated_node1766968490341, "step_trainer_landing":StepTrainerLanding_node1766968488707}, transformation_ctx = "SQLQuery_node1766986439311")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1766986439311, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766968280842", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1766968497241 = glueContext.getSink(path="s3://spark-877687662547-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1766968497241")
StepTrainerTrusted_node1766968497241.setCatalogInfo(catalogDatabase="lakehouse_db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1766968497241.setFormat("json")
StepTrainerTrusted_node1766968497241.writeFrame(SQLQuery_node1766986439311)
job.commit()