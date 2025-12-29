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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1766988029330 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1766988029330")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1766988031279 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1766988031279")

# Script generated for node Machine Learning Curator
SqlQuery1394 = '''
select accelerometer_trusted.timestamp as timestamp, user, x, y, z, serialnumber, distancefromobject from step_trainer_trusted
inner join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime;
'''
MachineLearningCurator_node1766988037631 = sparkSqlQuery(glueContext, query = SqlQuery1394, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1766988031279, "accelerometer_trusted":AccelerometerTrusted_node1766988029330}, transformation_ctx = "MachineLearningCurator_node1766988037631")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=MachineLearningCurator_node1766988037631, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766985864650", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1766988041113 = glueContext.getSink(path="s3://spark-877687662547-bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1766988041113")
MachineLearningCurated_node1766988041113.setCatalogInfo(catalogDatabase="lakehouse_db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1766988041113.setFormat("json")
MachineLearningCurated_node1766988041113.writeFrame(MachineLearningCurator_node1766988037631)
job.commit()