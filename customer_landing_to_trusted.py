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

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1764545788286 = glueContext.create_dynamic_frame.from_catalog(database="lakehouse_db", table_name="customer_landing", transformation_ctx="CustomerLandingZone_node1764545788286")

# Script generated for node Customer Filter
SqlQuery1265 = '''
select * from customer_landing 
where sharewithresearchasofdate is not null;
'''
CustomerFilter_node1766885200791 = sparkSqlQuery(glueContext, query = SqlQuery1265, mapping = {"customer_landing":CustomerLandingZone_node1764545788286}, transformation_ctx = "CustomerFilter_node1766885200791")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=CustomerFilter_node1766885200791, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1764545672152", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1764546064508 = glueContext.getSink(path="s3://spark-877687662547-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1764546064508")
TrustedCustomerZone_node1764546064508.setCatalogInfo(catalogDatabase="lakehouse_db",catalogTableName="customer_trusted")
TrustedCustomerZone_node1764546064508.setFormat("json")
TrustedCustomerZone_node1764546064508.writeFrame(CustomerFilter_node1766885200791)
job.commit()