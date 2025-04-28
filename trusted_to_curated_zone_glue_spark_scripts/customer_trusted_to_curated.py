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

# Script generated for node Customer trusted data
Customertrusteddata_node1745798447162 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customertrusteddata_node1745798447162")

# Script generated for node Accelerometer trusted data
Accelerometertrusteddata_node1745798507946 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusteddata_node1745798507946")

# Script generated for node SQL Query
SqlQuery1287 = '''
select distinct c.*
from accelerometer_trusted as a
inner join customer_trusted as c on a.user = c.email
;
'''
SQLQuery_node1745798502613 = sparkSqlQuery(glueContext, query = SqlQuery1287, mapping = {"customer_trusted":Customertrusteddata_node1745798447162, "accelerometer_trusted":Accelerometertrusteddata_node1745798507946}, transformation_ctx = "SQLQuery_node1745798502613")

# Script generated for node Customer curated data
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745798502613, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745796585600", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customercurateddata_node1745798611980 = glueContext.getSink(path="s3://udacity-spark-tianchi-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customercurateddata_node1745798611980")
Customercurateddata_node1745798611980.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
Customercurateddata_node1745798611980.setFormat("json")
Customercurateddata_node1745798611980.writeFrame(SQLQuery_node1745798502613)
job.commit()