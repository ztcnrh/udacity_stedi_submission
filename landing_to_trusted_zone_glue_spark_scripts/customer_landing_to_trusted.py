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

# Script generated for node Customer landing data
Customerlandingdata_node1745796682973 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-spark-tianchi-bucket/customer/landing/"], "recurse": True}, transformation_ctx="Customerlandingdata_node1745796682973")

# Script generated for node Filter for non-null sharewithresearchasofdate
SqlQuery1356 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
Filterfornonnullsharewithresearchasofdate_node1745796745916 = sparkSqlQuery(glueContext, query = SqlQuery1356, mapping = {"myDataSource":Customerlandingdata_node1745796682973}, transformation_ctx = "Filterfornonnullsharewithresearchasofdate_node1745796745916")

# Script generated for node Customer trusted data
EvaluateDataQuality().process_rows(frame=Filterfornonnullsharewithresearchasofdate_node1745796745916, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745796585600", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customertrusteddata_node1745796815358 = glueContext.getSink(path="s3://udacity-spark-tianchi-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customertrusteddata_node1745796815358")
Customertrusteddata_node1745796815358.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
Customertrusteddata_node1745796815358.setFormat("json")
Customertrusteddata_node1745796815358.writeFrame(Filterfornonnullsharewithresearchasofdate_node1745796745916)
job.commit()