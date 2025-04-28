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
Customertrusteddata_node1745797541185 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-spark-tianchi-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="Customertrusteddata_node1745797541185")

# Script generated for node Accelerometer landing data
Accelerometerlandingdata_node1745797403654 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-spark-tianchi-bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlandingdata_node1745797403654")

# Script generated for node Filter for readings from customers who've agreed to share data for research
SqlQuery1338 = '''
select
    a.user
    ,a.timestamp
    ,a.x
    ,a.y
    ,a.z
from accelerometer_landing as a
inner join customer_trusted as c on
    a.user = c.email
    and a.timestamp >= c.sharewithresearchasofdate
;
'''
Filterforreadingsfromcustomerswhoveagreedtosharedataforresearch_node1745797502804 = sparkSqlQuery(glueContext, query = SqlQuery1338, mapping = {"accelerometer_landing":Accelerometerlandingdata_node1745797403654, "customer_trusted":Customertrusteddata_node1745797541185}, transformation_ctx = "Filterforreadingsfromcustomerswhoveagreedtosharedataforresearch_node1745797502804")

# Script generated for node Accelerometer trusted data
EvaluateDataQuality().process_rows(frame=Filterforreadingsfromcustomerswhoveagreedtosharedataforresearch_node1745797502804, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745796585600", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Accelerometertrusteddata_node1745797776600 = glueContext.getSink(path="s3://udacity-spark-tianchi-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusteddata_node1745797776600")
Accelerometertrusteddata_node1745797776600.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Accelerometertrusteddata_node1745797776600.setFormat("json")
Accelerometertrusteddata_node1745797776600.writeFrame(Filterforreadingsfromcustomerswhoveagreedtosharedataforresearch_node1745797502804)
job.commit()