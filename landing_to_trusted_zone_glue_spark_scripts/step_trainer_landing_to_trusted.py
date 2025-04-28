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

# Script generated for node Customer curated data
Customercurateddata_node1745799214973 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="Customercurateddata_node1745799214973")

# Script generated for node Step Trainer landing data
StepTrainerlandingdata_node1745799205421 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerlandingdata_node1745799205421")

# Script generated for node Filter for step trainer data for customers who've agreed to share data for research and have acc data
SqlQuery1300 = '''
select train.*
from step_trainer_landing as train
inner join customer_curated as c on train.serialnumber = c.serialnumber
;
'''
Filterforsteptrainerdataforcustomerswhoveagreedtosharedataforresearchandhaveaccdata_node1745799241752 = sparkSqlQuery(glueContext, query = SqlQuery1300, mapping = {"step_trainer_landing":StepTrainerlandingdata_node1745799205421, "customer_curated":Customercurateddata_node1745799214973}, transformation_ctx = "Filterforsteptrainerdataforcustomerswhoveagreedtosharedataforresearchandhaveaccdata_node1745799241752")

# Script generated for node Step Trainer trusted data
EvaluateDataQuality().process_rows(frame=Filterforsteptrainerdataforcustomerswhoveagreedtosharedataforresearchandhaveaccdata_node1745799241752, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745796585600", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainertrusteddata_node1745799434910 = glueContext.getSink(path="s3://udacity-spark-tianchi-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainertrusteddata_node1745799434910")
StepTrainertrusteddata_node1745799434910.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainertrusteddata_node1745799434910.setFormat("json")
StepTrainertrusteddata_node1745799434910.writeFrame(Filterforsteptrainerdataforcustomerswhoveagreedtosharedataforresearchandhaveaccdata_node1745799241752)
job.commit()