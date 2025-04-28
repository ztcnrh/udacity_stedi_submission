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

# Script generated for node Accelerometer trusted data
Accelerometertrusteddata_node1745800298049 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusteddata_node1745800298049")

# Script generated for node Step Trainer trusted data
StepTrainertrusteddata_node1745800263323 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainertrusteddata_node1745800263323")

# Script generated for node Join step trainer data with acc data on timestamp
SqlQuery1344 = '''
select train.*, a.*
from step_trainer_trusted as train
inner join accelerometer_trusted as a on train.sensorreadingtime = a.timestamp
;
'''
Joinsteptrainerdatawithaccdataontimestamp_node1745800312806 = sparkSqlQuery(glueContext, query = SqlQuery1344, mapping = {"step_trainer_trusted":StepTrainertrusteddata_node1745800263323, "accelerometer_trusted":Accelerometertrusteddata_node1745800298049}, transformation_ctx = "Joinsteptrainerdatawithaccdataontimestamp_node1745800312806")

# Script generated for node Machine learning curated data
EvaluateDataQuality().process_rows(frame=Joinsteptrainerdatawithaccdataontimestamp_node1745800312806, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745796585600", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Machinelearningcurateddata_node1745800373736 = glueContext.getSink(path="s3://udacity-spark-tianchi-bucket/step_trainer/ml/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Machinelearningcurateddata_node1745800373736")
Machinelearningcurateddata_node1745800373736.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
Machinelearningcurateddata_node1745800373736.setFormat("json")
Machinelearningcurateddata_node1745800373736.writeFrame(Joinsteptrainerdatawithaccdataontimestamp_node1745800312806)
job.commit()