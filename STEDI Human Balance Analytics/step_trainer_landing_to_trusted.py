import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step-Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1678265701209 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1678265701209",
)

# Script generated for node Drop Duplicates Step-Trainer
DropDuplicatesStepTrainer_node1678265739259 = DynamicFrame.fromDF(
    StepTrainerLanding_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicatesStepTrainer_node1678265739259",
)

# Script generated for node Join Customer
JoinCustomer_node1678266033110 = Join.apply(
    frame1=DropDuplicatesStepTrainer_node1678265739259,
    frame2=CustomerCurated_node1678265701209,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="JoinCustomer_node1678266033110",
)

# Script generated for node Drop Fields
DropFields_node1678266422670 = DropFields.apply(
    frame=JoinCustomer_node1678266033110,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1678266422670",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678266493800 = DynamicFrame.fromDF(
    DropFields_node1678266422670.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678266493800",
)

# Script generated for node Step-Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678266493800,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
