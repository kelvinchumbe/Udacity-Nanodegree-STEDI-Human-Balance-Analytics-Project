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

# Script generated for node Customer Trusted
CustomerTrusted_node1678199086905 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1678199086905",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Drop Duplicate Accelerometer
DropDuplicateAccelerometer_node1678264144483 = DynamicFrame.fromDF(
    AccelerometerLanding_node1.toDF().dropDuplicates(["user", "timeStamp", "x", "y"]),
    glueContext,
    "DropDuplicateAccelerometer_node1678264144483",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=CustomerTrusted_node1678199086905,
    frame2=DropDuplicateAccelerometer_node1678264144483,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1678199161513 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "sharewithpublicasofdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1678199161513",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678253595336 = DynamicFrame.fromDF(
    DropFields_node1678199161513.toDF().dropDuplicates(["user", "timeStamp", "y", "x"]),
    glueContext,
    "DropDuplicates_node1678253595336",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678253595336,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
