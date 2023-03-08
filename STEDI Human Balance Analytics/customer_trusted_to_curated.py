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
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1678255280149 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1678255280149",
)

# Script generated for node Drop Duplicate Customers
DropDuplicateCustomers_node1678261944131 = DynamicFrame.fromDF(
    CustomerTrusted_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicateCustomers_node1678261944131",
)

# Script generated for node Drop Duplicate Accelerometer
DropDuplicateAccelerometer_node1678262089286 = DynamicFrame.fromDF(
    AccelerometerTrusted_node1678255280149.toDF().dropDuplicates(["user"]),
    glueContext,
    "DropDuplicateAccelerometer_node1678262089286",
)

# Script generated for node Join Accelerometer
JoinAccelerometer_node2 = Join.apply(
    frame1=DropDuplicateCustomers_node1678261944131,
    frame2=DropDuplicateAccelerometer_node1678262089286,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinAccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1678256433321 = DropFields.apply(
    frame=JoinAccelerometer_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1678256433321",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678255695487 = DynamicFrame.fromDF(
    DropFields_node1678256433321.toDF().dropDuplicates(
        [
            "serialNumber",
            "shareWithPublicAsOfDate",
            "birthDay",
            "registrationDate",
            "shareWithResearchAsOfDate",
            "customerName",
            "email",
            "lastUpdateDate",
            "phone",
            "shareWithFriendsAsOfDate",
        ]
    ),
    glueContext,
    "DropDuplicates_node1678255695487",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678255695487,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
