import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Sanitize Customer Data
SanitizeCustomerData_node2 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="SanitizeCustomerData_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678198160883 = DynamicFrame.fromDF(
    SanitizeCustomerData_node2.toDF().dropDuplicates(
        [
            "customerName",
            "email",
            "phone",
            "birthDay",
            "serialNumber",
            "registrationDate",
            "lastUpdateDate",
            "shareWithResearchAsOfDate",
            "shareWithPublicAsOfDate",
            "shareWithFriendsAsOfDate",
        ]
    ),
    glueContext,
    "DropDuplicates_node1678198160883",
)

# Script generated for node Customer Trusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678198160883,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/customers/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
