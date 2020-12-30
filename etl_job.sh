#!/bin/bash

aws emr create-cluster --name spark \
	--use-default-roles \
	--release-label emr-5.30.0 \
	--instance-count 3  \
	--applications Name=Spark Name=Hadoop \
	--ec2-attributes KeyName=spark-cluster,SubnetIds=subnet-69e6c242 \
	--instance-type m5.xlarge \
	--steps Type=Spark,Name="Sparkify ETL",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3://sparkify-dl-sp/sparkify-scripts/spark-etl.py] \
	--auto-terminate
