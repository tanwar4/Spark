# Makefile for MapReduce final project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/usr/local/spark
jar.name=MRProject-1.0.jar
jar.path=target/${jar.name}
job.name=Training
local.input=input
local.output=output
local.model=model
local.logs=logs
# Pseudo-Cluster Execution
hdfs.user.name=yasi
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.3.1
aws.region=us-east-1
aws.bucket.name=yasiahrabi-bucket
aws.subnet.id=subnet-913976bc
aws.input=input
aws.output=outputPredict
aws.model=RF_DEPTH25_TREES20
aws.finaloutput=outputPredict
aws.log.dir=log
aws.num.nodes=6	
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Removes local logs directory.
clean-local-logs:
	rm -rf ${local.logs}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.input} ${local.output} ${local.model} spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///tmp/test

# Start HDFS
start-hdfs:
	${spark.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${spark.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${spark.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${spark.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${spark.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${spark.root}/bin/hdfs dfs -rm -r -f /user
	${spark.root}/bin/hdfs dfs -mkdir /user
	${spark.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${spark.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${spark.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${spark.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output:
	mkdir ${local.output}
	${spark.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${spark.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${spark.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "MR Project Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Spark \
	    --steps '[{"Name":"Spark PR Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}", "s3://${aws.bucket.name}/${aws.model}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes  KeyName="mr_project",InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--configurations '[{"Classification":"spark", "Properties":{"maximizeResourceAllocation": "true"}}]' \
		--region ${aws.region} \
		--enable-debugging
# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.finaloutput} ${local.output}

download-logs-aws: clean-local-logs
	mkdir ${local.logs}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir} ${local.logs}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${spark.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${spark.root}/etc/hadoop

# Package for release.
distro:
	rm -rf build
	mkdir build
	mkdir build/deliv
	mkdir build/deliv/PageRank
	cp pom.xml build/deliv/PageRank
	cp -r src build/deliv/PageRank
	cp Makefile build/deliv/PageRank
	cp README.txt build/deliv/PageRank
	tar -czf PageRank.tar.gz -C build/deliv PageRank
	cd build/deliv && zip -rq ../../PageRank.zip PageRank
	