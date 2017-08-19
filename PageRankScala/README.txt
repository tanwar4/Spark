###################################################
  
  BUILD AND RUN
####################################################

Create the jar from the src file provided 
		jar cf assign4.jar src
				
Customize the variable in the Makefile according to your environment before running it.


job.name variable for the program are below:
PageRank :     cs6240.pagerank.PageRank


###############################
Commands to run on cloud:
###############################
Make sure to update AWS-related variables
 make upload-input-aws Makefile
 make cloud Makefile
 
Once the job is finished succesfully download the output
 
Make download-output-aws Makefile

###############################
Running Standalone
###############################
make sure to update hdfs and system related variables

make pseudo Makefile 
