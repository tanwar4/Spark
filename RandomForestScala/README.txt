1- Unzip the Project.zip into a directory (CUR_DIR)
* for Tuning program:
   - Copy the labeled.csv.bz2 file into project's input/ folder
   - Change the job.name in Makefile to Tuning
* for Training program: ( final model with trees = 20)
   - Copy the labeled.csv.bz2 file into project's input/ folder
   - Change the job.name in Makefile to Training
* for Prediction program:
   - make sure the model is placed in the model/ folder
   - remove the labeled.csv.bz2 file from project's input/ folder
   - Copy the unlabeled.csv.bz2 file into project's input/ folder
   - Change the job.name in Makefile to Predict
- the model is saved in model/ folder
- the output of prediction is saved in the output/predict folder

3- To run:
	- $ cd project_dir
	- customize the parameters in Makefile
	- To run in standalone mode:
		- $ make switch-standalone
		- $ make alone
	- To run in psuedo-distributed mode for the first time:
		- $ make switch-psuedo
		- $ make psuedo
	- To run in psuedo-distributed mode after the first time:
		- $ make psuedoq
	- To run in cloud mode:
		* first customize the parameters in Makefile according to your AWS account
		- $ make upload-input-aws
		- $ make cloud
		- wait for cluster to terminate by checking its status via the following command:
		-  $ aws emr list-clusters --cluster-states TERMINATED
		- once terminated:
			- to copy the output files from s3 to local directory:
			-  $ make download-output-aws
			- to copy the log files from s3 to local directory:
			-  $ make download-logs-aws

	- The output for all modes will be visible in PROJECT_DIR/output