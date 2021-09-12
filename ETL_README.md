# HelloFresh Data Engineering Test
This document contains brief explanation of the approach, data exploration and assumptions/considerations.

We are creating common dataframe using all 3 input files at once and cleaning & replacing newline character in few columns. 
For further transformation we require only those records that have beef in it as receipe. Thus we keep those records that conayins 'beef' in its ingrdients.
This would create a structured dataframe from which we extract cooking time and prep time duration and calculate total cooking time in minutes.
As per the cooking time we decide the difficulty and then do a group by on difficulty to find average cooking time for each difficulty.


##Input file
We are getting input file in json format. All the rows present in input file are single line JSON objects which has 9 key-value pairs.
There are no array or nested fields present.
I am assuming that there would not ba any nested fields and input would be in same format.
All the input json files are present in "input" folder.


##Utilities
###COMMON_UTILS
This contains common spark services that could be leveraged in the project for logging, path creation using python function, extraction or writing data.
####create_spark_session
This function is used to create spark session which would be used to communicate with all spark components

####write_to_csv
As per task assigned to us, we need to save the final output dataset as csv in output folder. This program is used to save dataframe as csv with given filename.
This function renames part file with the given input and delete other files in that folder.
We can change this function to save data as parquet file as per the actual project.

####extract_data
This function extracts data from json file input path and create dataframe on top of it

####create_file_path
This function is not used in the project but could be used by us in the actual project to create file path as per the execution date of the job as it be dynamic file path in that case.
But for this test we have considered file path as static in input folder.

###ETL_JOB
####pre_process_data
This function is used to pre-process input dataframe and remove newline characters from ingredients, receipeYeild and description column.
Here we have considered only 3 columns but we can make it dynamic by passing list of column on which this function needs to implemented by using python reduce.

`
for col_name in [list of columns]:`

`
actual_df = actual_df.withColumn(col_name, <function to be implemented>))
`

####data transformation
We are using `filter_data` function to filter records containing `beef` as ingredient in it.

Post filteration we are transforming the data as per the requirement using `transform_data`.
This function extracts duration from cookTime and prepTime to calculate total cooking time for the receipe. While extracting time if its null, to handle it we use coalesce function to put default value as 0, if its null.
    Difficulty(easy, medium, hard) for the receipe is calculated using total cooking time for the receipe.
    Average cooking time for each difficulty level is calculated finally.


Criteria for difficulty levels based on total cook time duration:
- easy - less than 30 mins
- medium - between 30 and 60 mins
- hard - more than 60 mins.


## Assumptions/Consideration:
We are assuming that we would be getting input file in the same format and there are no schema changes in json file.

Input path we have hardcoded as per the test but could changed and `create_file_path` function could be used to create path when we would be reading data from s3 buckets using `boto3` library.

Newline characters `\r\n` are the only junk characters present in the json file.

For unit testing, I have only considered number of column and rows check. Final dataframe value checks could also be added if final result is known.

##CI/CD
For CI/CD, we can link our local IDEs or notebooks with github or any other code repository. Where development could be done in feature branches and post testing and review, we can raise pull requests and merge the feature branch with master branch

##Important Points to consider
- Incase we are facing slowness in spark job, we need to tune number of executors, memory to each executor, number of cores to parallelize the whole process and speed it up.
- To speed up read of json files, we should avoid using inferSchema as true and enforce schema on json read.
- To schedule this pipeline, we could use UC4 scheduler if we are using CDH/HDP cluster. Step function, lambda function & cloudwatch could be used to orchestrate and schedule the job in AWS. ADF service could be used in Azure cloud.
- We can either schedule them daily at a particular time or we can use event base trigger i.e. whenever we receive json files in our buckets/cloud storage, jobs would be triggered.

#Note:
I have used virtual environment for the first time for development on personal system and code is running successfully on my system.
Do let me know if there are any issues while running the code on your system. I have been using Databricks notebook for development from last few years.

Please let me know if there are any feedbacks.

Thanks!



