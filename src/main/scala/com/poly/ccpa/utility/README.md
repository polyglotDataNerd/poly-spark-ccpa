# Spark Utilities

Common utilities that will be used within the CCPA and Anonymizer application. These objects arr written for and meant to be ran in a Spark environment.

Dependencies:
- Spark 2.4.4

Classes
-

* [DynamoDB ID Bulk Writer](https://github.com/polyglotDataNerd/poly-spark-ccpa/blob/master/src/main/scala/com/sg/ccpa/utility/IDWriter.scala)

     - This object will do a bulk write and build the initial dataset that will live in the DynamoDB identity service. This takes an s3 directory input and transforms into a Spark dateframe. This will do a lookup to another dataframe that will consist of the ID service and insert new ids that do not exist in DDB. 
     - Steps to run the bulk application
        1. Clone the git repo 
        2. Download the secret src/main/resources directory from s3

                 aws s3 cp s3://polyglotDataNerd-bigdata-utility/spark/poly-spark-ccpa/src/main/ ~/poly-spark-ccpa/src/main/ --recursive --exclude "*" --include "*/resources"
            - Keep in mind that all our environments live in private subnets so this will only run within the AWS domain and not locally.
        3. Run the shell in the repo that will trigger the  SQL/Unload to create the resultset that will be inserted in [DDB](https://github.com/polyglotDataNerd/poly-spark-ccpa/blob/master/sql/unload_bulk_write.sh). ```bash ~/poly-spark-ccpa/sql/unload_bulk_write.sh``` this will generate an unload to the object store for each ID and UUID in gravy_customers
        4. Once the Unload is completed we can run the binary in s3 as a step function in the testing/production EMR cluster ([run EMR](https://github.com/polyglotDataNerd/poly-spark-ccpa/blob/master/ETL.sh#L42))
            