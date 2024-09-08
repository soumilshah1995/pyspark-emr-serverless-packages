# pyspark-emr-serverless-packages
pyspark-emr-serverless-packages
![Screenshot 2024-09-07 at 7 51 56 PM](https://github.com/user-attachments/assets/09319a66-0557-436d-9b67-3a0037792295)

### Step 1: Create EMR Serverless Cluster 

![Screenshot 2024-09-07 at 7 53 11 PM](https://github.com/user-attachments/assets/e53554fe-0019-4a46-8be3-6c00c8bda13b)


### Step 2:Start Cloud Shell 
```

mkdir package

cd package

# initialize a python virtual environment
python3 -m venv pyspark_venvsource
source pyspark_venvsource/bin/activate

# optionally, ensure pip is up-to-date
pip3 install --upgrade pip

# install the python packages
pip3 install boto3
pip3 install Faker

# package the virtual environment into an archive
pip3 install venv-pack
venv-pack -f -o pyspark_venv.tar.gz

# copy the archive to an S3 location
aws s3 cp pyspark_venv.tar.gz s3://XXX/python-packages/

# delete the packaged virtual environment archive after uploading to S3
rm pyspark_venv.tar.gz

# optionally, remove the virtual environment directory
rm -fr pyspark_venvsource

```

#### Key Points
* Replace <your-s3-bucket> with the name of your S3 bucket where the virtual environment will be stored.
* The venv-pack utility simplifies the process of packaging the virtual environment into a portable zip file.


create python file  spark_job.py
```
try:

    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    from faker import Faker
    import random

    print("ALL SET")
except Exception as e:
    print("Some modules are missing ", e)

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("hudi_cow_faker") \
    .getOrCreate()

# Initialize Faker
fake = Faker()
base_path = "s3://XX/parquet_raw"


# Function to generate fake records
def generate_records(num_records):
    records = []
    for _ in range(num_records):
        record = (
            random.randint(1, 10000),  # random ID
            fake.first_name(),  # random name
            random.randint(18, 70),  # random age
            fake.city(),  # random city
            fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')  # random timestamp
        )
        records.append(record)
    return records


# Generate 100 fake records
num_records = 100
records = generate_records(num_records)

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("create_ts", StringType(), True)
])

# Create a DataFrame
df = spark.createDataFrame(records, schema)
df.show()

# Write DataFrame to Parquet files
output_path = base_path + "/people_data"
df.write.mode("overwrite").parquet(output_path)

print(f"Data successfully written to {output_path}")

```

Upload to S3
```
aws s3 cp spark_job.py s3://XXXXXX/jobs/spark_job.py
```

Export ENV variables and Submit job
```
export APPLICATION_ID="XXXXXX"
export BUCKET="XXXX"
export IAM_ROLE="XXXXX"


```
### Submit job
```
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "SparkJobRun" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'$BUCKET'/jobs/spark_job.py",
            "sparkSubmitParameters": "--conf spark.archives=s3://'$BUCKET'/python-packages/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$BUCKET'/logs/"
            }
        }
    }'


```
