# Food Orders Batch Data Pipeline
This data pipeline runs everyday which triggers dataflow flex template that fetches data from GCS for respective date and do transformations and write to bigquery tables.

## Google Cloud Services Used

### Google Cloud Storage
The data will land in GCS daily

```
gcloud storage buckets create gs://<BUCKET> --location=us-central1

cd data

gcloud storage cp food_daily_12_07_2024.csv gs://<BUCKET>/data/
gcloud storage cp food_daily_13_07_2024.csv gs://<BUCKET>/data/

cd ..
```

### Bigquery
Transformed data will be stored in bigquery under two tables: 'delivered_orders' and 'other_orders'

```
bq --location us-central1 mk <DATASET_NAME>
```

### Composer
Composer will run dag which is scheduled to run daily

```
gcloud composer environments create <ENVIRONMENT_NAME> \
    --location us-central1 \
    --image-version composer-3-airflow-2.7.3-build.7
```

### Artifact Registry
To create dataflow flex template, we need to store the respective docker image at this repository

```
gcloud artifacts repositories create <REPO_NAME> \
 --repository-format=docker \
 --location=us-central1
```

### Dataflow
Dataflow pipeline does the following:
1. Reads the data from GCS
2. Does data cleansing
3. Seperate data on the basis of order status
4. Write data to bigquery table

#### Run dataflow job to check if the job runs fine
```
python pipeline.py \
    --region us-central1 \
    --runner DataflowRunner \
    --project <PROJECT_ID> \
    --temp_location gs://<BUCKET>/tmp/ \
    --input gs://<BUCKET>/data/food_daily_12_07_2024.csv \
    --dataset <DATASET>
```

#### Build Dataflow Flex Template

```
gcloud dataflow flex-template build gs://<BUCKET>/batch_pipeline.json \
 --image-gcr-path "us-central1-docker.pkg.dev-<PROJECT_ID>/foodorders/batchpipeline:latest" \
 --sdk-language "PYTHON" \
 --flex-template-base-image "PYTHON3" \
 --metadata-file "metadata.json" \
 --py-path "." \
 --env "FLEX_TEMPLATE_PYTHON_PY_FILE=pipeline.py" \
 --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"
```

#### Run Flex Template
```
gcloud dataflow flex-template run batchfoodorders \
--template-file-gcs-location gs://<BUCKET>/batch_pipeline.json \
--region us-central1 \
--parameters input=gs://<BUCKET>/data/food_daily_12_07_2024.csv,dataset=<DATASET>,staging_location=gs://<BUCKET>/tmp/
```

## Orchestrate from Composer
Upload 'dag.py' to Composer DAGs bucket
