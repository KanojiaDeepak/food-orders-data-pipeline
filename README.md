# Food Orders Batch Data Pipeline

gcloud storage buckets create gs://food_orders_delivery --location=us-central1

gcloud storage cp food_daily_10_11_2020.csv gs://food_orders_delivery/data/
gcloud storage cp food_daily_11_11_2020.csv gs://food_orders_delivery/data/

gcloud storage cp pipeline.py gs://food_orders_delivery/code/


python -m gs://food_orders_delivery/code/pipeline.py \
    --region us-central1 \
    --runner DataflowRunner \
    --project <PROJECT_ID> \
    --temp_location gs://food_orders_delivery/tmp/ \
    --input gs://food_orders_delivery/data/food_daily_10_11_2020.csv \
    --dataset demodataset