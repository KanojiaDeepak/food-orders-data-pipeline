from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import os

# Set the environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"

project_id = ''
topic_id = 'demo'
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

csv_file_path = "data/food_daily_12_07_2024.csv"
def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback
with open(csv_file_path, mode='r') as csvfile:
    data=csvfile.readlines()
    data=data[1:]
    # Publish each row to the Pub/Sub topic
    for row in data:
        print(row)
        print(type(row))
        
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_path, row.encode("utf-8"))
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)
        break
# Wait for all the publish futures to resolve before exiting.
futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
print("All messages published.")