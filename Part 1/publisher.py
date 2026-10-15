# publisher.py
import os
import sys
import json
import time
import uuid
import threading
import requests
from datetime import datetime
import google.auth
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import (BatchSettings, LimitExceededBehavior,
                                          PublisherOptions, PublishFlowControl)

# Configuration
PROJECT_ID = None  # Auto-detected on GCE VM, or set manually
TOPIC_ID = 'bc_topic'

# Vehicle IDs for the group 'porcupine'
# (Replace this list with your team's exact vehicle IDs if different)
VEHICLE_IDS = [3565, 3131, 4518, 4021, 2929, 3719, 3703, 4006, 3253, 4023,
    3515, 3546, 4065, 4529, 3927, 3638, 4525, 3116, 3954, 2924,
    3928, 4017, 3554, 3210, 3005, 3261, 3718, 3622, 3102, 3402,
    4068, 3910, 3629, 3163, 3755, 3509, 3911, 3962, 3513, 3413,
    3157, 3017, 3502, 3945, 3914, 3405, 4303, 3056, 3412, 4226,
    4067, 3227, 3905, 3604, 3154, 3035, 4050, 3008, 3633, 3055,
    4515, 4003, 3220, 3613, 3632, 4227, 3258, 3518, 4010, 3501,
    3211, 3046, 2932, 4043, 3538, 3047, 3544, 3306, 3963, 3150,
    99222, 4302, 2931, 3234, 3735, 3310, 3307, 3639, 3170, 4203,
    3705, 3223, 3751, 3224, 4026, 2906, 3222, 2911, 4036, 3203,
    3305, 3215, 4517, 4234, 3626, 4238, 3918, 4016, 3239, 3006,
    4002, 3949, 3264, 3557, 3553, 3925, 3947, 3574, 4054, 3619,
    3003, 4052, 4019, 3944, 2904, 3057, 3216, 4030, 3326, 3246,
    3265, 4008, 3304, 3532, 3218, 3260, 4202, 3746, 2903, 4521,
    3514, 3547, 3039, 3104, 3169, 2936, 3540, 4004, 4229, 3739,
    3942, 3929, 3127, 3010, 3308, 3318, 3101, 4044, 3120, 3214,
    3648, 3570, 4213, 3204, 4045, 3749, 3311, 4304, 3328, 3627,
    3611, 3548, 3714, 3960, 3526, 3563, 3408, 3643, 3715, 4009,
    3510, 3567, 3958, 3559, 3938, 3543, 4201, 3937, 3757, 4033,
    4520, 3529, 4035, 2901, 2927, 3409, 3254, 3134, 4230, 3727,
    3919, 3024, 3531, 3156, 3964, 3325, 3539, 3226, 3545, 3542,
    3238, 3138, 3917, 2914, 3303, 4057, 3162, 3623, 3930, 3161,
    3920, 3524, 3624, 3802, 3528, 4523, 4060, 3902, 3913, 3016,
    3939, 3252, 3504, 3051, 3738, 3577, 3754, 2939, 3221, 3732,
    4223, 4042, 4051, 3634, 3549, 4217, 3042, 4049, 3126, 4231,
]

# Batch and Flow Control Settings for performance and safety
batch_settings = BatchSettings(
    max_messages=100,
    max_latency=0.05,
)
flow_control = PublishFlowControl(
    message_limit=10_000,
    byte_limit=20 * 1024 * 1024,
    limit_exceeded_behavior=LimitExceededBehavior.BLOCK,
)

def resolve_project_id(explicit=None):
    project = explicit or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project:
        _, project = google.auth.default()
    if not project:
        raise SystemExit('Could not determine GCP project ID. Set PROJECT_ID or export GOOGLE_CLOUD_PROJECT.')
    return project

# Initialize Publisher
publisher = pubsub_v1.PublisherClient(
    batch_settings=batch_settings,
    publisher_options=PublisherOptions(flow_control=flow_control),
)
topic_path = publisher.topic_path(resolve_project_id(PROJECT_ID), TOPIC_ID)

try:
    publisher.get_topic(topic=topic_path)
except Exception as e:
    print(f"Error: Verification of topic {topic_path} failed: {e}")
    sys.exit(1)

# State tracking
RUN_ID = time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
done_cv = threading.Condition()
published_count = 0
confirmed_count = 0
error_count = 0
error_examples = []

def on_publish_done(future):
    global confirmed_count, error_count
    exc = future.exception()
    with done_cv:
        if exc is None:
            confirmed_count += 1
        else:
            error_count += 1
            if len(error_examples) < 5:
                error_examples.append(repr(exc))
        done_cv.notify_all()

# Run metrics collection
start_api_time = datetime.now()  # Moment when publisher begins accessing API
start_wall = time.time()
unique_vehicles_received = set()

print(f"Starting Publisher Run: {RUN_ID}")
print(f"Accessing BusData API at {start_api_time.isoformat()}")

for vehicle_id in VEHICLE_IDS:
    api_url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        response = requests.get(api_url, timeout=30)
        if response.status_code != 200:
            print(f"Warning: API returned status {response.status_code} for vehicle {vehicle_id}")
            continue
        
        breadcrumbs = response.json()
        if not isinstance(breadcrumbs, list):
            continue
            
        if len(breadcrumbs) > 0:
            unique_vehicles_received.add(vehicle_id)
            
        for record in breadcrumbs:
            payload = json.dumps(record).encode('utf-8')
            # Publish immediately without saving to filesystem
            future = publisher.publish(
                topic_path, 
                payload,
                run_id=RUN_ID,
                vehicle_id=str(vehicle_id)
            )
            future.add_done_callback(on_publish_done)
            with done_cv:
                published_count += 1
                
    except Exception as e:
        print(f"Error processing vehicle {vehicle_id}: {e}")
        with done_cv:
            error_count += 1

# Wait for all outstanding callbacks to finish
with done_cv:
    while (confirmed_count + error_count) < published_count:
        done_cv.wait(timeout=1)

end_api_time = datetime.now()  # Moment when publisher finishes sending all messages
elapsed_wall = time.time() - start_wall
throughput = confirmed_count / elapsed_wall if elapsed_wall > 0 else 0

# Print Required Statistics
print("\n--- Publisher Run Statistics ---")
print(f"Run ID                               : {RUN_ID}")
print(f"API Access Start Timestamp            : {start_api_time.isoformat()}")
print(f"Final Message Publish Timestamp      : {end_api_time.isoformat()}")
print(f"Elapsed Wall-Clock Time              : {elapsed_wall:.3f} seconds")
print(f"Total Breadcrumbs Published          : {confirmed_count}")
print(f"Publishing Throughput                : {throughput:.2f} msg/s")
print(f"Unique Vehicles with Data Received   : {len(unique_vehicles_received)} ({list(unique_vehicles_received)})")
print(f"Publishing Errors                    : {error_count}")

if error_examples:
    print("\nExample errors:")
    for exc in error_examples:
        print(f" - {exc}")

sys.exit(1 if error_count > 0 else 0)