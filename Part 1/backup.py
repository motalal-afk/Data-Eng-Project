# backup.py
import os
import sys
import json
import time
import gzip
import shutil
import threading
from datetime import datetime, timedelta
import google.auth
from google.cloud import pubsub_v1

PROJECT_ID = None  # Auto-detected on a GCE VM; set manually if needed
SUBSCRIPTION_ID = 'backup_sub'

# How long to wait without any new message before considering the day's run finished
IDLE_THRESHOLD_S = 30

def resolve_project_id(explicit=None):
    project = explicit or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project:
        _, project = google.auth.default()
    if not project:
        raise SystemExit('Could not determine the GCP project ID. Set PROJECT_ID or export GOOGLE_CLOUD_PROJECT.')
    return project

# Thread-safe state for the active day's run
lock = threading.Lock()

# Statistics storage
first_msg_t = None
last_msg_t = None
unique_breadcrumbs = set()
vehicle_ids = set()
trip_ids = set()
earliest_bc_dt = None
latest_bc_dt = None
duplicate_count = 0
expected_count = 0
missing_count = 0
total_bytes_written = 0
current_backup_file = None

def parse_bc_datetime(record):
    # Support both uppercase and lowercase keys returned from the API
    opd_date = record.get('OPD_DATE') or record.get('opd_date', '')
    act_time = record.get('ACT_TIME') if record.get('ACT_TIME') is not None else record.get('act_time')
    if not opd_date or act_time is None:
        return None
    try:
        # BusData uses DDMMMYYYY:HH:MM:SS; also accept ISO date/datetime values.
        try:
            base_dt = datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')
        except ValueError:
            base_dt = datetime.strptime(opd_date.split('T')[0], '%Y-%m-%d')
        return base_dt + timedelta(seconds=int(act_time))
    except (AttributeError, TypeError, ValueError) as exc:
        print(f"Warning: Could not parse breadcrumb timestamp "
              f"(OPD_DATE={opd_date!r}, ACT_TIME={act_time!r}): {exc}")
        return None

def reset_stats():
    global first_msg_t, last_msg_t, unique_breadcrumbs, vehicle_ids, trip_ids
    global earliest_bc_dt, latest_bc_dt, duplicate_count, expected_count, missing_count
    global total_bytes_written, current_backup_file
    first_msg_t = None
    last_msg_t = None
    unique_breadcrumbs.clear()
    vehicle_ids.clear()
    trip_ids.clear()
    earliest_bc_dt = None
    latest_bc_dt = None
    duplicate_count = 0
    expected_count = 0
    missing_count = 0
    total_bytes_written = 0
    current_backup_file = None

def compress_backup_file(file_path):
    if not file_path or not os.path.exists(file_path):
        return None
    gz_path = f"{file_path}.gz"
    try:
        with open(file_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(file_path)
        return gz_path
    except Exception as e:
        print(f"Compression failed for {file_path}: {e}")
        return None

def log_daily_statistics(compressed_path):
    global first_msg_t, last_msg_t, unique_breadcrumbs, vehicle_ids, trip_ids
    global earliest_bc_dt, latest_bc_dt, duplicate_count, expected_count, missing_count
    global total_bytes_written

    if not first_msg_t:
        return

    now_wall = time.time()
    elapsed_wall = now_wall - first_msg_t
    total_unique = len(unique_breadcrumbs)
    throughput = total_unique / elapsed_wall if elapsed_wall > 0 else 0

    print("\n=================== DAILY BACKUP STATISTICS ===================")
    print(f"First Breadcrumb Received Timestamp   : {datetime.fromtimestamp(first_msg_t).isoformat()}")
    print(f"Backup Compression Finish Timestamp   : {datetime.fromtimestamp(now_wall).isoformat()}")
    print(f"Elapsed Wall-Clock Time (to compress) : {elapsed_wall:.3f} seconds")
    print(f"Count of Unique Breadcrumbs Received   : {total_unique}")
    print(f"Backup Throughput                     : {throughput:.2f} msg/s")
    print(f"Count of Unique Vehicle IDs            : {len(vehicle_ids)} ({list(vehicle_ids)})")
    print(f"Earliest Breadcrumb (Data Timestamp)   : {earliest_bc_dt.isoformat() if earliest_bc_dt else 'N/A'}")
    print(f"Latest Breadcrumb (Data Timestamp)     : {latest_bc_dt.isoformat() if latest_bc_dt else 'N/A'}")
    print(f"Count of Unique Trips (EVENT_NO_TRIP)  : {len(trip_ids)}")
    print(f"Expected Breadcrumbs                   : {expected_count}")
    print(f"Count of Duplicate Breadcrumbs Received: {duplicate_count}")
    print(f"Count of Missing Breadcrumbs           : {missing_count}")
    print(f"Uncompressed Backup File Size (bytes)  : {total_bytes_written}")
    print(f"Compressed Backup File Location        : {compressed_path if compressed_path else 'Failed'}")
    print("=================================================================\n")

def callback(message):
    global first_msg_t, last_msg_t, unique_breadcrumbs, vehicle_ids, trip_ids
    global earliest_bc_dt, latest_bc_dt, duplicate_count, expected_count, missing_count
    global total_bytes_written, current_backup_file

    now = time.time()
    try:
        payload_bytes = message.data
        record = json.loads(payload_bytes.decode('utf-8'))
    except Exception:
        message.ack()
        return

    vehicle_id = record.get('VEHICLE_ID') or record.get('vehicle_id')
    event_no_trip = record.get('EVENT_NO_TRIP') or record.get('event_no_trip')
    act_time = record.get('ACT_TIME') if record.get('ACT_TIME') is not None else record.get('act_time')
    composite_key = f"{vehicle_id}-{event_no_trip}-{act_time}"

    with lock:
        if first_msg_t is None:
            first_msg_t = now
            today_str = datetime.fromtimestamp(now).strftime('%Y-%m-%d')
            current_backup_file = f"breadcrumbs_{today_str}.log"
        last_msg_t = now

        if vehicle_id is not None:
            vehicle_ids.add(vehicle_id)
        if event_no_trip is not None:
            trip_ids.add(event_no_trip)

        bc_dt = parse_bc_datetime(record)
        if bc_dt:
            if earliest_bc_dt is None or bc_dt < earliest_bc_dt:
                earliest_bc_dt = bc_dt
            if latest_bc_dt is None or bc_dt > latest_bc_dt:
                latest_bc_dt = bc_dt

        if composite_key in unique_breadcrumbs:
            duplicate_count += 1
        else:
            unique_breadcrumbs.add(composite_key)

        # Append to log file and track bytes
        try:
            line_data = json.dumps(record) + "\n"
            line_bytes = line_data.encode('utf-8')
            with open(current_backup_file, 'ab') as f_out:
                f_out.write(line_bytes)
            total_bytes_written += len(line_bytes)
        except Exception as e:
            print(f"Failed writing to log file: {e}")

        try:
            expected_count = max(expected_count, int(message.attributes.get('total', 0)))
        except (ValueError, TypeError):
            pass

    message.ack()

# Initialize Subscriber Client
subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(resolve_project_id(PROJECT_ID), SUBSCRIPTION_ID)

try:
    subscriber.get_subscription(subscription=sub_path)
except Exception as e:
    print(f"Error connecting to subscription {sub_path}: {e}")
    sys.exit(1)

print(f"Backup service started. Listening on subscription: {sub_path}")

# Establish flow control
flow_control = pubsub_v1.types.FlowControl(max_messages=1000)
streaming_pull = subscriber.subscribe(
    sub_path,
    callback=callback,
    flow_control=flow_control,
    await_callbacks_on_shutdown=True
)

with subscriber:
    try:
        while True:
            time.sleep(2)
            with lock:
                if last_msg_t is not None and (time.time() - last_msg_t) > IDLE_THRESHOLD_S:
                    missing_count = max(0, expected_count - len(unique_breadcrumbs))
                    gz_path = compress_backup_file(current_backup_file)
                    log_daily_statistics(gz_path)
                    reset_stats()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        streaming_pull.result()