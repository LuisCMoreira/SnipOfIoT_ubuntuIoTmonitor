import psutil
import paho.mqtt.client as mqtt
import json
import subprocess
import time

# Define the MQTT broker and credentials
broker_address = "127.0.0.1"
# username = "mqtt_user"
# password = "mqtt_password"
token_secret = "some_token"

while True:
    # Get CPU usage
    cpu_percent = psutil.cpu_percent()

    # Get memory usage
    mem = psutil.virtual_memory()
    mem_total = mem.total
    mem_used = mem.used
    mem_percent = mem.percent

    # Get disk usage
    disk = psutil.disk_usage('/')
    disk_total = disk.total
    disk_used = disk.used
    disk_percent = disk.percent

    # Check the status of Kafka, Cassandra, and Thingsboard services
    def check_service_status(service):
        status = subprocess.run(['systemctl', 'is-active', service], capture_output=True, text=True).stdout.strip()
        return status

    kafka_status = check_service_status('kafka')
    cassandra_status = check_service_status('cassandra')
   
    # Create a dictionary containing the system information
    system_info = {
        'cpu_percent': cpu_percent,
        'mem_total': mem_total,
        'mem_used': mem_used,
        'mem_percent': mem_percent,
        'disk_total': disk_total,
        'disk_used': disk_used,
        'disk_percent': disk_percent,
        'kafka_status': kafka_status,
        'cassandra_status': cassandra_status,
        'thingsboard_status': thingsboard_status
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(system_info)

    print(json_data)

    # Connect to the MQTT broker and publish the system information
    client = mqtt.Client()
    client.username_pw_set(token_secret)
    client.connect(broker_address, 1883)
    client.publish('the/mqtt/topic', json_data, qos=1, retain=True)
    time.sleep(0.1)
    client.disconnect()


    # Wait for 5 minutes before the next iteration
    time.sleep(300)
