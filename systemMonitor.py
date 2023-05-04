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

# find out the PID of the process to monitor
cassandra_pid = None  # Initialize pid variable to None
username = "cassandra"  # Replace with the username of the process owner you want to check
process_name = "java"  # Replace with the name of the process you want to check
for process in psutil.process_iter(['name', 'pid', 'username']):
    if process.info['username'] == username and process.info['name'] == process_name:
        cassandra_pid= process.info['pid']
        break

tb_pid = None
username = "thingsboard"  # Replace with the name of the process you want to check
for process in psutil.process_iter(['name', 'pid', 'username']):
    if process.info['username'] == username and process.info['name'] == process_name:
        tb_pid = process.info['pid']
        break  
        
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
        
    # Get process RAM
    if cassandra_pid is not None:
        process = psutil.Process(cassandra_pid)
        cassandra_memory = process.memory_info()
        cassandra_memory_percent = (100 * cassandra_memory.rss)/mem_total
    else:
        cassandra_memory_percent = 'not available'

    if tb_pid is not None:
        process = psutil.Process(tb_pid)
        tb_memory = process.memory_info()
        tb_memory_percent = (100 * tb_memory.rss)/mem_total
    else:
        tb_memory_percent = 'not available'
        
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
        'cassandra_mem_used' : "{:.2f}".format(cassandra_memory_percent),
        'thingsboard_mem_used' : "{:.2f}".format(tb_memory_percent),
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
