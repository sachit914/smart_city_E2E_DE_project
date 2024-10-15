# for production
import sys

import os
import uuid
from confluent_kafka import SerializingProducer
# import simplejson as json
import simplejson as json
from datetime import datetime, timedelta
import random 
# from time import sleep
import time


LONDON_COORDINATES= {
    "latitude":51.5074,
    "longitude": -0.1278
}

BIRMINGHAM_COORIDNATES= {
    "latitude":52.4862,
    "longitude":-1.8904
}

# calculate movement increment
LATITUDE_INCREMENT= (BIRMINGHAM_COORIDNATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORIDNATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vechicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)
start_time=datetime.now()
start_location=LONDON_COORDINATES.copy()

def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30, 60))  # Update frequency

    return start_time

def simulate_vehicle_movement():
    global start_location

    # Move towards Birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    

    # Add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    print(start_location)
    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C580',
        'year': 2824,
        'fuelType': 'Hybrid'
    }
 
 
def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),  # km/h
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }
    
def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }
       
def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),  # percentage
        'airQualityIndex': random.uniform(0, 500)  # AQI Value goes here
    }
    

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

# Usage
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
  
# json.dumps(): This function converts (or serializes) a Python object (like a dictionary, list, etc.) into a JSON string.
def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    
    producer.flush()
f"""
Flush the message buffer: When producing messages to Kafka, they are usually buffered internally to improve performance. 
Instead of sending each message immediately, Kafka producers may batch messages and send them in bulk to reduce overhead. 
The flush() method forces the producer to send all the messages still in the buffer to Kafka.

If you don’t call flush(), some messages might remain in the internal buffer and not actually be sent to Kafka, especially if the program terminates unexpectedly.
"""



def simulate_journey(producer,device_id):
    while True:
        # device_id is nothing but car id car detail
        vechicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id,vechicle_data['timestamp'])
        traffic_camera_data= generate_traffic_camera_data(device_id,vechicle_data['timestamp'], vechicle_data['location'],"NIkon-Camera123")
        weather_data = generate_weather_data(device_id,vechicle_data['timestamp'],vechicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id,vechicle_data['timestamp'],vechicle_data['location'])
        # print(vechicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_incident_data)
        if (vechicle_data['location'][0] >= BIRMINGHAM_COORIDNATES['latitude']
            and vechicle_data['location'][1] <= BIRMINGHAM_COORIDNATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vechicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        
        time.sleep(55)
        # break
# ----------------------------------------------------------------------------------main-------------------------------------------------------------
if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        # Serialization is the process of converting complex Python objects (like dictionaries, custom objects, or other data structures) into a format (like JSON, strings, or byte arrays)
        simulate_journey(producer, 'Vehicle-CodeWithYu-123')

    except KeyboardInterrupt:
        print('Simulation ended by the user')

    except Exception as e:
        print(f'Unexpected Error occurred: {e}')

