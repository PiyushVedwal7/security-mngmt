import random,time
from kafka import KafkaProducer
import json
from datetime import datetime


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


fire_locations = ['Main Hall', 'Cafeteria', 'Library', 'Lab']
entry_points = ['Main Gate', 'Back Entrance', 'Library Entrance', 'Lab Entrance']


def events():
    while True:
        if random.random()<0.1:
            location=random.choice(fire_locations)
            alert_time=datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            fire_event={
                "location": location,
                "alert_type": "fire_detected",
                "severity": random.choice(['low', 'medium', 'high']),
                "time": alert_time

            }
            producer.send('fire-alert', fire_event)
            print(f"Produced fire alert: {fire_event}")

        if random.random()<0.05:
            location=random.choice(entry_points)
            detection_time=datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            intruder_alert={
                "location": location,
                "activity": "intruder_detected",
                "time": detection_time
            }
            producer.send('intruder-detection', intruder_alert)
            print(f"Produced intruder event: {intruder_alert}")


        time.sleep(150)


if __name__ == "__main__":
    events()            


    


