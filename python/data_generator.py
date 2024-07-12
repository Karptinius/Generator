import random
import time
import faker
import json
from kafka import KafkaProducer

"""
def  generate_sample():
    return random.randint(0, 100)

def generate_data_stream():
    while True:
        time.sleep(0.2)
        yield generate_sample()

for i in generate_data_stream(): print(i)
"""

def generate_data(num_records):
    """Génère des données fictives.

    Args:
        num_records (int): Le nombre d'enregistrements à générer.

    Returns:
        list: Une liste de dictionnaires représentant les enregistrements de données.
    """
    data = []
    for _ in range(num_records):
        record = {
            "id": random.randint(0,100)
        }
        data.append(record)
    return data

def send_data_to_kafka(data, kafka_topic, kafka_producer):
    """Envoie les données générées vers un sujet Kafka.

    Args:
        data (list): La liste des enregistrements de données.
        kafka_topic (str): Le nom du sujet Kafka.
        kafka_producer (KafkaProducer): Le producteur Kafka.
    """
    for record in data:
        kafka_producer.send(kafka_topic, value=json.dumps(record).encode('utf-8'))

if __name__ == "__main__":
    # Générer des données
    num_records = 1000
    data = generate_data(num_records)

    # Configurer le producteur Kafka
    kafka_topic = "data-topic"
    kafka_producer = KafkaProducer(bootstrap_servers=['localhost'])

    # Envoyer les données vers Kafka
    send_data_to_kafka(data, kafka_topic, kafka_producer)

    # Fermer le producteur Kafka
    kafka_producer.close()