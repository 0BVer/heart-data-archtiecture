import json
import time

from kafka import KafkaProducer
import pandas as pd

bootstrap_server = ["127.0.0.1:19092", "127.0.0.1:29092", "127.0.0.1:39092"]
TOPIC_NAME = "heart_data"
prod = KafkaProducer(bootstrap_servers=bootstrap_server)
heart_data = pd.read_csv('../../data/heart.csv')


def select_visual(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        start = time.time()
        produce(data['col'], data['val'])
        end = time.time()

        return end - start


def produce(col, val):
    gene = (for _, row in heart_data.iterrows() if row[col] == val)
    for _, row in gene:
        data = {'age': int(row['age']),
                'sex': int(row['sex']),
                'cp': int(row['cp']),
                'trestbps': int(row['trestbps']),
                'chol': int(row['chol']),
                'fbs': int(row['fbs']),
                'restecg': int(row['restecg']),
                'thalach': int(row['thalach']),
                'exang': int(row['exang']),
                'oldpeak': int(row['oldpeak']),
                'slope': int(row['slope']),
                'ca': int(row['ca']),
                'thal': int(row['thal']),
                'target': int(row['target'])}
        json_tf = json.dumps(data)

        prod.send(topic=TOPIC_NAME, value=json_tf.encode(encoding='utf-8'))
        prod.flush()
