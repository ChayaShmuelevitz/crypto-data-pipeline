import json
import boto3
from confluent_kafka import Consumer
from datetime import datetime

# חיבור ל-AWS S3
s3_client = boto3.client('s3', region_name='eu-west-1')

# שם ה-bucket — שני את זה לשם האמיתי שלך!
BRONZE_BUCKET = 'data-pipeline-bronze-e601c2c3' 

# הגדרת ה-Consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'crypto-consumer-group',
    'auto.offset.reset': 'earliest'
})

# הרשמה ל-topic
consumer.subscribe(['crypto-trades'])

# אוסף הודעות לפני שמירה ל-S3
batch = []
BATCH_SIZE = 100  # נשמור כל 100 הודעות

def save_to_s3(batch):
    """
    שומר batch של הודעות ל-S3 כקובץ JSON
    """
    timestamp = datetime.utcnow().strftime('%Y/%m/%d/%H-%M-%S')
    file_key = f"crypto-trades/{timestamp}.json"
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=file_key,
        Body=json.dumps(batch).encode('utf-8'),
        ContentType='application/json'
    )
    
    print(f"✅ נשמר ל-S3: {file_key} ({len(batch)} הודעות)")

print("Consumer מתחיל לקרוא מ-Kafka...")

try:
    while True:
        # קריאת הודעה אחת מ-Kafka
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            print(f"שגיאה: {msg.error()}")
            continue
        
        # המרת ההודעה מ-bytes ל-dictionary
        trade = json.loads(msg.value().decode('utf-8'))
        batch.append(trade)
        
        print(f"📨 התקבל: {trade['symbol']} ${trade['price']}")
        
        # כשמצטברות 100 הודעות — שמור ל-S3
        if len(batch) >= BATCH_SIZE:
            save_to_s3(batch)
            batch = []  # אפס את ה-batch

except KeyboardInterrupt:
    # כשלוחצים Ctrl+C — שמור מה שנשאר
    if batch:
        save_to_s3(batch)
    print("Consumer נסגר")
    
finally:
    consumer.close()