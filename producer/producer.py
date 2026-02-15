import json
import websocket
from confluent_kafka import Producer
from datetime import datetime

producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

def on_message(ws, message):
    raw = json.loads(message)
    
    # הנתונים נמצאים בתוך 'data'
    data = raw['data']
    
    trade = {
        'symbol': data['s'],
        'price': float(data['p']),
        'quantity': float(data['q']),
        'timestamp': datetime.now(datetime.UTC).isoformat(),
        'trade_time': data['T']
    }
    
    producer.produce('crypto-trades', value=json.dumps(trade).encode('utf-8'))
    producer.flush()
    
    print(f"[{trade['timestamp']}] {trade['symbol']}: ${trade['price']}")

def on_error(ws, error):
    print(f"שגיאה: {error}")

def on_close(ws, close_status_code, close_msg):
    print("החיבור נסגר")

def on_open(ws):
    print("מחובר ל-Binance! מתחיל לקבל נתונים...")

socket = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade"

ws = websocket.WebSocketApp(
    socket,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

ws.run_forever()