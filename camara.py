import cv2
import json
import base64
import time
from kafka import KafkaProducer

print("Iniciando Cámara...")

# CONEXIÓN REAL A KAFKA
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("CÁMARA CONECTADA AL CLÚSTER KAFKA.")
except:
    print("Error: Enciende Docker primero.")
    exit()

cap = cv2.VideoCapture(0)
TOPIC_NAME = 'frutas-stream'

while True:
    ret, frame = cap.read()
    if not ret: break

    # Enviar frame
    frame_small = cv2.resize(frame, (224, 224))
    _, buffer = cv2.imencode('.jpg', frame_small)
    img_str = base64.b64encode(buffer).decode('utf-8')

    producer.send(TOPIC_NAME, {'imagen': img_str})
    
    cv2.imshow('PRODUCTOR KAFKA (Real)', frame)
    
    # IMPORTANTE: Si envías muy rápido, Kafka puede saturarse en una laptop.
    # 0.1s es seguro.
    time.sleep(0.1) 
    
    if cv2.waitKey(1) & 0xFF == ord('q'): break

cap.release()
cv2.destroyAllWindows()