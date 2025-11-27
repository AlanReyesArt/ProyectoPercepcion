import cv2
import json
import base64
import time
from kafka import KafkaProducer

print("Iniciando Cámara de Alta Velocidad...")

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        # Compresión gzip para que viaje más rápido por la red
        compression_type='gzip',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("CÁMARA CONECTADA.")
except:
    print("Error: Enciende Docker primero.")
    exit()

cap = cv2.VideoCapture(0)
# Bajamos la resolución de captura de la webcam para ganar velocidad
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)

TOPIC_NAME = 'frutas-stream'

while True:
    ret, frame = cap.read()
    if not ret: break

    # --- OPTIMIZACIÓN CLAVE: REDUCIR AL TAMAÑO EXACTO DEL MODELO ---
    # En lugar de enviar una foto HD gigante, enviamos una de 224x224 (muy ligera)
    frame_model = cv2.resize(frame, (224, 224))
    
    # Codificar la imagen pequeña
    _, buffer = cv2.imencode('.jpg', frame_model, [int(cv2.IMWRITE_JPEG_QUALITY), 70]) # Calidad 70% para velocidad
    img_str = base64.b64encode(buffer).decode('utf-8')

    # Enviar a Kafka
    producer.send(TOPIC_NAME, {'imagen': img_str})
    
    # Mostrar en pantalla la versión normal (para que tú veas bien)
    cv2.imshow('CAMARA (Envia)', frame)
    
    # Enviamos cada 0.15s (aprox 6-7 cuadros por segundo). 
    # Esto es suficiente para frutas y evita saturar al consumidor.
    time.sleep(0.15)
    
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()