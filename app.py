import json
import base64
import cv2
import numpy as np
import tensorflow as tf
from kafka import KafkaConsumer
from tensorflow.keras.applications import EfficientNetB0
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D, Dropout, Input
from tensorflow.keras.models import Model

# 1. CARGA DEL MODELO (Esto ya sabemos que funciona bien)
print("Cargando Cerebro IA...")
try:
    with open('clases.json', 'r') as f:
        class_indices = json.load(f)
    idx_to_class = {v: k for k, v in class_indices.items()}
    NUM_CLASES = len(class_indices)
    
    inputs = Input(shape=(224, 224, 3))
    base_model = EfficientNetB0(weights=None, include_top=False, input_tensor=inputs)
    base_model.trainable = False
    x = GlobalAveragePooling2D()(base_model.output)
    x = Dropout(0.5)(x)
    outputs = Dense(NUM_CLASES, activation='softmax')(x)
    
    model = Model(inputs=inputs, outputs=outputs)
    model.load_weights('modelo_final.h5', by_name=True, skip_mismatch=True)
    print("CEREBRO CARGADO.")
except Exception as e:
    print(f"Error Modelo: {e}")
    exit()

# 2. CONEXIÓN REAL A KAFKA
print("Buscando servidor Kafka en localhost:9092...")
TOPIC_NAME = 'frutas-stream'

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("CONEXIÓN EXITOSA A KAFKA.")
except Exception as e:
    print(f"ERROR: No veo a Docker corriendo. {e}")
    exit()

print(f"ESPERANDO IMÁGENES...")

for message in consumer:
    try:
        datos = message.value
        
        # Decodificar
        img_bytes = base64.b64decode(datos['imagen'])
        nparr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # Predecir
        img_resized = cv2.resize(img, (224, 224))
        img_array = np.expand_dims(img_resized / 255.0, axis=0)
        
        preds = model.predict(img_array, verbose=0)
        idx = np.argmax(preds)
        clase = idx_to_class.get(idx, "Desconocido")
        confianza = float(np.max(preds)) * 100

        # Mostrar
        estado = "PODRIDO " if "Rotten" in clase else "FRESCO "
        color = (0, 0, 255) if "Rotten" in clase else (0, 255, 0)
        
        texto = f"{clase} ({confianza:.1f}%)"
        cv2.putText(img, estado, (10, 40), cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2)
        cv2.putText(img, texto, (10, 80), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 1)
        
        cv2.imshow('CONSUMIDOR KAFKA (Real)', img)
        if cv2.waitKey(1) & 0xFF == ord('q'): break
            
    except Exception as e:
        print(f"Glitch en la matriz: {e}")

cv2.destroyAllWindows()