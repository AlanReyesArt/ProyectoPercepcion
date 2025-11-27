import json
import base64
import cv2
import numpy as np
import tensorflow as tf
from kafka import KafkaConsumer

# IMPORTACIONES CLAVE
from tensorflow.keras.applications.efficientnet import preprocess_input 
from tensorflow.keras.applications import EfficientNetB0
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D, Dropout, Input
from tensorflow.keras.models import Model

print("Construyendo Arquitectura Ganadora...")

try:
    # 1. CARGAR CLASES
    with open('clases.json', 'r') as f:
        class_indices = json.load(f)
    idx_to_class = {v: k for k, v in class_indices.items()}
    NUM_CLASES = len(class_indices)
    
    # 2. CONSTRUCCIÓN MANUAL (Modo Estricto)
    # Definimos explícitamente RGB (3 canales)
    inputs = Input(shape=(224, 224, 3))
    
    # Base EfficientNet vacía
    base_model = EfficientNetB0(include_top=False, input_tensor=inputs, weights=None)
    
    x = GlobalAveragePooling2D()(base_model.output)
    x = Dropout(0.5)(x)
    outputs = Dense(NUM_CLASES, activation='softmax')(x)
    
    model = Model(inputs=inputs, outputs=outputs)
    
    # 3. CARGA DE PESOS (LA QUE FUNCIONÓ EN EL TEST)
    print("Inyectando pesos (Modo Estricto)...")
    model.load_weights('modelo_final.h5')
    print("CEREBRO CARGADO AL 97%.")

except Exception as e:
    print(f"Error Crítico: {e}")
    exit()

# --- CONEXIÓN KAFKA ---
TOPIC_NAME = 'frutas-stream'
print(f"Buscando Kafka...")

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("SISTEMA ONLINE.")
except:
    print("Error: Kafka no corre. Ejecuta 'docker-compose up -d'")
    exit()

print(f"ESPERANDO IMÁGENES...")

for message in consumer:
    try:
        datos = message.value
        
        # 1. Decodificar Imagen (Viene en BGR desde OpenCV)
        img_bytes = base64.b64decode(datos['imagen'])
        nparr = np.frombuffer(img_bytes, np.uint8)
        img_bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # 2. CONVERTIR A RGB (CRUCIAL PARA EL 97%)
        # El modelo entrena en RGB, la cámara ve en BGR. Si no cambias esto, falla.
        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)

        # 3. Preprocesar
        img_resized = cv2.resize(img_rgb, (224, 224))
        img_array = np.expand_dims(img_resized, axis=0)
        img_ready = preprocess_input(img_array)

        # 4. Predicción
        preds = model.predict(img_ready, verbose=0)
        idx = np.argmax(preds)
        clase = idx_to_class.get(idx, "Desconocido")
        confianza = float(np.max(preds)) * 100

        # 5. Visualización Inteligente
        # Usamos img_bgr para mostrar en pantalla (OpenCV prefiere BGR)
        
        # Filtro: Si la confianza es baja (ej. tu cara), espera.
        if confianza < 50.0:
            texto = "ESPERANDO FRUTA..."
            cv2.putText(img_bgr, texto, (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (100, 100, 100), 2)
        else:
            # Detección Confirmada
            if "Rotten" in clase:
                estado = "PODRIDO"
                color = (0, 0, 255) # Rojo
            else:
                estado = "FRESCO"
                color = (0, 255, 0) # Verde
            
            label = f"{clase} ({confianza:.1f}%)"
            
            # Dibujar caja y texto
            cv2.putText(img_bgr, estado, (10, 40), cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2)
            cv2.putText(img_bgr, label, (10, 80), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 1)
        
        cv2.imshow('CONSUMIDOR KAFKA (IA)', img_bgr)
        if cv2.waitKey(1) & 0xFF == ord('q'): break
            
    except Exception as e:
        print(f"Error: {e}")

cv2.destroyAllWindows()