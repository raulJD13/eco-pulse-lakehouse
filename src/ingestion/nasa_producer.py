import os
import time
import json
import pandas as pd
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# CONFIGURACIÓN
NASA_MAP_KEY = os.getenv("NASA_MAP_KEY")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "fire-events"

# Definir regiones de España con sus bounding boxes
# Formato: "oeste,sur,este,norte" (longitud_min,latitud_min,longitud_max,latitud_max)
REGIONS = {
    "peninsula": {
        "bbox": "-9.5,35.5,4.5,43.8",
        "name": "España Peninsular y Baleares"
    },
    "canarias": {
        "bbox": "-18.5,27.5,-13,29.5",
        "name": "Islas Canarias"
    }
}

def get_kafka_producer():
    """Crea y retorna un productor de Kafka con serialización JSON."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_mock_data():
    """Genera datos simulados para pruebas."""
    return [
        {"latitude": 28.29, "longitude": -16.51, "brightness": 350, "confidence": "h", "region": "canarias"},
        {"latitude": 28.60, "longitude": -17.85, "brightness": 320, "confidence": "l", "region": "canarias"},
        {"latitude": 40.41, "longitude": -3.70, "brightness": 310, "confidence": "n", "region": "peninsula"},
        {"latitude": 37.38, "longitude": -5.99, "brightness": 305, "confidence": "h", "region": "peninsula"},
    ]

def fetch_fire_data_for_region(region_id, bbox, region_name):
    """
    Descarga datos de incendios para una región específica.
    
    Args:
        region_id: Identificador de la región (peninsula/canarias)
        bbox: Coordenadas del bounding box
        region_name: Nombre descriptivo de la región
    
    Returns:
        Lista de diccionarios con datos de incendios
    """
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_MAP_KEY}/VIIRS_SNPP_NRT/{bbox}/1"
    
    try:
        print(f"    Consultando {region_name}...")
        
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            print(f"       Error HTTP {response.status_code}")
            return []
        
        if response.text.startswith("Invalid"):
            print(f"       API Error: {response.text.strip()}")
            return []
        
        # Leer CSV
        df = pd.read_csv(url)
        
        if df.empty:
            print(f"       Sin incendios activos")
            return []
        
        # Verificar columnas necesarias
        required_cols = ['latitude', 'longitude', 'bright_ti4', 'confidence']
        if not all(col in df.columns for col in required_cols):
            print(f"       Faltan columnas requeridas")
            return []
        
        # Extraer y limpiar datos
        df_clean = df[required_cols].copy()
        df_clean.columns = ['latitude', 'longitude', 'brightness', 'confidence']
        
        # Añadir región a cada registro
        fire_list = df_clean.to_dict(orient='records')
        for fire in fire_list:
            fire['region'] = region_id
        
        print(f"       {len(fire_list)} incendio(s) detectado(s)")
        
        return fire_list
    
    except requests.exceptions.Timeout:
        print(f"       Timeout al conectar con NASA")
        return []
    
    except Exception as e:
        print(f"       Error: {type(e).__name__}: {e}")
        return []

def fetch_fire_data():
    """
    Descarga datos de incendios para todas las regiones de España.
    Si falla o no hay MAP_KEY válida, retorna datos simulados.
    """
    # Validar MAP_KEY
    if not NASA_MAP_KEY or len(NASA_MAP_KEY) < 20:
        print("⚠️  WARN: MAP_KEY no configurada correctamente.")
        print("   Solicítala en: https://firms.modaps.eosdis.nasa.gov/api/area/")
        print("   Usando datos simulados...")
        return get_mock_data()

    try:
        print(f"📡 INFO: Consultando API NASA FIRMS para España completa...")
        
        all_fires = []
        
        # Consultar cada región
        for region_id, region_data in REGIONS.items():
            fires = fetch_fire_data_for_region(
                region_id,
                region_data["bbox"],
                region_data["name"]
            )
            all_fires.extend(fires)
            time.sleep(0.5)  # Pequeña pausa entre regiones
        
        print(f"\n🔥 TOTAL: {len(all_fires)} incendio(s) activo(s) en España")
        
        # Si no hay datos reales, usar mock para pruebas
        if len(all_fires) == 0:
            print("   ℹ️  Sin incendios activos - Usando datos simulados para pruebas")
            return get_mock_data()
        
        return all_fires

    except Exception as e:
        print(f" ERROR inesperado: {type(e).__name__}: {e}")
        print("   Usando datos simulados...")
        return get_mock_data()

def run_producer():
    """
    Ejecuta el productor de Kafka en un loop infinito.
    Consulta la API cada 5 minutos y envía eventos al topic.
    """
    producer = get_kafka_producer()
    
    print("="*70)
    print("🚀 NASA FIRMS Fire Producer - España Completa")
    print("="*70)
    print(f"   Kafka Server: {KAFKA_SERVER}")
    print(f"   Topic: {TOPIC_NAME}")
    print(f"   MAP_KEY configurada: {'✅ Sí' if NASA_MAP_KEY else ' No'}")
    print(f"   Regiones monitorizadas:")
    for region_id, region_data in REGIONS.items():
        print(f"      • {region_data['name']} ({region_id})")
    print("="*70)
    print()

    cycle_count = 0
    
    while True:
        cycle_count += 1
        print(f"\n{'='*70}")
        print(f" CICLO #{cycle_count} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        # Obtener datos de incendios
        fire_data = fetch_fire_data()
        
        if not fire_data:
            print("ℹ  No hay datos para enviar en este ciclo")
        else:
            # Agrupar por región para estadísticas
            peninsula_count = sum(1 for f in fire_data if f.get('region') == 'peninsula')
            canarias_count = sum(1 for f in fire_data if f.get('region') == 'canarias')
            
            print(f"\n Distribución por región:")
            print(f"   • Península/Baleares: {peninsula_count}")
            print(f"   • Canarias: {canarias_count}")
            print()
            
            # Enviar cada incendio como un mensaje a Kafka
            for idx, fire in enumerate(fire_data, 1):
                message = {
                    "source": "NASA_VIIRS",
                    "region": fire.get("region", "unknown"),
                    "lat": fire["latitude"],
                    "lon": fire["longitude"],
                    "temp_k": fire["brightness"],
                    "confidence": fire["confidence"],
                    "timestamp": time.time()
                }
                
                producer.send(TOPIC_NAME, message)
                
                region_emoji = "" if message["region"] == "canarias" else "🏔️"
                print(f"   [{idx}/{len(fire_data)}] {region_emoji} ✉️  Enviado: "
                      f"({message['lat']:.2f}, {message['lon']:.2f}) "
                      f"Temp={message['temp_k']:.1f}K Conf={message['confidence']}")
                
                time.sleep(0.3)  # Pequeña pausa entre mensajes
            
            producer.flush()
            print(f"\n {len(fire_data)} mensaje(s) enviado(s) correctamente")
        
        # Esperar antes del siguiente ciclo
        # Recomendado: 300 segundos (5 minutos) para producción
        wait_time = 300
        print(f"\n Esperando {wait_time} segundos hasta el próximo ciclo...")
        print(f"   (Próxima consulta: {time.strftime('%H:%M:%S', time.localtime(time.time() + wait_time))})")
        time.sleep(wait_time)

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n\n⚠️  Productor detenido por el usuario (Ctrl+C)")
        print(" ¡Adiós!")
    except Exception as e:
        print(f"\n\n💥 ERROR FATAL: {e}")
        raise