import os
import time
import json
import random
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# CONFIGURACIÓN
API_KEY = os.getenv("OPENWEATHER_API_KEY")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "weather-events"

# Ubicaciones estratégicas de monitoreo en España
# Ubicaciones estratégicas de monitoreo del viento en incendios forestales en España
LOCATIONS = {
    "canarias": [
        {"name": "Teide_National_Park", "lat": 28.27, "lon": -16.64, "region": "Tenerife"},
        {"name": "La_Caldera_Taburiente", "lat": 28.71, "lon": -17.87, "region": "La Palma"},
        {"name": "Garajonay", "lat": 28.11, "lon": -17.22, "region": "La Gomera"},
        {"name": "Timanfaya", "lat": 29.03, "lon": -13.76, "region": "Lanzarote"},
        {"name": "Cumbre_Vieja", "lat": 28.57, "lon": -17.84, "region": "La Palma"},
        {"name": "Cumbre_Dorsal_Tenerife", "lat": 28.35, "lon": -16.50, "region": "Tenerife"},
        {"name": "Jandia", "lat": 28.06, "lon": -14.35, "region": "Fuerteventura"},
        {"name": "Tamabada", "lat": 28.12, "lon": -15.72, "region": "Gran Canaria"}
    ],

    "peninsula": [
        {"name": "Sierra_Nevada", "lat": 37.09, "lon": -3.39, "region": "Granada"},
        {"name": "Picos_Europa", "lat": 43.19, "lon": -4.85, "region": "Asturias"},
        {"name": "Ordesa_Monte_Perdido", "lat": 42.67, "lon": -0.03, "region": "Huesca"},
        {"name": "Donana", "lat": 37.01, "lon": -6.42, "region": "Huelva"},
        {"name": "Cabrera", "lat": 39.14, "lon": 2.94, "region": "Baleares"},
        {"name": "Monfrague", "lat": 39.85, "lon": -6.02, "region": "Cáceres"},

        # Zonas críticas para incendios y viento
        {"name": "Sierra_de_Gredos", "lat": 40.25, "lon": -5.29, "region": "Ávila"},
        {"name": "Sierra_de_la_Demanda", "lat": 42.15, "lon": -3.10, "region": "Burgos"},
        {"name": "Montseny", "lat": 41.77, "lon": 2.39, "region": "Cataluña"},
        {"name": "Montsec", "lat": 42.05, "lon": 0.75, "region": "Lleida"},
        {"name": "Sierra_de_Albarracin", "lat": 40.41, "lon": -1.44, "region": "Teruel"},
        {"name": "Serrania_de_Cuenca", "lat": 40.45, "lon": -2.05, "region": "Cuenca"},
        {"name": "Sistema_Iberico_Central", "lat": 41.80, "lon": -2.50, "region": "Soria"},
        {"name": "Cabo_Gata", "lat": 36.76, "lon": -2.11, "region": "Almería"},
        {"name": "Cabo_Creus", "lat": 42.32, "lon": 3.32, "region": "Girona"},
        {"name": "Cabo_Peñas", "lat": 43.66, "lon": -5.85, "region": "Asturias"},
        {"name": "Cabo_Trafalgar", "lat": 36.18, "lon": -6.03, "region": "Cádiz"},
        {"name": "Alto_Tajo", "lat": 40.70, "lon": -1.90, "region": "Guadalajara"},
        {"name": "Sierra_de_Cazorla", "lat": 37.91, "lon": -2.89, "region": "Jaén"},
        {"name": "Sierra_de_los_Filabres", "lat": 37.25, "lon": -2.50, "region": "Almería"},
        {"name": "Sierra_de_Espadan", "lat": 39.88, "lon": -0.35, "region": "Castellón"},
        {"name": "Sierra_Calderona", "lat": 39.68, "lon": -0.43, "region": "Valencia"},
        {"name": "Montes_de_Toledo", "lat": 39.50, "lon": -4.20, "region": "Toledo"}
    ]
}


def get_kafka_producer():
    """Crea y retorna un productor de Kafka con serialización JSON."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_mock_weather():
    """Genera clima aleatorio para pruebas."""
    return {
        "wind_speed": round(random.uniform(5.0, 60.0), 2),
        "wind_deg": random.randint(0, 360),
        "humidity": random.randint(10, 90),
        "temp": round(random.uniform(15.0, 35.0), 1),
        "pressure": random.randint(990, 1030),
        "clouds": random.randint(0, 100)
    }

def get_weather(lat, lon, location_name):
    """
    Obtiene clima real de OpenWeather API.
    Si falla, devuelve simulado.
    
    Args:
        lat: Latitud
        lon: Longitud
        location_name: Nombre de la ubicación (para logs)
    
    Returns:
        Diccionario con datos meteorológicos
    """
    if not API_KEY or len(API_KEY) < 20:
        return get_mock_weather()
    
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            print(f"      ⚠️  API Error {response.status_code} para {location_name}")
            return get_mock_weather()

        data = response.json()
        
        return {
            "wind_speed": data["wind"]["speed"],
            "wind_deg": data["wind"].get("deg", 0),
            "humidity": data["main"]["humidity"],
            "temp": data["main"]["temp"],
            "pressure": data["main"]["pressure"],
            "clouds": data["clouds"]["all"],
            "weather_main": data["weather"][0]["main"],
            "weather_desc": data["weather"][0]["description"]
        }
    
    except requests.exceptions.Timeout:
        print(f"      ⚠️  Timeout para {location_name}")
        return get_mock_weather()
    
    except KeyError as e:
        print(f"      ⚠️  Dato faltante ({e}) para {location_name}")
        return get_mock_weather()
    
    except Exception as e:
        print(f"      ⚠️  Error ({type(e).__name__}) para {location_name}")
        return get_mock_weather()

def fetch_weather_for_zone(zone_name, locations):
    """
    Obtiene datos meteorológicos para una zona completa.
    
    Args:
        zone_name: Nombre de la zona (canarias/peninsula)
        locations: Lista de ubicaciones en la zona
    
    Returns:
        Lista de diccionarios con datos meteorológicos
    """
    zone_display = " Canarias" if zone_name == "canarias" else "🏔️ Península/Baleares"
    print(f"   📍 Consultando {zone_display} ({len(locations)} ubicaciones)...")
    
    weather_data = []
    
    for loc in locations:
        weather = get_weather(loc["lat"], loc["lon"], loc["name"])
        
        if weather:
            weather_data.append({
                "location": loc,
                "weather": weather
            })
            
            # Mostrar condiciones preocupantes
            warnings = []
            if weather.get("wind_speed", 0) > 40:
                warnings.append(f"⚠️ Viento fuerte: {weather['wind_speed']}km/h")
            if weather.get("humidity", 100) < 30:
                warnings.append(f"⚠️ Humedad baja: {weather['humidity']}%")
            if weather.get("temp", 0) > 35:
                warnings.append(f"⚠️ Temperatura alta: {weather['temp']}°C")
            
            warning_str = " " + " ".join(warnings) if warnings else ""
            print(f"       {loc['name']}: {weather.get('temp', 0):.1f}°C, "
                  f"Viento {weather.get('wind_speed', 0):.1f}km/h, "
                  f"Humedad {weather.get('humidity', 0)}%{warning_str}")
        
        time.sleep(0.3)  # Evitar rate limiting de OpenWeather
    
    return weather_data

def run_producer():
    """
    Ejecuta el productor de Kafka en un loop infinito.
    Consulta la API cada 10 minutos (frecuencia de actualización de OpenWeather).
    """
    producer = get_kafka_producer()
    
    total_locations = sum(len(locs) for locs in LOCATIONS.values())
    
    print("="*70)
    print("  OpenWeather Producer - España Completa")
    print("="*70)
    print(f"   Kafka Server: {KAFKA_SERVER}")
    print(f"   Topic: {TOPIC_NAME}")
    print(f"   API_KEY configurada: {'✅ Sí' if API_KEY else '❌ No'}")
    print(f"   Ubicaciones monitorizadas: {total_locations}")
    for zone_name, locs in LOCATIONS.items():
        zone_emoji = "🏝️" if zone_name == "canarias" else "🏔️"
        print(f"      {zone_emoji} {zone_name.capitalize()}: {len(locs)} ubicaciones")
    print("="*70)
    print()

    cycle_count = 0
    
    while True:
        cycle_count += 1
        print(f"\n{'='*70}")
        print(f" CICLO #{cycle_count} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        all_weather_data = []
        
        # Consultar cada zona
        for zone_name, locations in LOCATIONS.items():
            weather_data = fetch_weather_for_zone(zone_name, locations)
            all_weather_data.extend(weather_data)
        
        print(f"\n Total de lecturas: {len(all_weather_data)}")
        
        # Enviar datos a Kafka
        if all_weather_data:
            print()
            for idx, item in enumerate(all_weather_data, 1):
                loc = item["location"]
                weather = item["weather"]
                
                message = {
                    "source": "OpenWeather",
                    "location_id": loc["name"],
                    "region": loc["region"],
                    "zone": "canarias" if loc in LOCATIONS["canarias"] else "peninsula",
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "wind_speed": weather["wind_speed"],
                    "wind_deg": weather["wind_deg"],
                    "humidity": weather["humidity"],
                    "temperature": weather["temp"],
                    "pressure": weather.get("pressure", 1013),
                    "clouds": weather.get("clouds", 0),
                    "weather_main": weather.get("weather_main", "Clear"),
                    "weather_desc": weather.get("weather_desc", "clear sky"),
                    "timestamp": time.time()
                }
                
                producer.send(TOPIC_NAME, message)
                
                zone_emoji = "" if message["zone"] == "canarias" else "🏔️"
                print(f"   [{idx}/{len(all_weather_data)}] {zone_emoji} ✉️  Enviado: {loc['name']}")
            
            producer.flush()
            print(f"\n {len(all_weather_data)} mensaje(s) enviado(s) correctamente")
        
        # Esperar antes del siguiente ciclo
        # OpenWeather actualiza datos cada ~10 minutos
        wait_time = 600  # 10 minutos
        print(f"\n Esperando {wait_time} segundos hasta el próximo ciclo...")
        print(f"   (OpenWeather actualiza datos cada ~10 minutos)")
        print(f"   (Próxima consulta: {time.strftime('%H:%M:%S', time.localtime(time.time() + wait_time))})")
        time.sleep(wait_time)

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n\n⚠️  Productor detenido por el usuario (Ctrl+C)")
        print("👋")
    except Exception as e:
        print(f"\n\n ERROR FATAL: {e}")
        raise