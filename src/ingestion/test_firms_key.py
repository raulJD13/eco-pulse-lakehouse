import requests
from dotenv import load_dotenv
import os

load_dotenv()
NASA_MAP_KEY = os.getenv("NASA_MAP_KEY")

print("="*70)
print("🔧 DIAGNÓSTICO DE MAP_KEY DE FIRMS")
print("="*70)
print(f"MAP_KEY detectada: {NASA_MAP_KEY}")
print(f"Longitud: {len(NASA_MAP_KEY) if NASA_MAP_KEY else 0} caracteres")
print()

# TEST 1: Verificar disponibilidad de datos
print("📋 TEST 1: Verificando disponibilidad de datos...")
test1_url = f"https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_MAP_KEY}/VIIRS_SNPP_NRT"
try:
    r1 = requests.get(test1_url, timeout=10)
    print(f"   Status: {r1.status_code}")
    print(f"   Respuesta: {r1.text[:200]}")
    if r1.status_code == 200 and not r1.text.startswith("Invalid"):
        print("   ✅ Este endpoint funciona!")
    else:
        print("   ❌ Este endpoint falla")
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# TEST 2: Lista de países
print("📋 TEST 2: Verificando lista de países (sin auth)...")
test2_url = "https://firms.modaps.eosdis.nasa.gov/api/countries"
try:
    r2 = requests.get(test2_url, timeout=10)
    print(f"   Status: {r2.status_code}")
    if "ESP" in r2.text:
        print("   ✅ España está en la lista")
    else:
        print("   ⚠️  España no encontrada en lista")
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# TEST 3: Endpoint de país con diferentes variaciones
print("📋 TEST 3: Probando endpoint /country con España...")
test_urls = [
    ("Original", f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_MAP_KEY}/VIIRS_SNPP_NRT/ESP/1"),
    ("Con firms2", f"https://firms2.modaps.eosdis.nasa.gov/api/country/csv/{NASA_MAP_KEY}/VIIRS_SNPP_NRT/ESP/1"),
    ("MODIS en vez de VIIRS", f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_MAP_KEY}/MODIS_NRT/ESP/1"),
]

for name, url in test_urls:
    print(f"\n   Probando: {name}")
    print(f"   URL: {url}")
    try:
        r = requests.get(url, timeout=10)
        print(f"   Status: {r.status_code}")
        print(f"   Primeros 100 chars: {r.text[:100]}")
        if r.status_code == 200 and not r.text.startswith("Invalid"):
            print(f"   ✅ ¡ESTE FUNCIONA!")
        else:
            print(f"   ❌ Este falla")
    except Exception as e:
        print(f"   ❌ Error: {e}")

print()

# TEST 4: Endpoint de área (bounding box)
print("📋 TEST 4: Probando endpoint /area con Canarias...")
# Bounding box de Canarias: lon_min,lat_min,lon_max,lat_max
test4_url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_MAP_KEY}/VIIRS_SNPP_NRT/-18.5,27.5,-13,29.5/1"
print(f"   URL: {test4_url}")
try:
    r4 = requests.get(test4_url, timeout=10)
    print(f"   Status: {r4.status_code}")
    print(f"   Primeros 100 chars: {r4.text[:100]}")
    if r4.status_code == 200 and not r4.text.startswith("Invalid"):
        print("   ✅ ¡Este endpoint funciona!")
    else:
        print("   ❌ Este endpoint falla")
except Exception as e:
    print(f"   ❌ Error: {e}")

print()
print("="*70)
print("🎯 CONCLUSIONES:")
print("="*70)
print("Si TODOS los tests fallan con 'Invalid API call':")
print("  1. Tu MAP_KEY podría no estar activada aún")
print("  2. Puede haber un problema con tu cuenta FIRMS")
print("  3. Intenta solicitar una nueva MAP_KEY")
print()
print("Si algunos tests funcionan:")
print("  - Usa el endpoint que funcione en tu código principal")
print("="*70)