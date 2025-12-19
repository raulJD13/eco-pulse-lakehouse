eco-pulse-lakehouse/
│
├── config/                 # Configuraciones (YAMLs, JSONs)
│   └── spark_config.conf   # Configs de Spark (memoria, cores)
│
├── data/                   # Datos locales (solo para pruebas, ignorado en git)
│   ├── gold/
│   └── silver/
│   └── bronze/
│
├── docker/                 # Dockerfiles personalizados (si los necesitamos)
│
├── notebooks/              # Jupyter Notebooks para experimentación (EDA)
│   └── 01_explore_nasa_api.ipynb
│
├── src/                    # CÓDIGO FUENTE PRINCIPAL
│   ├── __init__.py
│   ├── ingestion/          # Scripts del Producer (Python + Kafka)
│   │   └── nasa_producer.py
│   ├── processing/         # Scripts de Spark (Streaming + Batch)
│   │   └── fire_risk_processor.py
│   ├── utils/              # Funciones auxiliares (loggers, connectores)
│   │   └── spark_utils.py
│   └── visualization/      # Streamlit App
│       └── app.py
│
├── .env                    # VARIABLES DE ENTORNO (API KEYS - ¡Nunca al git!)
├── .gitignore              # Archivos a ignorar
├── docker-compose.yml      # La infraestructura completa
├── README.md               # Documentación del proyecto
└── requirements.txt        # Dependencias de Python


🌍 PROYECTO: Eco-Pulse Lakehouse

Tagline: Sistema de monitorización de incendios forestales en tiempo real y arquitectura Lakehouse distribuida.

1. ¿De qué trata realmente? (El Negocio)

Imagina que eres el Responsable de Operaciones de TUI en Canarias.

Tienes turistas en Tenerife, Gran Canaria y La Palma.

Problema: Un satélite detecta un incendio en La Palma. ¿Debes evacuar el hotel?

Tu Solución: Depende del viento. Si el fuego está a 20km pero el viento sopla hacia el hotel, es peligroso. Si sopla al revés, no.

Eco-Pulse: Ingesta el dato del fuego, le pregunta a una API de clima "qué tiempo hace justo en esa coordenada", calcula el riesgo y lo guarda para análisis histórico.

2. Arquitectura de Datos (El "Medallion Architecture")

No vamos a guardar datos a lo loco. Vamos a usar la arquitectura Medallion (Bronce, Plata, Oro) que es el estándar de la industria (Databricks).

Getty Images

Capa Bronze (Raw): Los datos tal cual llegan de la NASA y OpenWeather (sucios, JSONs anidados).

Capa Silver (Cleansed): Datos limpios, deduplicados y con tipos correctos (Fechas como Datetime, Coordenadas como Float).

Capa Gold (Business): Datos agregados. Ejemplo: "Tabla de Riesgo Diario por Isla".

3. Las Tecnologías (Tu Stack)

Aquí está el arsenal que vas a usar. Cada una tiene una función específica:

ComponenteTecnologíaFunción (El "Por qué")IngestaPython + Apache KafkaPara desacoplar la velocidad de entrada (satélites) de la velocidad de proceso. Kafka actúa de "buffer".ProcesamientoApache Spark (PySpark)El motor. Permite hacer Streaming Joins (unir fuego + viento en movimiento).AlmacenamientoDelta LakeFormato de archivo open source que permite ACID transactions sobre archivos Parquet. Es la magia del Lakehouse.Cloud StorageMinIO (Local) / AWS S3Donde se guardan los archivos físicos. MinIO simula S3 en tu PC para que sea gratis.OrquestaciónDocker ComposePara levantar toda la infraestructura (6 contenedores) con un comando.VisualizaciónStreamlitPara pintar el mapa con los puntos de fuego y vectores de viento.4. Las APIs y Datos Reales 📡

Vas a beber de dos fuentes públicas increíbles:

A. NASA FIRMS (Fire Information for Resource Management System)

Qué es: Datos casi en tiempo real (NRT) de los satélites MODIS y VIIRS.

El Dato: Te da Latitud, Longitud, Confidence (0-100% probabilidad de que sea fuego) y Frp (Fire Radiative Power - la intensidad del fuego).

Acceso: Gratis. Te registras y te dan una MAP_KEY.

Formato: CSV o WMS.

B. OpenWeatherMap (API)

Qué es: Datos meteorológicos hiper-locales.

El Dato: Te interesa wind_speed (velocidad) y wind_deg (dirección en grados).

La Lógica: Cuando recibes un fuego en Lat: 28.0, Lon: -15.5, tu script llama a esta API preguntando: "¿Qué viento hace AHORA MISMO en 28.0, -15.5?".

5. El Flujo de Trabajo (Pipeline) 🔄

Así es como viaja el dato dentro de tu ordenador:

The Producer (Python):

Consulta a la NASA: "¿Nuevos fuegos en Europa?"

Por cada fuego detectado, consulta a OpenWeather: "¿Viento en esta zona?"

Empaqueta todo en un JSON: {"lat": 28.1, "lon": -15.4, "wind": 30kmh, "risk": "HIGH"}.

Lo envía al Topic de Kafka fire-risk-events.

The Stream Processor (Spark):

Lee de Kafka constantemente.

Silver Logic: Convierte el JSON a columnas, filtra fuegos con confidence < 50%.

Escribe en MinIO en formato delta (Ruta: s3a://lakehouse/silver/fire_events).

The Analytics (Spark SQL / Streamlit):

Lee la tabla Delta.

Muestra en el mapa los puntos rojos (fuego) y flechas azules (dirección del humo).

6. ¿Por qué esto es "Nivel Dios"?

Streaming Join: Unir dos fuentes de datos en tiempo real es difícil. Pocos juniors saben hacerlo.

Delta Lake: Al usar Delta, puedes hacer "Time Travel". En la entrevista podrás decir: "Podemos consultar el estado del incendio tal y como estaba ayer a las 18:00, aunque los datos hayan cambiado hoy".

Escalabilidad: Si mañana en lugar de Europa quieres monitorizar el mundo entero, tu arquitectura aguanta. Solo tienes que añadir más "Workers" a Spark.

Datos Reales: Usarás los satélites de la NASA (FIRMS) y OpenWeather. Son datos que salvan vidas y son públicos.
----

1. ¿Por qué este proyecto es "Gold Standard" para tu CV?
Tu propuesta toca las tres "Vs" del Big Data (Volumen, Velocidad, Variedad) y demuestra competencias clave:

Arquitectura Moderna: El uso de Delta Lake y la arquitectura Medallion es exactamente lo que empresas como Databricks, Microsoft y grandes bancos están implementando ahora mismo. Te alejas del Data Warehouse tradicional y entras en el mundo del Lakehouse.

Desacoplamiento: Entender que Kafka actúa como buffer para que el sistema no colapse si la NASA envía millones de datos de golpe es un concepto de System Design fundamental.

Problema Real: La narrativa de "TUI en Canarias" es brillante. Consejo de oro: En las entrevistas, empieza SIEMPRE por el problema de negocio (salvar vidas/turismo), no por el código.


Getty Images
2. Análisis Técnico y Ajustes de "Nivel Experto"
Aunque el plan es excelente, quiero afinar un par de tuercas para que cuando te entreviste un Senior Staff Engineer, no te pille en renuncio.

A. La Trampa del "Streaming Join" (Importante)
En tu descripción dices:

Producer: Consulta NASA -> Consulta Weather -> Empaqueta JSON -> Envía a Kafka.

Spark: Lee Kafka -> Procesa.

Ojo con la terminología: Tal como lo describes en el paso 1, NO estás haciendo un Streaming Join en Spark. Estás haciendo Enriquecimiento en la Ingesta (Ingestion Enrichment). El dato ya llega unido a Spark.

Opción A (La que describes - Más fácil y robusta): Es válida. Reduces complejidad en Spark, pero aumentas latencia en el productor (si la API del clima falla, pierdes el dato del fuego o se bloquea el script).

Opción B (El verdadero "Nivel Dios" - Streaming Join):

Producer 1: Envía datos de Fuego al Topic fire-events.

Producer 2: Envía datos de Clima al Topic weather-events.

Spark: Lee ambos topics y los une en tiempo real basándose en coordenadas geográficas (geo-spatial join) o tiempo.

Veredicto: Si quieres vender "Streaming Joins" en la entrevista, ve por la Opción B. Si quieres vender "Robustez", quédate con la A, pero llámalo "Enriquecimiento pre-ingesta".

B. El Eslabón Perdido: Orquestación y Calidad
Tu stack es bueno, pero le faltan dos piezas para ser un entorno productivo real:

Calidad del Dato (Data Quality):

¿Qué pasa si la NASA envía una latitud 999? Tu pipeline explotará o ensuciará la capa Silver.

Acción: Implementa Great Expectations (librería Python) dentro de tu pipeline de Spark o como paso intermedio para validar que los datos cumplen las reglas antes de pasar a Silver.

Orquestación (El que manda):

Mencionas Docker Compose (infraestructura), pero ¿quién dispara los scripts?

Para un proceso de Streaming continuo, Spark corre siempre. Pero para mantenimientos o procesos batch (ej: compactar archivos Delta pequeños en uno grande - VACUUM), necesitas un orquestador.

Acción: Considera añadir Airflow o Mage.ai (más moderno) para orquestar la limpieza del Lakehouse.

3. Hoja de Ruta para Construirlo (Plan de Mentor)
No intentes hacerlo todo a la vez. Sigue este orden para no frustrarte:

Fase 1: El MVP (Minimum Viable Pipeline)

Script Python simple que lee NASA API y guarda en un CSV local.

Levantar Kafka y Zookeeper con Docker.

Hacer que el script envíe al topic de Kafka.

Fase 2: El Motor Spark

Levantar Spark y MinIO con Docker.

Script PySpark que lee de Kafka y escribe en consola (console sink).

Cambiar la escritura de consola a MinIO (formato Delta).

Fase 3: La Lógica de Negocio (Medallion)

Implementar la separación Bronze (Raw) -> Silver (Clean) -> Gold (Aggregated).

Aquí es donde aplicas la lógica de "Risk = High/Low".

Fase 4: La Capa Visual

Conectar Streamlit a la tabla Delta (Gold) y pintar el mapa.

4. Simulación de Entrevista: ¿Cómo defenderlo?
Cuando te pregunten por este proyecto, usa este guion:

Entrevistador: "Cuéntame sobre el proyecto Eco-Pulse."

Tú: "Lo construí porque quería resolver un problema de latencia en la toma de decisiones de seguridad turística.

El Reto: Detectar incendios y correlacionarlos con el viento en tiempo real para evaluar riesgos de evacuación.

La Arquitectura: Elegí una arquitectura Lakehouse sobre Delta Lake para garantizar transacciones ACID. Utilicé Kafka para desacoplar la ingesta de la NASA del procesamiento, lo que me permite escalar los productores sin tocar el cluster de Spark.

La decisión técnica más difícil: Fue elegir dónde hacer el cruce de datos. Decidí hacerlo [en la ingesta / en Spark] porque [razón de latencia / razón de escalabilidad].

El resultado: Un dashboard en Streamlit que actualiza el riesgo cada X segundos, con capacidad de 'Time Travel' para auditorías forenses gracias a Delta."


Tu intuición es correcta, pero vamos a precisar un detalle técnico importante: Spark NO habla con los Producers.

El flujo es así:

NASA Producer: "¡Tengo un incendio!" -> Lo lanza al buzón de Kafka (topic: fire-events).

Weather Producer: "¡Tengo viento!" -> Lo lanza al buzón de Kafka (topic: weather-events).

(Aquí los datos esperan en el buzón hasta que alguien los recoja).

Spark (Processor): Va al buzón de Kafka constantemente, recoge las cartas nuevas, las limpia (les pone tipos de datos correctos, timestamps, etc.) y las archiva ordenadas en MinIO (Tu Data Lake).

Así que sí: Spark recoge de Kafka y guarda en MinIO.

3. El Orden de Encendido (El Protocolo de Arranque) 🚦
En un sistema desacoplado (gracias a Kafka), el orden no es estricto (no explota si lo haces mal), pero el orden lógico profesional es este:

PASO 1: La Infraestructura (El Suelo)

Terminal 1: docker-compose up -d

Sin esto, no hay buzón (Kafka) ni archivo (MinIO).

PASO 2: La Ingesta (Los Proveedores)

Terminal 2: python src/ingestion/nasa_producer.py

Terminal 3: python src/ingestion/weather_producer.py

Estos empiezan a llenar Kafka de datos. Si Spark está apagado, no pasa nada, Kafka guarda los mensajes (por defecto hasta 7 días).

silver/: Aquí viven tus datos. Si entras, verás archivos .parquet (y carpetas _delta_log). Son tus datos de incendios y clima guardados en un formato comprimido y optimizado para Big Data.

checkpoints/: Esta es la "memoria" de Spark. Como es un proceso continuo, Spark apunta aquí qué mensajes ya ha leído. Si se va la luz y reinicias el script, Spark mirará aquí y dirá: "Me quedé en el mensaje 502", y seguirá desde ahí sin duplicar datos. Esto es la tolerancia a fallos.
