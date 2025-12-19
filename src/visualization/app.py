import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt
from deltalake import DeltaTable

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE PÁGINA Y ESTILOS CSS
# ---------------------------------------------------------
st.set_page_config(
    page_title="Eco-Pulse | Fire Monitoring",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS para dar look "Cyberpunk/Dashboard Profesional"
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    div[data-testid="stMetric"] {
        background-color: #262730;
        border: 1px solid #464b5f;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
    }
    div[data-testid="stMetric"]:hover {
        border-color: #ff4b4b;
    }
    h1, h2, h3 {
        font-family: 'Segoe UI', sans-serif;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. CARGA DE DATOS ROBUSTA
# ---------------------------------------------------------
@st.cache_data(ttl=30) 
def load_data():
    try:
        storage_options = {
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true"
        }
        
        dt = DeltaTable("s3a://lakehouse/gold/fire_risk_alerts", storage_options=storage_options)
        df = dt.to_pandas()
        
        if df.empty:
            return pd.DataFrame()

        df['fire_id'] = df['fire_lat'].astype(str) + "_" + df['fire_lon'].astype(str)
        
        risk_order = {'EXTREME': 0, 'VERY_HIGH': 1, 'HIGH': 2, 'MODERATE': 3, 'LOW': 4}
        df['risk_rank'] = df['risk_level'].map(risk_order)
        
        df_sorted = df.sort_values(by=['fire_id', 'risk_rank', 'distance_deg'], ascending=[True, True, True])
        unique_fires = df_sorted.drop_duplicates(subset=['fire_id'])
        
        return unique_fires

    except Exception:
        return pd.DataFrame()

df = load_data()

# ---------------------------------------------------------
# 3. SIDEBAR
# ---------------------------------------------------------
with st.sidebar:
    st.title("🎛️ Panel de Control")
    st.markdown("---")
    
    st.subheader("Filtrar por Nivel")
    if not df.empty:
        available_risks = df['risk_level'].unique().tolist()
        selected_risks = st.multiselect(
            "Niveles visibles:",
            options=available_risks,
            default=available_risks
        )
        df_filtered = df[df['risk_level'].isin(selected_risks)]
    else:
        df_filtered = df

    st.markdown("---")
    st.caption("v1.0.0 | Lakehouse Architecture")
    
    if st.button('🔄 Actualizar Ahora'):
        st.cache_data.clear()
        st.rerun()

# ---------------------------------------------------------
# 4. DASHBOARD PRINCIPAL
# ---------------------------------------------------------
st.title("🛰️ Eco-Pulse Monitor")
st.markdown(f"**Estado:** {'🟢 LIVE' if not df.empty else '🟠 ESPERANDO DATOS'} | **Incendios únicos detectados en España**")

if df_filtered.empty:
    st.info("ℹ️ No hay alertas activas que coincidan con los filtros o el sistema está procesando.")
    st.stop()

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)

total_fires = len(df_filtered)
extreme_risk = len(df_filtered[df_filtered['risk_level'] == 'EXTREME'])
high_risk = len(df_filtered[df_filtered['risk_level'].isin(['HIGH', 'VERY_HIGH'])])
avg_temp = df_filtered['temperature'].mean() - 273.15

col1.metric("🔥 Focos Activos", f"{total_fires}")
col2.metric("🚨 Riesgo Extremo", f"{extreme_risk}")
col3.metric("⚠️ Riesgo Alto", f"{high_risk}")
col4.metric("🌡️ Temp. Media (Fuego)", f"{avg_temp:.1f} °C")

st.markdown("---")

# --- MAPA Y GRÁFICOS ---
c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("📍 Mapa de Situación")
    
    def get_color(risk):
        if risk == 'EXTREME': return [255, 0, 0, 200]
        if risk == 'VERY_HIGH': return [255, 69, 0, 180]
        if risk == 'HIGH': return [255, 140, 0, 160]
        if risk == 'MODERATE': return [255, 215, 0, 140]
        return [0, 255, 0, 140]

    map_data = df_filtered.copy()
    map_data = map_data.rename(columns={"fire_lat": "lat", "fire_lon": "lon"})
    map_data['color'] = map_data['risk_level'].apply(get_color)
    
    layer = pdk.Layer(
        "ColumnLayer",
        map_data,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_elevation="wind_speed * 200",
        elevation_scale=1,
        radius=10000,
        pickable=True,
        extruded=True,
    )

    view_state = pdk.ViewState(
        latitude=map_data['lat'].mean(),
        longitude=map_data['lon'].mean(),
        zoom=5.5,
        pitch=45,
        bearing=0
    )

    st.pydeck_chart(
        pdk.Deck(
            map_provider="carto",
            map_style="dark",
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"html": "<b>Ubicación:</b> {weather_station}<br/><b>Riesgo:</b> {risk_level}<br/><b>Viento:</b> {wind_speed} km/h"}
        ),
        width="stretch"
    )

with c2:
    st.subheader("📊 Distribución")
    
    chart = alt.Chart(df_filtered).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="risk_level", aggregate="count"),
        color=alt.Color(field="risk_level", scale=alt.Scale(
            domain=['EXTREME', 'VERY_HIGH', 'HIGH', 'MODERATE', 'LOW'],
            range=['#ff0000', '#ff4500', '#ff8c00', '#ffd700', '#00ff00']
        )),
        tooltip=['risk_level', 'count()']
    ).properties(height=250)
    
    st.altair_chart(chart, width="stretch")
    
    st.markdown("##### 🌪️ Zonas con más viento")
    st.dataframe(
        df_filtered[['weather_station', 'wind_speed', 'risk_level']]
        .sort_values('wind_speed', ascending=False)
        .head(5),
        hide_index=True,
        width="stretch"
    )
