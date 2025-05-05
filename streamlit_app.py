import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import os

# Título
st.title("🚲 Cycle World Dashboard Unificado")

# Verificación de credenciales
if not all([os.getenv("SNOWFLAKE_ACCOUNT"), os.getenv("SNOWFLAKE_USER"), os.getenv("SNOWFLAKE_PASSWORD")]):
    st.error("❌ Faltan credenciales para conectarse a Snowflake. Verifica tus secrets en Streamlit Cloud.")
    st.stop()

# Conexión a Snowflake
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "CYCLE_WORLD",
    "schema": "PUBLIC"
}
session = Session.builder.configs(connection_parameters).create()

# Carga de datos
st.cache_data()
def cargar_datos():
    df_journeys = session.sql("""
        SELECT
            JOURNEY_ID,
            BIKE_ID,
            START_STATION_ID,
            END_STATION_ID,
            START_DATE, START_MONTH, START_YEAR,
            END_DATE, END_MONTH, END_YEAR,
            TO_DATE(
                LPAD(START_DATE::STRING, 2, '0') || '/' ||
                LPAD(START_MONTH::STRING, 2, '0') || '/' ||
                (CASE WHEN START_YEAR < 100 THEN START_YEAR + 2000 ELSE START_YEAR END)::STRING,
                'DD/MM/YYYY'
            ) AS FECHA_INICIO,
            TO_DATE(
                LPAD(END_DATE::STRING, 2, '0') || '/' ||
                LPAD(END_MONTH::STRING, 2, '0') || '/' ||
                (CASE WHEN END_YEAR < 100 THEN END_YEAR + 2000 ELSE END_YEAR END)::STRING,
                'DD/MM/YYYY'
            ) AS FECHA_FIN
        FROM JOURNEYS
    """).to_pandas()

    df_weather = session.table("WEATHER").to_pandas()
    df_stations = session.table("STATIONS").to_pandas()
    df_bikes = session.table("BIKES").to_pandas()

    return df_journeys, df_weather, df_stations, df_bikes

df_journeys, df_weather, df_stations, df_bikes = cargar_datos()

# Conversión de fechas a datetime
df_journeys["FECHA_INICIO"] = pd.to_datetime(df_journeys["FECHA_INICIO"])
df_journeys["FECHA_FIN"] = pd.to_datetime(df_journeys["FECHA_FIN"])

# Sidebar: Filtro global por fecha
with st.sidebar:
    st.header("Filtros Globales")
    fecha_min = df_journeys["FECHA_INICIO"].min()
    fecha_max = df_journeys["FECHA_INICIO"].max()
    fecha_inicio = st.date_input("Fecha de inicio", fecha_min)
    fecha_fin = st.date_input("Fecha de fin", fecha_max)

# Filtro aplicado
df_filtrado = df_journeys[
    (df_journeys["FECHA_INICIO"] >= pd.to_datetime(fecha_inicio)) &
    (df_journeys["FECHA_INICIO"] <= pd.to_datetime(fecha_fin))
]

# Reporte: Resumen de viajes
st.subheader("📊 Resumen de viajes filtrados")
st.write(df_filtrado.head())

# Reporte: Top 10 estaciones más usadas
st.subheader("🏙️ Top 10 estaciones (inicio)")
viajes_por_estacion = df_filtrado["START_STATION_ID"].value_counts().head(10)
st.bar_chart(viajes_por_estacion)

# Análisis cruzado con clima
st.subheader("🌧️ Porcentaje de viajes en días lluviosos")
df_weather["FECHA"] = pd.to_datetime(df_weather["DATETIME"]).dt.date
df_filtrado["FECHA"] = df_filtrado["FECHA_INICIO"].dt.date
df_con_clima = pd.merge(df_filtrado, df_weather, on="FECHA", how="left")
dias_lluvia = df_con_clima[df_con_clima["weather"].isin([3, 4])]
porcentaje_lluvia = (len(dias_lluvia) / len(df_con_clima)) * 100
st.metric("% viajes bajo lluvia", f"{porcentaje_lluvia:.2f}%")

