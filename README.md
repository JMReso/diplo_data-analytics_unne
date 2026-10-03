# Pipeline ETL: Exportaciones de la Región NEA (1993–2024)
**Diplomatura en Ciencia de Datos y Analytics — UNNE**
**Trabajo Práctico Integrador — Unidad II**

---

## Descripción del Proyecto
Este proyecto implementa un pipeline **ETL (Extract, Transform, Load)** modular y automatizado en Python para procesar, limpiar, validar y estructurar las estadísticas de exportaciones de la región del **Noreste Argentino (NEA)**, conformada por las provincias de **Chaco, Corrientes, Formosa y Misiones**, abarcando el período **1993–2024**. El flujo descarga automáticamente los datos crudos desde el repositorio de datos abiertos (datos.gob.ar / INDEC), aplica reglas de negocio y limpieza de datos, y genera un dataset consolidado junto a una ficha técnica y registros de auditoría.

---

## Estructura del Repositorio
```text
diplo_data-analytics_unne/
│
├── data/
│   ├── raw/          # Datasets crudos descargados (CSV)
│   └── processed/    # Datasets finales procesados (exportaciones_nea.csv, resumen.json)
│
├── logs/
│   └── pipeline.log  # Historial de ejecución y auditoría
│
├── src/
│   ├── extract.py    # Extracción y gestión de fuentes de datos
│   ├── transform.py  # Limpieza, estandarización y transformación (13 columnas)
│   ├── load.py       # Validación de calidad, exportación a CSV y ficha JSON
│   └── main.py       # Script principal de orquestación del pipeline
│
├── tests/            # Pruebas unitarias de calidad
├── README.md         # Documentación general del proyecto
└── requirements.txt  # Dependencias del proyecto
```

---

## Módulos del Sistema

### 1. Extracción (`src/extract.py`)
Encargado de gestionar la descarga de las fuentes externas. Incluye un sistema de persistencia local y generación de datos sintéticos de respaldo para garantizar la ejecución en entornos sin conectividad a internet.

```python
import os
import ssl
import urllib.error
import pandas as pd

URL_DESTINOS = "https://githubusercontent.com"
URL_RUBROS = "https://githubusercontent.com"
PATH_DESTINOS = "data/raw/exportaciones_destinos_raw.csv"
PATH_RUBROS = "data/raw/exportaciones_rubros_raw.csv"

def generar_datos_locales_fallback() -> tuple[pd.DataFrame, pd.DataFrame]:
    print("Generando datos crudos locales en 'data/raw/' para modo offline...")
    provincias = ["Chaco", "Corrientes", "Formosa", "Misiones"]
    destinos = ["China", "Brasil", "Estados Unidos", "Uruguay", "Paraguay", "España", "Japón", "Chile"]
    anios = list(range(1993, 2025))
    
    filas_destinos = []
    for anio in anios:
        for prov in provincias:
            for dest in destinos:
                valor = round((hash(f"{anio}{prov}{dest}") % 10000) / 100 + 5.0, 2)
                filas_destinos.append({
                    "anio": anio,
                    "provincia": prov,
                    "destino": dest,
                    "valor_musd": valor
                })
    df_destinos = pd.DataFrame(filas_destinos)
    
    filas_rubros = []
    for anio in anios:
        for prov in provincias:
            filas_rubros.append({
                "anio": anio,
                "provincia": prov,
                "rubro_principal": "Productos primarios" if prov != "Misiones" else "Manufacturas de origen agropecuario",
                "pp_participacion_pct": 75.5 if prov != "Misiones" else 45.2
            })
    df_rubros = pd.DataFrame(filas_rubros)
    
    df_destinos.to_csv(PATH_DESTINOS, index=False, encoding="utf-8")
    df_rubros.to_csv(PATH_RUBROS, index=False, encoding="utf-8")
    return df_destinos, df_rubros

def extraer_datos() -> tuple[pd.DataFrame, pd.DataFrame]:
    os.makedirs("data/raw", exist_ok=True)
    ssl._create_default_https_context = ssl._create_unverified_context
    print("Intentando extraer datos crudos...")
    
    try:
        df_destinos = pd.read_csv(URL_DESTINOS)
        df_rubros = pd.read_csv(URL_RUBROS)
        df_destinos.to_csv(PATH_DESTINOS, index=False, encoding="utf-8")
        df_rubros.to_csv(PATH_RUBROS, index=False, encoding="utf-8")
        print("Datos crudos descargados y guardados en data/raw/ con éxito.")
    except (urllib.error.URLError, Exception) as e:
        print(f"Sin acceso a la red ({e}).")
        
        if os.path.exists(PATH_DESTINOS) and os.path.exists(PATH_RUBROS):
            df_destinos = pd.read_csv(PATH_DESTINOS)
            df_rubros = pd.read_csv(PATH_RUBROS)
            print("Datos leídos exitosamente desde los archivos locales en 'data/raw/'.")
        else:
            df_destinos, df_rubros = generar_datos_locales_fallback()
            print("Archivos crudos creados exitosamente en 'data/raw/'.")
            
    return df_destinos, df_rubros
```

### 2. Transformación (`src/transform.py`)
Contiene las reglas de negocio aplicadas al conjunto de datos, cálculos estadísticos agrupados por provincia y ventanas temporales, asignación de clasificaciones geoeconómicas y ordenamiento posicional.

```python
import pandas as pd
import numpy as np

COLUMNAS = [
    "anio",
    "provincia",
    "destino",
    "region_destino",
    "valor_musd",
    "total_provincia_musd",
    "participacion_pct",
    "var_interanual_pct",
    "decada",
    "ranking_destino",
    "es_top3",
    "rubro_principal",
    "pp_participacion_pct",
]

PROVINCIAS_NEA = ["Chaco", "Corrientes", "Formosa", "Misiones"]

MAPEO_REGIONES = {
    "Brasil": "Mercosur",
    "Uruguay": "Mercosur",
    "Paraguay": "Mercosur",
    "China": "Asia",
    "India": "Asia",
    "Japón": "Asia",
    "Estados Unidos": "América del Norte",
    "Canadá": "América del Norte",
    "México": "América del Norte",
    "España": "Unión Europea",
    "Alemania": "Unión Europea",
    "Italia": "Unión Europea",
    "Países Bajos": "Unión Europea",
    "Chile": "Resto de América",
    "Bolivia": "Resto de América",
    "Perú": "Resto de América",
    "Colombia": "Resto de América",
}

def limpiar_datos_crudos(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw[df_raw["provincia"].isin(PROVINCIAS_NEA)].copy()
    df["anio"] = df["anio"].astype(int)
    df["valor_musd"] = df["valor_musd"].astype(float)
    return df

def mapear_regiones(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["region_destino"] = df["destino"].map(MAPEO_REGIONES).fillna("Resto")
    return df

def calcular_totales_y_participacion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["total_provincia_musd"] = df.groupby(["provincia", "anio"])["valor_musd"].transform("sum")
    df["participacion_pct"] = (df["valor_musd"] / df["total_provincia_musd"]) * 100
    return df

def calcular_variacion_interanual(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["provincia", "destino", "anio"]).reset_index(drop=True)
    df["var_interanual_pct"] = (
        df.groupby(["provincia", "destino"])["valor_musd"]
        .pct_change() * 100
    )
    return df

def asignar_decada(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["decada"] = ((df["anio"] // 10) * 10).astype(str) + "s"
    return df

def calcular_ranking_destinos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ranking_destino"] = (
        df.groupby(["provincia", "anio"])["valor_musd"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    df["es_top3"] = df["ranking_destino"] <= 3
    return df

def unir_datos_rubros(df_destinos: pd.DataFrame, df_rubros: pd.DataFrame) -> pd.DataFrame:
    df = df_destinos.copy()
    df = df.merge(
        df_rubros[["anio", "provincia", "rubro_principal", "pp_participacion_pct"]],
        on=["anio", "provincia"],
        how="left"
    )
    return df

def transformar_datos(df_destinos_raw: pd.DataFrame, df_rubros_raw: pd.DataFrame) -> pd.DataFrame:
    df = limpiar_datos_crudos(df_destinos_raw)
    df = mapear_regiones(df)
    df = calcular_totales_y_participacion(df)
    df = calcular_variacion_interanual(df)
    df = asignar_decada(df)
    df = calcular_ranking_destinos(df)
    df = unir_datos_rubros(df, df_rubros_raw)
    return df[COLUMNAS]
```

### 3. Carga (`src/load.py`)
Módulo encargado de ejecutar los controles de calidad sobre la estructura final, exportar los resultados a CSV, generar una ficha técnica en formato JSON y registrar las auditorías de ejecución.

```python
import os
import json
import logging
from datetime import datetime
import pandas as pd

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

def ejecutar_controles_calidad(df: pd.DataFrame) -> bool:
    if df.empty:
        raise ValueError("Control fallido: El DataFrame está vacío.")
        
    if len(df.columns) != 13:
        raise ValueError(f"Control fallido: Se esperaban 13 columnas, pero se encontraron {len(df.columns)}.")
        
    provincias_validas = {"Chaco", "Corrientes", "Formosa", "Misiones"}
    provincias_df = set(df["provincia"].unique())
    if not provincias_df.issubset(provincias_validas):
        raise ValueError(f"Control fallido: Hay provincias fuera del NEA: {provincias_df - provincias_validas}")
        
    print("Todos los controles de calidad pasaron exitosamente.")
    return True

def guardar_csv(df: pd.DataFrame, ruta_csv: str = "data/processed/exportaciones_nea.csv") -> None:
    os.makedirs(os.path.dirname(ruta_csv), exist_ok=True)
    df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
    print(f"CSV guardado exitosamente en: {ruta_csv}")

def generar_resumen_json(df: pd.DataFrame, ruta_json: str = "data/processed/resumen.json") -> None:
