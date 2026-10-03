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
    os.makedirs(os.path.dirname(ruta_json), exist_ok=True)
    
    resumen = {
        "fuente": "API Series de Tiempo (datos.gob.ar / INDEC)",
        "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cantidad_filas": int(len(df)),
        "cantidad_columnas": int(len(df.columns)),
        "provincias_incluidas": list(df["provincia"].unique()),
        "anios_cubiertos": {
            "min": int(df["anio"].min()),
            "max": int(df["anio"].max()),
        },
        "total_exportado_musd": float(df["valor_musd"].sum()),
        "controles_calidad": "OK",
    }
    
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=4, ensure_ascii=False)
    print(f"Ficha técnica JSON guardada en: {ruta_json}")

def registrar_log(mensaje: str) -> None:
    logging.info(mensaje)

def cargar_datos(df: pd.DataFrame) -> None:
    ejecutar_controles_calidad(df)
    guardar_csv(df)
    generar_resumen_json(df)
    registrar_log(f"Pipeline ejecutado con éxito. Filas procesadas: {len(df)}")
