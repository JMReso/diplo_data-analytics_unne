import pandas as pd
import numpy as np

# Contrato obligatorio de las 13 columnas requeridas en el archivo final
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

# Provincias del NEA requeridas
PROVINCIAS_NEA = ["Chaco", "Corrientes", "Formosa", "Misiones"]

def limpiar_datos_crudos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Limpia tipos de datos y filtra provincias del NEA."""
    # Filtrar las provincias pertenecientes a PROVINCIAS_NEA
    df = df_raw[df_raw["provincia"].isin(PROVINCIAS_NEA)].copy()
    
    # Convertir tipos de datos de forma robusta
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype(int)
    df["valor_musd"] = pd.to_numeric(df["valor_musd"], errors="coerce").astype(float)
    
    return df

# Diccionario de referencia para mapear destinos a regiones geoeconómicas
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

def mapear_regiones(df: pd.DataFrame) -> pd.DataFrame:
    """Asigna la región geoeconómica según el país de destino."""
    df = df.copy()
    
    # Asignar la región utilizando el mapa y asignar "Resto" a destinos no especificados
    df["region_destino"] = df["destino"].map(MAPEO_REGIONES).fillna("Resto")
    
    return df


def calcular_totales_y_participacion(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el total exportado por provincia/año y el % de participación."""
    df = df.copy()
    
    # Sumar el total de valor_musd agrupando por provincia y año
    df["total_provincia_musd"] = df.groupby(["provincia", "anio"])["valor_musd"].transform("sum")
    
    # Calcular el porcentaje de participación individual sobre el total
    df["participacion_pct"] = (df["valor_musd"] / df["total_provincia_musd"]) * 100
    
    return df

def calcular_variacion_interanual(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula la variación porcentual interanual respecto al año anterior."""
    df = df.copy()
    
    # Ordenar el DataFrame para asegurar el orden cronológico
    df = df.sort_values(["provincia", "destino", "anio"]).reset_index(drop=True)
    
    # Calcular el cambio porcentual interanual por combinación de provincia y destino
    df["var_interanual_pct"] = (
        df.groupby(["provincia", "destino"])["valor_musd"]
        .pct_change() * 100
    )
    
    return df


def asignar_decada(df: pd.DataFrame) -> pd.DataFrame:
    """Etiqueta los años según la década correspondiente (1990s, 2000s, etc.)."""
    df = df.copy()
    
    # Asignar la década formateada (ej. 1990s, 2000s, 2010s, 2020s)
    df["decada"] = ((df["anio"] // 10) * 10).astype(str) + "s"
    
    return df

def calcular_ranking_destinos(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula la posición ordinal por valor exportado y determina si es Top 3."""
    df = df.copy()
    
    # Calcular ranking descendente por provincia y año según el valor_musd
    df["ranking_destino"] = (
        df.groupby(["provincia", "anio"])["valor_musd"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    
    # Determinar si el destino está entre los 3 principales de esa provincia en ese año
    df["es_top3"] = df["ranking_destino"] <= 3
    
    return df


def unir_datos_rubros(df_destinos: pd.DataFrame, df_rubros: pd.DataFrame) -> pd.DataFrame:
    """Realiza el merge con el dataset de rubros exportados."""
    df = df_destinos.copy()
    
    # Realizar la unión (left join) por año y provincia
    df = df.merge(
        df_rubros[["anio", "provincia", "rubro_principal", "pp_participacion_pct"]],
        on=["anio", "provincia"],
        how="left"
    )
    
    return df


def transformar_datos(df_destinos_raw: pd.DataFrame, df_rubros_raw: pd.DataFrame) -> pd.DataFrame:
    """Orquesta la ejecución secuencial de todas las transformaciones."""
    # 1. Limpieza y filtrado del NEA
    df = limpiar_datos_crudos(df_destinos_raw)
    
    # 2. Mapeo de regiones geoeconómicas
    df = mapear_regiones(df)
    
    # 3. Totales por provincia/año y % de participación
    df = calcular_totales_y_participacion(df)
    
    # 4. Variación interanual porcentual
    df = calcular_variacion_interanual(df)
    
    # 5. Asignación de década
    df = asignar_decada(df)
    
    # 6. Ranking de destinos y booleano Top 3
    df = calcular_ranking_destinos(df)
    
    # 7. Unión con el dataset de rubros
    df = unir_datos_rubros(df, df_rubros_raw)
    
    # 8. Retornar con el orden estricto de las 13 columnas del contrato
    return df[COLUMNAS]

