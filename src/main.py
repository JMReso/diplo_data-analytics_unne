from extract import extraer_datos
from transform import transformar_datos
from load import cargar_datos

def main():
    print("Iniciando el pipeline ETL...")
    
    print("\n1 Etapa: Extracción de datos crudos...")
    df_destinos_raw, df_rubros_raw = extraer_datos()
    
    print("\n2 Etapa: Transformación y limpieza...")
    df_procesado = transformar_datos(df_destinos_raw, df_rubros_raw)
    
    print("\n3 Etapa: Carga, validación y generación de reportes...")
    cargar_datos(df_procesado)
    
    print("\n¡Pipeline ETL ejecutado y completado con éxito!")

if __name__ == "__main__":
    main()
