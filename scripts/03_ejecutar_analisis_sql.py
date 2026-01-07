"""
Script para ejecutar queries SQL y exportar resultados
"""

import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def ejecutar_query_y_exportar(conn, nombre_query, query_sql, exportar_csv=True):
    """Ejecuta query y opcionalmente exporta a CSV"""
    print(f"\n{'='*80}")
    print(f"Ejecutando: {nombre_query}")
    print(f"{'='*80}")
    
    try:
        df = pd.read_sql_query(query_sql, conn)
        
        print(f" Query ejecutada exitosamente")
        print(f"Filas retornadas: {len(df)}")
        
        if len(df) > 0:
            print("\nPrimeras filas:")
            print(df.head(10).to_string())
        
        if exportar_csv and len(df) > 0:
            filename = f"data/processed/analisis_{nombre_query.lower().replace(' ', '_')}.csv"
            df.to_csv(filename, index=False)
            print(f"\n Exportado a: {filename}")
        
        return df
        
    except Exception as e:
        print(f" Error al ejecutar query: {e}")
        return None

def main():
    print("="*80)
    print("CRYPTOOPS ANALYZER - EJECUCIÓN DE ANÁLISIS SQL")
    print("="*80)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Query 1: Overview general
        query1 = """
        SELECT 
            COUNT(*) as total_transacciones,
            COUNT(DISTINCT user_id) as usuarios_activos,
            COUNT(CASE WHEN estado = 'exitosa' THEN 1 END) as transacciones_exitosas,
            COUNT(CASE WHEN estado = 'fallida' THEN 1 END) as transacciones_fallidas,
            ROUND(COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*), 2) as tasa_error_pct,
            SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
            ROUND(AVG(CASE WHEN estado = 'exitosa' THEN monto_usd END), 2) as ticket_promedio_usd,
            ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg
        FROM transacciones;
        """
        df_overview = ejecutar_query_y_exportar(conn, "Overview General", query1)
        
        # Query 2: Análisis por hora
        query2 = """
        SELECT 
            EXTRACT(HOUR FROM timestamp_inicio) as hora_del_dia,
            COUNT(*) as num_transacciones,
            ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tiempo_procesamiento), 2) as tiempo_p95_seg,
            COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
        FROM transacciones
        WHERE estado IN ('exitosa', 'fallida')
        GROUP BY EXTRACT(HOUR FROM timestamp_inicio)
        ORDER BY hora_del_dia;
        """
        df_por_hora = ejecutar_query_y_exportar(conn, "Analisis Por Hora", query2)
        
        # Query 3: Transacciones lentas
        query3 = """
        SELECT 
            transaction_id,
            user_id,
            tipo_operacion,
            cripto,
            monto_usd,
            tiempo_procesamiento,
            timestamp_inicio,
            estado,
            requiere_validacion_manual
        FROM transacciones
        WHERE tiempo_procesamiento > 300
        ORDER BY tiempo_procesamiento DESC
        LIMIT 100;
        """
        df_lentas = ejecutar_query_y_exportar(conn, "Transacciones Lentas", query3)
        
        # Query 4: Performance por cripto
        query4 = """
        SELECT 
            cripto,
            COUNT(*) as num_transacciones,
            SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
            ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg,
            COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
        FROM transacciones
        GROUP BY cripto
        ORDER BY volumen_total_usd DESC;
        """
        df_por_cripto = ejecutar_query_y_exportar(conn, "Performance Por Cripto", query4)
        
        # Query 5: Motivos de fallo
        query5 = """
        SELECT 
            motivo_fallo,
            COUNT(*) as num_fallos,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje
        FROM transacciones
        WHERE estado = 'fallida'
        GROUP BY motivo_fallo
        ORDER BY num_fallos DESC;
        """
        df_fallos = ejecutar_query_y_exportar(conn, "Motivos De Fallo", query5)
        
        print(f"\n{'='*80}")
        print(" ANÁLISIS SQL COMPLETADO EXITOSAMENTE")
        print(f"{'='*80}")
        print(f"Archivos generados en: data/processed/")
        
    except Exception as e:
        print(f"\n Error: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()