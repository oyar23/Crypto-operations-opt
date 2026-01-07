"""
CRYPTOOPS ANALYZER - Generador de Datos Realistas
Genera datos simulados para análisis de operaciones crypto

Este script genera:
- 10,000 usuarios con distribución realista
- 100,000 transacciones con patrones de uso reales
- Métricas operativas agregadas
- Logs del sistema
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Cargar variables de entorno
load_dotenv()

# Inicializar Faker
fake = Faker('es_AR')  # Español Argentina
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Configuración
NUM_USUARIOS = 10000
NUM_TRANSACCIONES = 100000
FECHA_INICIO = datetime(2024, 7, 1)
FECHA_FIN = datetime(2024, 12, 31)

# Configuración de base de datos
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# ============================================
# GENERACIÓN DE USUARIOS
# ============================================

def generar_usuarios(n=NUM_USUARIOS):
    """
    Genera dataset de usuarios con distribución realista
    
    Distribución:
    - 60% Argentina, 20% México, 10% Colombia, 10% Chile
    - 50% básico, 30% intermedio, 20% completo
    - 95% cuentas activas, 4% suspendidas, 1% cerradas
    """
    print(f"\n{'='*80}")
    print(f"GENERANDO {n:,} USUARIOS")
    print(f"{'='*80}")
    
    usuarios = []
    
    # Distribuciones
    paises = ['Argentina', 'México', 'Colombia', 'Chile']
    prob_paises = [0.6, 0.2, 0.1, 0.1]
    
    niveles = ['basico', 'intermedio', 'completo']
    prob_niveles = [0.5, 0.3, 0.2]
    
    estados = ['activa', 'suspendida', 'cerrada']
    prob_estados = [0.95, 0.04, 0.01]
    
    ciudades_por_pais = {
        'Argentina': ['Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'Tucumán'],
        'México': ['Ciudad de México', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana'],
        'Colombia': ['Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena'],
        'Chile': ['Santiago', 'Valparaíso', 'Concepción', 'Viña del Mar', 'Antofagasta']
    }
    
    print("Generando datos de usuarios...")
    
    for i in tqdm(range(1, n + 1), desc="Usuarios"):
        pais = np.random.choice(paises, p=prob_paises)
        ciudad = random.choice(ciudades_por_pais[pais])
        nivel = np.random.choice(niveles, p=prob_niveles)
        estado = np.random.choice(estados, p=prob_estados)
        
        # Fecha de registro: últimos 18 meses con distribución exponencial (más recientes)
        dias_atras = int(np.random.exponential(180))
        dias_atras = min(dias_atras, 545)  # máximo 18 meses
        fecha_registro = FECHA_FIN - timedelta(days=dias_atras)
        
        # Fecha de verificación (si no es básico)
        fecha_verificacion = None
        if nivel != 'basico':
            dias_despues_registro = random.randint(1, 30)
            fecha_verificacion = fecha_registro + timedelta(days=dias_despues_registro)
        
        usuario = {
            'user_id': i,
            'username': f"{fake.user_name()}{random.randint(10, 99)}_{i}",  # f-string para agregar el ID (i) y garantizar que sea único
            'email': fake.unique.email(),
            'fecha_registro': fecha_registro,
            'pais': pais,
            'ciudad': ciudad,
            'nivel_verificacion': nivel,
            'fecha_ultima_verificacion': fecha_verificacion,
            'estado_cuenta': estado
        }
        
        usuarios.append(usuario)
    
    df_usuarios = pd.DataFrame(usuarios)
    
    # Estadísticas
    print(f"\n {len(df_usuarios):,} usuarios generados")
    print(f"\nDistribución por país:")
    print(df_usuarios['pais'].value_counts())
    print(f"\nDistribución por nivel:")
    print(df_usuarios['nivel_verificacion'].value_counts())
    print(f"\nDistribución por estado:")
    print(df_usuarios['estado_cuenta'].value_counts())
    
    return df_usuarios

# ============================================
# GENERACIÓN DE TRANSACCIONES
# ============================================

def generar_transacciones(df_usuarios, n=NUM_TRANSACCIONES):
    """
    Genera transacciones con patrones realistas:
    - Más actividad en horas pico (18-23hs)
    - Usuarios verificados hacen más transacciones
    - Mayor tasa de error en horas pico
    - Diferentes velocidades de procesamiento
    """
    print(f"\n{'='*80}")
    print(f"GENERANDO {n:,} TRANSACCIONES")
    print(f"{'='*80}")
    
    transacciones = []
    
    # Parámetros
    tipos_operacion = ['compra', 'venta', 'swap', 'retiro']
    prob_operacion = [0.45, 0.35, 0.15, 0.05]
    
    criptos = ['BTC', 'ETH', 'USDT', 'USDC', 'BNB', 'ADA', 'SOL']
    prob_criptos = [0.30, 0.25, 0.20, 0.15, 0.05, 0.03, 0.02]
    
    metodos_pago = ['transferencia', 'tarjeta', 'wallet_crypto']
    prob_metodos = [0.50, 0.35, 0.15]
    
    networks = ['Bitcoin', 'Ethereum', 'Binance Smart Chain', 'Polygon', 'Tron']
    
    # Precios aproximados en USD (para simulación)
    precios_cripto = {
        'BTC': 45000,
        'ETH': 2500,
        'USDT': 1,
        'USDC': 1,
        'BNB': 350,
        'ADA': 0.5,
        'SOL': 100
    }
    
    # Usuarios activos (solo cuentas activas)
    usuarios_activos = df_usuarios[df_usuarios['estado_cuenta'] == 'activa']['user_id'].tolist()
    
    print("Generando transacciones con patrones realistas...")
    
    for i in tqdm(range(1, n + 1), desc="Transacciones"):
        # Seleccionar usuario (usuarios con nivel superior hacen más transacciones)
        user_id = random.choice(usuarios_activos)
        usuario = df_usuarios[df_usuarios['user_id'] == user_id].iloc[0]
        
        # Fecha y hora de la transacción
        # Más transacciones en días de semana y horario 18-23hs
        dias_desde_inicio = random.randint(0, (FECHA_FIN - FECHA_INICIO).days)
        fecha_base = FECHA_INICIO + timedelta(days=dias_desde_inicio)
        
        # Asegurarse que la transacción es después del registro del usuario
        if fecha_base < usuario['fecha_registro']:
            fecha_base = usuario['fecha_registro'] + timedelta(days=random.randint(0, 30))
        
        # Distribución de horas (más actividad en horario pico)
        if random.random() < 0.6:  # 60% en horario pico
            hora = random.randint(18, 23)
        else:
            hora = random.randint(0, 23)
        
        timestamp_inicio = fecha_base.replace(
            hour=hora,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        
        # Tipo de operación y cripto
        tipo_operacion = np.random.choice(tipos_operacion, p=prob_operacion)
        cripto = np.random.choice(criptos, p=prob_criptos)
        
        # Monto según nivel de verificación
        if usuario['nivel_verificacion'] == 'basico':
            monto_usd = round(random.uniform(10, 1000), 2)
        elif usuario['nivel_verificacion'] == 'intermedio':
            monto_usd = round(random.uniform(100, 5000), 2)
        else:  # completo
            monto_usd = round(random.uniform(500, 20000), 2)
        
        # Calcular cantidad de cripto
        precio_unitario = precios_cripto[cripto] * random.uniform(0.98, 1.02)  # variación ±2%
        cantidad_cripto = monto_usd / precio_unitario
        
        # Comisión (0.5%)
        comision_usd = round(monto_usd * 0.005, 2)
        monto_total_usd = monto_usd + comision_usd
        
        # Tiempo de procesamiento
        # Horario pico: más lento
        # Montos altos: requieren validación, más lentos
        es_hora_pico = hora >= 18 and hora <= 23
        requiere_validacion = monto_usd > 5000
        
        if es_hora_pico:
            tiempo_base = random.randint(60, 150)
        else:
            tiempo_base = random.randint(20, 60)
        
        if requiere_validacion:
            tiempo_base += random.randint(30, 120)
        
        # Añadir variabilidad
        tiempo_procesamiento = max(10, int(np.random.normal(tiempo_base, 20)))
        
        timestamp_completado = timestamp_inicio + timedelta(seconds=tiempo_procesamiento)
        
        # Estado de la transacción
        # Mayor tasa de error en hora pico y transacciones grandes
        tasa_base_error = 0.05
        if es_hora_pico:
            tasa_base_error += 0.10
        if requiere_validacion:
            tasa_base_error += 0.05
        
        if random.random() < tasa_base_error:
            estado = 'fallida'
            motivos_fallo = [
                'Fondos insuficientes',
                'Límite diario excedido',
                'Validación de identidad fallida',
                'Timeout de la red blockchain',
                'Error en validación antifraude',
                'Método de pago rechazado'
            ]
            motivo_fallo = random.choice(motivos_fallo)
            timestamp_completado = timestamp_inicio + timedelta(seconds=random.randint(5, 30))
        else:
            estado = 'exitosa'
            motivo_fallo = None
        
        # Método de pago
        metodo_pago = np.random.choice(metodos_pago, p=prob_metodos)
        
        # Network blockchain
        if cripto in ['BTC']:
            network = 'Bitcoin'
        elif cripto in ['ETH', 'USDT', 'USDC']:
            network = random.choice(['Ethereum', 'Polygon', 'Binance Smart Chain'])
        else:
            network = random.choice(networks)
        
        # Hash blockchain (solo si exitosa)
        hash_blockchain = None
        confirmaciones = 0
        if estado == 'exitosa':
            hash_blockchain = fake.sha256()
            confirmaciones = random.randint(1, 12)
        
        # Score de fraude
        score_fraude = random.uniform(0, 100)
        if monto_usd > 10000:
            score_fraude = min(100, score_fraude + random.uniform(10, 30))
        
        flagged_fraude = score_fraude > 75
        
        # IP y user agent
        ip_address = fake.ipv4()
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)',
            'Mozilla/5.0 (Android 11; Mobile)',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        ]
        user_agent = random.choice(user_agents)
        
        dispositivos = ['Desktop', 'Mobile Android', 'Mobile iOS', 'Tablet']
        dispositivo = random.choice(dispositivos)
        
        # Cripto destino para swaps
        cripto_destino = None
        if tipo_operacion == 'swap':
            criptos_swap = [c for c in criptos if c != cripto]
            cripto_destino = random.choice(criptos_swap)
        
        transaccion = {
            'transaction_id': i,
            'user_id': user_id,
            'tipo_operacion': tipo_operacion,
            'cripto': cripto,
            'cripto_destino': cripto_destino,
            'cantidad_cripto': round(cantidad_cripto, 8),
            'precio_unitario_usd': round(precio_unitario, 2),
            'monto_usd': monto_usd,
            'comision_usd': comision_usd,
            'monto_total_usd': monto_total_usd,
            'timestamp_inicio': timestamp_inicio,
            'timestamp_completado': timestamp_completado,
            'tiempo_procesamiento': tiempo_procesamiento,
            'estado': estado,
            'motivo_fallo': motivo_fallo,
            'requiere_validacion_manual': requiere_validacion,
            'metodo_pago': metodo_pago,
            'network': network,
            'hash_blockchain': hash_blockchain,
            'confirmaciones_blockchain': confirmaciones,
            'score_fraude': round(score_fraude, 2),
            'flagged_fraude': flagged_fraude,
            'ip_address': ip_address,
            'user_agent': user_agent,
            'dispositivo': dispositivo
        }
        
        transacciones.append(transaccion)
    
    df_transacciones = pd.DataFrame(transacciones)
    
    # Estadísticas
    print(f"\n {len(df_transacciones):,} transacciones generadas")
    print(f"\nDistribución por tipo:")
    print(df_transacciones['tipo_operacion'].value_counts())
    print(f"\nDistribución por estado:")
    print(df_transacciones['estado'].value_counts())
    print(f"\nDistribución por cripto:")
    print(df_transacciones['cripto'].value_counts())
    print(f"\nEstadísticas de tiempo de procesamiento:")
    print(df_transacciones['tiempo_procesamiento'].describe())
    
    return df_transacciones

# ============================================
# GENERACIÓN DE MÉTRICAS OPERATIVAS
# ============================================

def generar_metricas_operativas(df_transacciones):
    """
    Genera métricas agregadas por hora para análisis de performance
    """
    print(f"\n{'='*80}")
    print("GENERANDO MÉTRICAS OPERATIVAS")
    print(f"{'='*80}")
    
    # Extraer fecha y hora
    df_transacciones['fecha'] = df_transacciones['timestamp_inicio'].dt.date
    df_transacciones['hora'] = df_transacciones['timestamp_inicio'].dt.hour
    
    print("Agregando métricas por fecha-hora...")
    
    # Agrupar por fecha y hora
    metricas = df_transacciones.groupby(['fecha', 'hora']).agg({
        'transaction_id': 'count',
        'user_id': 'nunique',
        'tiempo_procesamiento': ['mean', 'median', lambda x: x.quantile(0.95), 'max'],
        'monto_usd': ['sum', 'mean'],
        'comision_usd': 'sum'
    }).reset_index()
    
    # Aplanar columnas
    metricas.columns = [
        'fecha', 'hora', 'num_transacciones', 'num_usuarios_activos',
        'tiempo_promedio_procesamiento', 'tiempo_mediano_procesamiento',
        'tiempo_p95_procesamiento', 'tiempo_max_procesamiento',
        'volumen_total_usd', 'volumen_promedio_usd', 'comisiones_totales_usd'
    ]
    
    # Contar por estado
    exitosas = df_transacciones[df_transacciones['estado'] == 'exitosa'].groupby(['fecha', 'hora']).size()
    fallidas = df_transacciones[df_transacciones['estado'] == 'fallida'].groupby(['fecha', 'hora']).size()
    
    metricas = metricas.merge(
        exitosas.reset_index(name='num_transacciones_exitosas'),
        on=['fecha', 'hora'],
        how='left'
    )
    metricas = metricas.merge(
        fallidas.reset_index(name='num_transacciones_fallidas'),
        on=['fecha', 'hora'],
        how='left'
    )
    
    metricas['num_transacciones_exitosas'].fillna(0, inplace=True)
    metricas['num_transacciones_fallidas'].fillna(0, inplace=True)
    
    # Calcular tasas
    metricas['tasa_error'] = (
        metricas['num_transacciones_fallidas'] / metricas['num_transacciones'] * 100
    ).round(2)
    
    # Contar por tipo de operación
    por_tipo = df_transacciones.groupby(['fecha', 'hora', 'tipo_operacion']).size().unstack(fill_value=0)
    por_tipo.columns = ['num_' + col if col != 'transferencia' else 'num_transferencias' for col in por_tipo.columns]
    por_tipo = por_tipo.reset_index()
    
    metricas = metricas.merge(por_tipo, on=['fecha', 'hora'], how='left')
    
    # Rellenar NaNs
    for col in metricas.columns:
        if 'num_' in col:
            metricas[col].fillna(0, inplace=True)
    
    # Validaciones manuales y fraude
    validaciones_manuales = df_transacciones[
        df_transacciones['requiere_validacion_manual']
    ].groupby(['fecha', 'hora']).size()
    
    metricas = metricas.merge(
        validaciones_manuales.reset_index(name='num_validaciones_manuales'),
        on=['fecha', 'hora'],
        how='left'
    )
    metricas['num_validaciones_manuales'].fillna(0, inplace=True)
    
    metricas['tasa_validacion_manual'] = (
        metricas['num_validaciones_manuales'] / metricas['num_transacciones'] * 100
    ).round(2)
    
    fraudes = df_transacciones[
        df_transacciones['flagged_fraude']
    ].groupby(['fecha', 'hora']).size()
    
    metricas = metricas.merge(
        fraudes.reset_index(name='num_fraudes'),
        on=['fecha', 'hora'],
        how='left'
    )
    metricas['num_fraudes'].fillna(0, inplace=True)
    
    metricas['tasa_fraude'] = (
        metricas['num_fraudes'] / metricas['num_transacciones'] * 100
    ).round(2)
    
    print(f"\n {len(metricas):,} registros de métricas generados")
    print(f"Periodo: {metricas['fecha'].min()} a {metricas['fecha'].max()}")
    
    return metricas

# ============================================
# CARGA DE DATOS A BASE DE DATOS
# ============================================

def cargar_usuarios_db(df_usuarios):
    """Carga usuarios en la base de datos"""
    print(f"\n{'='*80}")
    print("CARGANDO USUARIOS A BASE DE DATOS")
    print(f"{'='*80}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Preparar datos para inserción
        datos = []
        for _, row in df_usuarios.iterrows():
            #  Si la fecha es NaT (vacía), enviamos None a la base de datos
            fecha_verif = row['fecha_ultima_verificacion']
            if pd.isna(fecha_verif):
                fecha_verif = None
                
            datos.append((
                row['username'],
                row['email'],
                row['fecha_registro'],
                row['pais'],
                row['ciudad'],
                row['nivel_verificacion'],
                fecha_verif, 
                row['estado_cuenta']
            ))
        
        query = """
            INSERT INTO usuarios 
            (username, email, fecha_registro, pais, ciudad, nivel_verificacion, 
             fecha_ultima_verificacion, estado_cuenta)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        print("Insertando usuarios...")
        execute_batch(cursor, query, datos, page_size=1000)
        
        conn.commit()
        print(f" {len(datos):,} usuarios insertados exitosamente")
        
    except Exception as e:
        conn.rollback()
        print(f" Error al insertar usuarios: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def cargar_transacciones_db(df_transacciones):
    """Carga transacciones en la base de datos"""
    print(f"\n{'='*80}")
    print("CARGANDO TRANSACCIONES A BASE DE DATOS")
    print(f"{'='*80}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Preparar datos para inserción
        datos = [
            (
                row['user_id'],
                row['tipo_operacion'],
                row['cripto'],
                row['cripto_destino'],
                row['cantidad_cripto'],
                row['precio_unitario_usd'],
                row['monto_usd'],
                row['comision_usd'],
                row['monto_total_usd'],
                row['timestamp_inicio'],
                row['timestamp_completado'],
                row['tiempo_procesamiento'],
                row['estado'],
                row['motivo_fallo'],
                row['requiere_validacion_manual'],
                row['metodo_pago'],
                row['network'],
                row['hash_blockchain'],
                row['confirmaciones_blockchain'],
                row['score_fraude'],
                row['flagged_fraude'],
                row['ip_address'],
                row['user_agent'],
                row['dispositivo']
            )
            for _, row in df_transacciones.iterrows()
        ]
        
        query = """
            INSERT INTO transacciones 
            (user_id, tipo_operacion, cripto, cripto_destino, cantidad_cripto,
             precio_unitario_usd, monto_usd, comision_usd, monto_total_usd,
             timestamp_inicio, timestamp_completado, tiempo_procesamiento,
             estado, motivo_fallo, requiere_validacion_manual, metodo_pago,
             network, hash_blockchain, confirmaciones_blockchain,
             score_fraude, flagged_fraude, ip_address, user_agent, dispositivo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        print("Insertando transacciones (esto puede tomar varios minutos)...")
        
        # Insertar en lotes con barra de progreso
        batch_size = 5000
        total_batches = len(datos) // batch_size + 1
        
        for i in tqdm(range(0, len(datos), batch_size), total=total_batches, desc="Lotes"):
            batch = datos[i:i + batch_size]
            execute_batch(cursor, query, batch, page_size=1000)
            conn.commit()
        
        print(f" {len(datos):,} transacciones insertadas exitosamente")
        
    except Exception as e:
        conn.rollback()
        print(f" Error al insertar transacciones: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def cargar_metricas_db(df_metricas):
    """Carga métricas operativas en la base de datos"""
    print(f"\n{'='*80}")
    print("CARGANDO MÉTRICAS OPERATIVAS A BASE DE DATOS")
    print(f"{'='*80}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Preparar datos para inserción
        datos = [
            (
                row['fecha'],
                row['hora'],
                row['num_transacciones'],
                row['num_transacciones_exitosas'],
                row['num_transacciones_fallidas'],
                row['num_usuarios_activos'],
                row['tiempo_promedio_procesamiento'],
                row['tiempo_mediano_procesamiento'],
                row['tiempo_p95_procesamiento'],
                row['tiempo_max_procesamiento'],
                row['tasa_error'],
                row['tasa_validacion_manual'],
                row['tasa_fraude'],
                row['volumen_total_usd'],
                row['volumen_promedio_usd'],
                row['comisiones_totales_usd'],
                row.get('num_compra', 0),
                row.get('num_venta', 0),
                row.get('num_swap', 0),
                row.get('num_retiro', 0)
            )
            for _, row in df_metricas.iterrows()
        ]
        
        query = """
            INSERT INTO metricas_operativas 
            (fecha, hora, num_transacciones, num_transacciones_exitosas,
             num_transacciones_fallidas, num_usuarios_activos,
             tiempo_promedio_procesamiento, tiempo_mediano_procesamiento,
             tiempo_p95_procesamiento, tiempo_max_procesamiento,
             tasa_error, tasa_validacion_manual, tasa_fraude,
             volumen_total_usd, volumen_promedio_usd, comisiones_totales_usd,
             num_compras, num_ventas, num_swaps, num_retiros)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
        """
        
        print("Insertando métricas...")
        execute_batch(cursor, query, datos, page_size=1000)
        
        conn.commit()
        print(f" {len(datos):,} registros de métricas insertados exitosamente")
        
    except Exception as e:
        conn.rollback()
        print(f" Error al insertar métricas: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    """Función principal de generación y carga de datos"""
    print("="*80)
    print("CRYPTOOPS ANALYZER - GENERACIÓN DE DATOS")
    print("="*80)
    print(f"Fecha/Hora inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    inicio = datetime.now()
    
    try:
        # 1. Generar usuarios
        df_usuarios = generar_usuarios()
        
        # 2. Generar transacciones
        df_transacciones = generar_transacciones(df_usuarios)
        
        # 3. Generar métricas operativas
        df_metricas = generar_metricas_operativas(df_transacciones)
        
        # 4. Guardar CSVs (backup)
        print(f"\n{'='*80}")
        print("GUARDANDO DATOS EN CSV (BACKUP)")
        print(f"{'='*80}")
        
        df_usuarios.to_csv('data/processed/usuarios.csv', index=False)
        print(" usuarios.csv guardado")
        
        df_transacciones.to_csv('data/processed/transacciones.csv', index=False)
        print(" transacciones.csv guardado")
        
        df_metricas.to_csv('data/processed/metricas_operativas.csv', index=False)
        print(" metricas_operativas.csv guardado")
        
        # 5. Cargar en base de datos
        cargar_usuarios_db(df_usuarios)
        cargar_transacciones_db(df_transacciones)
        cargar_metricas_db(df_metricas)
        
        # Resumen final
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        
        print(f"\n{'='*80}")
        print("RESUMEN FINAL")
        print(f"{'='*80}")
        print(f" Usuarios generados: {len(df_usuarios):,}")
        print(f" Transacciones generadas: {len(df_transacciones):,}")
        print(f" Métricas generadas: {len(df_metricas):,}")
        print(f"\nTiempo total de ejecución: {duracion:.2f} segundos ({duracion/60:.2f} minutos)")
        print(f"Fecha/Hora fin: {fin.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\n GENERACIÓN Y CARGA DE DATOS COMPLETADA EXITOSAMENTE!")
        
    except Exception as e:
        print(f"\n ERROR FATAL: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    import sys
    exito = main()
    sys.exit(0 if exito else 1)