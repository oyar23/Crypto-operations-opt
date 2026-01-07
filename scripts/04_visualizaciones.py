"""
CRYPTOOPS ANALYZER - Generador de Visualizaciones
Crea gráficos profesionales para análisis de operaciones
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

# Conexión a base de datos
engine = create_engine(
    f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
    f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
)

# Configuración de estilo
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("Set2")

COLORS = {
    'primary': '#1f77b4',
    'success': '#2ecc71',
    'danger': '#e74c3c',
    'warning': '#f39c12',
    'info': '#3498db'
}

def cargar_datos():
    """Carga datos desde la base de datos"""
    print("Cargando datos desde base de datos...")
    
    df_transacciones = pd.read_sql_query(
        "SELECT * FROM transacciones WHERE timestamp_inicio >= '2024-07-01'",
        engine
    )
    
    df_metricas = pd.read_sql_query(
        "SELECT * FROM metricas_operativas",
        engine
    )
    
    # Preparar datos
    df_transacciones['fecha'] = pd.to_datetime(df_transacciones['timestamp_inicio']).dt.date
    df_transacciones['hora'] = pd.to_datetime(df_transacciones['timestamp_inicio']).dt.hour
    df_transacciones['dia_semana'] = pd.to_datetime(df_transacciones['timestamp_inicio']).dt.dayofweek
    
    df_transacciones['periodo'] = df_transacciones['hora'].apply(
        lambda x: 'Hora Pico' if 18 <= x <= 23 else 'Hora Normal'
    )
    
    print(f" {len(df_transacciones):,} transacciones cargadas")
    print(f" {len(df_metricas):,} métricas cargadas")
    
    return df_transacciones, df_metricas

def viz1_heatmap_transacciones(df):
    """Heatmap de volumen de transacciones por hora y día"""
    print("\nGenerando: Heatmap de transacciones...")
    
    pivot_table = df.pivot_table(
        values='transaction_id',
        index='dia_semana',
        columns='hora',
        aggfunc='count',
        fill_value=0
    )
    
    dias_labels = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    
    plt.figure(figsize=(16, 8))
    sns.heatmap(
        pivot_table,
        annot=True,
        fmt='g',
        cmap='YlOrRd',
        cbar_kws={'label': 'Número de Transacciones'},
        yticklabels=dias_labels
    )
    plt.title('Volumen de Transacciones por Hora y Día de la Semana', 
              fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Hora del Día', fontsize=12)
    plt.ylabel('Día de la Semana', fontsize=12)
    plt.tight_layout()
    plt.savefig('visualizations/01_heatmap_transacciones.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Guardado: visualizations/01_heatmap_transacciones.png")

def viz2_tiempo_procesamiento_hora(df):
    """Gráfico de tiempo de procesamiento por hora"""
    print("\nGenerando: Tiempo de procesamiento por hora...")
    
    stats_por_hora = df[df['estado'].isin(['exitosa', 'fallida'])].groupby('hora').agg({
        'tiempo_procesamiento': ['mean', 'median', lambda x: x.quantile(0.95)],
        'transaction_id': 'count'
    }).reset_index()
    
    stats_por_hora.columns = ['hora', 'tiempo_promedio', 'tiempo_mediano', 'tiempo_p95', 'num_transacciones']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    
    # Gráfico 1: Tiempo de procesamiento
    ax1.plot(stats_por_hora['hora'], stats_por_hora['tiempo_promedio'], 
             marker='o', linewidth=2, markersize=8, label='Promedio', color=COLORS['primary'])
    ax1.plot(stats_por_hora['hora'], stats_por_hora['tiempo_mediano'], 
             marker='s', linewidth=2, markersize=8, label='Mediana', color=COLORS['success'])
    ax1.plot(stats_por_hora['hora'], stats_por_hora['tiempo_p95'], 
             marker='^', linewidth=2, markersize=8, label='P95', color=COLORS['danger'])
    
    # Resaltar hora pico
    ax1.axvspan(18, 23, alpha=0.2, color='red', label='Hora Pico')
    
    ax1.set_title('Tiempo de Procesamiento por Hora del Día', 
                  fontsize=14, fontweight='bold')
    ax1.set_xlabel('Hora del Día', fontsize=12)
    ax1.set_ylabel('Tiempo (segundos)', fontsize=12)
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(0, 24))
    
    # Gráfico 2: Volumen de transacciones
    ax2.bar(stats_por_hora['hora'], stats_por_hora['num_transacciones'], 
            color=COLORS['info'], alpha=0.7, edgecolor='black')
    ax2.axvspan(18, 23, alpha=0.2, color='red')
    ax2.set_title('Volumen de Transacciones por Hora', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Hora del Día', fontsize=12)
    ax2.set_ylabel('Número de Transacciones', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(range(0, 24))
    
    plt.tight_layout()
    plt.savefig('visualizations/02_tiempo_procesamiento_hora.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Guardado: visualizations/02_tiempo_procesamiento_hora.png")

def viz3_comparacion_hora_pico_normal(df):
    """Comparación entre hora pico y hora normal"""
    print("\nGenerando: Comparación hora pico vs. normal...")
    
    comparacion = df[df['estado'].isin(['exitosa', 'fallida'])].groupby('periodo').agg({
        'tiempo_procesamiento': ['mean', 'median', lambda x: x.quantile(0.95)],
        'transaction_id': 'count',
        'estado': lambda x: (x == 'fallida').sum() * 100 / len(x)
    }).reset_index()
    
    comparacion.columns = ['periodo', 'tiempo_promedio', 'tiempo_mediano', 'tiempo_p95', 
                          'num_transacciones', 'tasa_error']
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    # Gráfico 1: Tiempo de procesamiento
    x = range(len(comparacion))
    width = 0.25
    
    axes[0].bar([i - width for i in x], comparacion['tiempo_promedio'], width, 
                label='Promedio', color=COLORS['primary'])
    axes[0].bar([i for i in x], comparacion['tiempo_mediano'], width, 
                label='Mediana', color=COLORS['success'])
    axes[0].bar([i + width for i in x], comparacion['tiempo_p95'], width, 
                label='P95', color=COLORS['danger'])
    
    axes[0].set_title('Tiempo de Procesamiento', fontsize=12, fontweight='bold')
    axes[0].set_ylabel('Tiempo (segundos)', fontsize=11)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(comparacion['periodo'])
    axes[0].legend()
    axes[0].grid(True, alpha=0.3, axis='y')
    
    # Gráfico 2: Tasa de error
    colors = [COLORS['success'] if p == 'Hora Normal' else COLORS['danger'] 
              for p in comparacion['periodo']]
    axes[1].bar(comparacion['periodo'], comparacion['tasa_error'], color=colors, alpha=0.7)
    axes[1].set_title('Tasa de Error', fontsize=12, fontweight='bold')
    axes[1].set_ylabel('Tasa de Error (%)', fontsize=11)
    axes[1].grid(True, alpha=0.3, axis='y')
    
    # Añadir valores encima de las barras
    for i, v in enumerate(comparacion['tasa_error']):
        axes[1].text(i, v + 0.3, f'{v:.2f}%', ha='center', fontweight='bold')
    
    # Gráfico 3: Volumen de transacciones
    axes[2].bar(comparacion['periodo'], comparacion['num_transacciones'], 
                color=COLORS['info'], alpha=0.7)
    axes[2].set_title('Volumen de Transacciones', fontsize=12, fontweight='bold')
    axes[2].set_ylabel('Número de Transacciones', fontsize=11)
    axes[2].grid(True, alpha=0.3, axis='y')
    
    # Añadir valores
    for i, v in enumerate(comparacion['num_transacciones']):
        axes[2].text(i, v + 100, f'{v:,}', ha='center', fontweight='bold')
    
    plt.suptitle('Comparación: Hora Pico vs. Hora Normal', 
                 fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig('visualizations/03_comparacion_hora_pico.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Guardado: visualizations/03_comparacion_hora_pico.png")

def viz4_distribucion_tiempos(df):
    """Distribución de tiempos de procesamiento"""
    print("\nGenerando: Distribución de tiempos...")
    
    df_exitosas = df[df['estado'] == 'exitosa']
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Histograma general
    axes[0, 0].hist(df_exitosas['tiempo_procesamiento'], bins=50, 
                    color=COLORS['primary'], alpha=0.7, edgecolor='black')
    axes[0, 0].axvline(df_exitosas['tiempo_procesamiento'].mean(), 
                       color='red', linestyle='--', linewidth=2, label='Media')
    axes[0, 0].axvline(df_exitosas['tiempo_procesamiento'].median(), 
                       color='green', linestyle='--', linewidth=2, label='Mediana')
    axes[0, 0].set_title('Distribución General de Tiempos', fontsize=12, fontweight='bold')
    axes[0, 0].set_xlabel('Tiempo (segundos)')
    axes[0, 0].set_ylabel('Frecuencia')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # Box plot por periodo
    data_boxplot = [
        df_exitosas[df_exitosas['periodo'] == 'Hora Normal']['tiempo_procesamiento'],
        df_exitosas[df_exitosas['periodo'] == 'Hora Pico']['tiempo_procesamiento']
    ]
    bp = axes[0, 1].boxplot(data_boxplot, labels=['Hora Normal', 'Hora Pico'],
                             patch_artist=True)
    for patch, color in zip(bp['boxes'], [COLORS['success'], COLORS['danger']]):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    axes[0, 1].set_title('Comparación de Distribución por Periodo', fontsize=12, fontweight='bold')
    axes[0, 1].set_ylabel('Tiempo (segundos)')
    axes[0, 1].grid(True, alpha=0.3)
    
    # Histograma por cripto principal
    for cripto, color in [('BTC', COLORS['warning']), ('ETH', COLORS['info'])]:
        data = df_exitosas[df_exitosas['cripto'] == cripto]['tiempo_procesamiento']
        axes[1, 0].hist(data, bins=30, alpha=0.6, label=cripto, color=color, edgecolor='black')
    axes[1, 0].set_title('Distribución por Criptomoneda (BTC vs ETH)', fontsize=12, fontweight='bold')
    axes[1, 0].set_xlabel('Tiempo (segundos)')
    axes[1, 0].set_ylabel('Frecuencia')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    # CDF (Cumulative Distribution Function)
    tiempos_ordenados = np.sort(df_exitosas['tiempo_procesamiento'])
    cdf = np.arange(1, len(tiempos_ordenados) + 1) / len(tiempos_ordenados)
    axes[1, 1].plot(tiempos_ordenados, cdf * 100, linewidth=2, color=COLORS['primary'])
    axes[1, 1].axhline(95, color='red', linestyle='--', label='P95')
    axes[1, 1].axvline(np.percentile(df_exitosas['tiempo_procesamiento'], 95), 
                       color='red', linestyle='--')
    axes[1, 1].set_title('Función de Distribución Acumulada (CDF)', fontsize=12, fontweight='bold')
    axes[1, 1].set_xlabel('Tiempo (segundos)')
    axes[1, 1].set_ylabel('Percentil (%)')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('visualizations/04_distribucion_tiempos.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✅ Guardado: visualizations/04_distribucion_tiempos.png")

def viz5_performance_por_cripto(df):
    """Performance por criptomoneda"""
    print("\nGenerando: Performance por criptomoneda...")
    
    stats_cripto = df[df['estado'] == 'exitosa'].groupby('cripto').agg({
        'transaction_id': 'count',
        'monto_usd': 'sum',
        'tiempo_procesamiento': 'mean'
    }).reset_index()
    
    stats_cripto.columns = ['cripto', 'num_transacciones', 'volumen_usd', 'tiempo_promedio']
    stats_cripto = stats_cripto.sort_values('volumen_usd', ascending=False)
    
    # Calcular tasa de error por cripto
    tasa_error = df.groupby('cripto').apply(
        lambda x: (x['estado'] == 'fallida').sum() * 100 / len(x)
    ).reset_index()
    tasa_error.columns = ['cripto', 'tasa_error']
    
    stats_cripto = stats_cripto.merge(tasa_error, on='cripto')
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    
    # Gráfico 1: Volumen por cripto
    colors_gradient = plt.cm.viridis(np.linspace(0, 1, len(stats_cripto)))
    axes[0, 0].barh(stats_cripto['cripto'], stats_cripto['volumen_usd'], color=colors_gradient)
    axes[0, 0].set_title('Volumen Total por Criptomoneda', fontsize=12, fontweight='bold')
    axes[0, 0].set_xlabel('Volumen USD')
    axes[0, 0].grid(True, alpha=0.3, axis='x')
    
    # Añadir valores
    for i, v in enumerate(stats_cripto['volumen_usd']):
        axes[0, 0].text(v + 100000, i, f'${v/1e6:.1f}M', va='center')
    
    # Gráfico 2: Número de transacciones
    axes[0, 1].bar(stats_cripto['cripto'], stats_cripto['num_transacciones'], 
                   color=colors_gradient, alpha=0.7)
    axes[0, 1].set_title('Número de Transacciones', fontsize=12, fontweight='bold')
    axes[0, 1].set_ylabel('Transacciones')
    axes[0, 1].grid(True, alpha=0.3, axis='y')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # Gráfico 3: Tiempo promedio de procesamiento
    axes[1, 0].bar(stats_cripto['cripto'], stats_cripto['tiempo_promedio'], 
                   color=colors_gradient, alpha=0.7)
    axes[1, 0].set_title('Tiempo Promedio de Procesamiento', fontsize=12, fontweight='bold')
    axes[1, 0].set_ylabel('Tiempo (segundos)')
    axes[1, 0].grid(True, alpha=0.3, axis='y')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # Gráfico 4: Tasa de error
    colors_error = [COLORS['success'] if x < 5 else COLORS['warning'] if x < 10 else COLORS['danger'] 
                    for x in stats_cripto['tasa_error']]
    axes[1, 1].bar(stats_cripto['cripto'], stats_cripto['tasa_error'], 
                   color=colors_error, alpha=0.7)
    axes[1, 1].axhline(10, color='red', linestyle='--', label='Umbral Crítico (10%)')
    axes[1, 1].set_title('Tasa de Error por Criptomoneda', fontsize=12, fontweight='bold')
    axes[1, 1].set_ylabel('Tasa de Error (%)')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3, axis='y')
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('visualizations/05_performance_por_cripto.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Guardado: visualizations/05_performance_por_cripto.png")

def viz6_evolucion_temporal(df):
    """Evolución temporal de métricas"""
    print("\nGenerando: Evolución temporal...")
    
    # Agrupar por fecha
    evolucion = df.groupby('fecha').agg({
        'transaction_id': 'count',
        'user_id': 'nunique',
        'monto_usd': lambda x: x[df.loc[x.index, 'estado'] == 'exitosa'].sum(),
        'tiempo_procesamiento': 'mean'
    }).reset_index()
    
    evolucion.columns = ['fecha', 'num_transacciones', 'usuarios_activos', 'volumen_usd', 'tiempo_promedio']
    
    # Calcular tasa de error
    tasa_error_diaria = df.groupby('fecha').apply(
        lambda x: (x['estado'] == 'fallida').sum() * 100 / len(x)
    ).reset_index()
    tasa_error_diaria.columns = ['fecha', 'tasa_error']
    
    evolucion = evolucion.merge(tasa_error_diaria, on='fecha')
    evolucion['fecha'] = pd.to_datetime(evolucion['fecha'])
    
    fig, axes = plt.subplots(3, 1, figsize=(18, 14))
    
    # Gráfico 1: Volumen y número de transacciones
    ax1 = axes[0]
    ax1_twin = ax1.twinx()
    
    ax1.plot(evolucion['fecha'], evolucion['volumen_usd'], 
             color=COLORS['primary'], linewidth=2, marker='o', markersize=4, label='Volumen USD')
    ax1_twin.plot(evolucion['fecha'], evolucion['num_transacciones'], 
                  color=COLORS['success'], linewidth=2, marker='s', markersize=4, label='# Transacciones')
    
    ax1.set_title('Evolución del Volumen y Número de Transacciones', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Fecha')
    ax1.set_ylabel('Volumen (USD)', color=COLORS['primary'])
    ax1_twin.set_ylabel('Número de Transacciones', color=COLORS['success'])
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='y', labelcolor=COLORS['primary'])
    ax1_twin.tick_params(axis='y', labelcolor=COLORS['success'])
    
    # Gráfico 2: Tiempo de procesamiento
    axes[1].plot(evolucion['fecha'], evolucion['tiempo_promedio'], 
                 color=COLORS['warning'], linewidth=2, marker='o', markersize=4)
    axes[1].axhline(evolucion['tiempo_promedio'].mean(), color='red', linestyle='--', 
                    label=f'Media: {evolucion["tiempo_promedio"].mean():.2f}s')
    axes[1].fill_between(evolucion['fecha'], 
                         evolucion['tiempo_promedio'].mean() - evolucion['tiempo_promedio'].std(),
                         evolucion['tiempo_promedio'].mean() + evolucion['tiempo_promedio'].std(),
                         alpha=0.2, color='red', label='±1 Desv. Est.')
    axes[1].set_title('Evolución del Tiempo de Procesamiento', fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Fecha')
    axes[1].set_ylabel('Tiempo Promedio (segundos)')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    # Gráfico 3: Tasa de error
    axes[2].plot(evolucion['fecha'], evolucion['tasa_error'], 
                 color=COLORS['danger'], linewidth=2, marker='o', markersize=4)
    axes[2].axhline(10, color='red', linestyle='--', label='Umbral Crítico (10%)')
    axes[2].fill_between(evolucion['fecha'], 0, evolucion['tasa_error'], 
                         where=(evolucion['tasa_error'] > 10), alpha=0.3, color='red')
    axes[2].set_title('Evolución de la Tasa de Error', fontsize=12, fontweight='bold')
    axes[2].set_xlabel('Fecha')
    axes[2].set_ylabel('Tasa de Error (%)')
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('visualizations/06_evolucion_temporal.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Guardado: visualizations/06_evolucion_temporal.png")

def viz7_motivos_fallo(df):
    """Análisis de motivos de fallo"""
    print("\nGenerando: Análisis de fallos...")
    
    df_fallidas = df[df['estado'] == 'fallida']
    
    motivos = df_fallidas['motivo_fallo'].value_counts().head(8)
    
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    
    # Gráfico de barras
    axes[0].barh(range(len(motivos)), motivos.values, color=COLORS['danger'], alpha=0.7)
    axes[0].set_yticks(range(len(motivos)))
    axes[0].set_yticklabels(motivos.index)
    axes[0].set_title('Top Motivos de Fallo', fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Número de Transacciones Fallidas')
    axes[0].grid(True, alpha=0.3, axis='x')
    
    # Añadir valores
    for i, v in enumerate(motivos.values):
        axes[0].text(v + 50, i, f'{v:,}', va='center')
    
    # Gráfico de pie
    colors_pie = plt.cm.Reds(np.linspace(0.4, 0.8, len(motivos)))
    axes[1].pie(motivos.values, labels=motivos.index, autopct='%1.1f%%', 
                colors=colors_pie, startangle=90)
    axes[1].set_title('Distribución Porcentual de Motivos de Fallo', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('visualizations/07_motivos_fallo.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Guardado: visualizations/07_motivos_fallo.png")

def viz8_dashboard_interactivo(df, df_metricas):
    """Dashboard interactivo con Plotly"""
    print("\nGenerando: Dashboard interactivo...")
    
    # Preparar datos
    stats_hora = df[df['estado'].isin(['exitosa', 'fallida'])].groupby('hora').agg({
        'tiempo_procesamiento': 'mean',
        'transaction_id': 'count'
    }).reset_index()
    
    stats_hora.columns = ['hora', 'tiempo_promedio', 'num_transacciones']
    
    tasa_error_hora = df.groupby('hora').apply(
        lambda x: (x['estado'] == 'fallida').sum() * 100 / len(x)
    ).reset_index()
    tasa_error_hora.columns = ['hora', 'tasa_error']
    
    stats_hora = stats_hora.merge(tasa_error_hora, on='hora')
    
    # Crear subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Tiempo de Procesamiento por Hora', 'Volumen de Transacciones',
                       'Tasa de Error por Hora', 'Distribución de Tiempos'),
        specs=[[{'type': 'scatter'}, {'type': 'bar'}],
               [{'type': 'scatter'}, {'type': 'histogram'}]]
    )
    
    # Gráfico 1: Tiempo de procesamiento
    fig.add_trace(
        go.Scatter(
            x=stats_hora['hora'],
            y=stats_hora['tiempo_promedio'],
            mode='lines+markers',
            name='Tiempo Promedio',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8)
        ),
        row=1, col=1
    )
    
    # Resaltar hora pico
    fig.add_vrect(
        x0=18, x1=23,
        fillcolor="red", opacity=0.2,
        layer="below", line_width=0,
        annotation_text="Hora Pico", annotation_position="top left",
        row=1, col=1
    )
    
    # Gráfico 2: Volumen
    fig.add_trace(
        go.Bar(
            x=stats_hora['hora'],
            y=stats_hora['num_transacciones'],
            name='Transacciones',
            marker_color='#2ecc71'
        ),
        row=1, col=2
    )
    
    # Gráfico 3: Tasa de error
    fig.add_trace(
        go.Scatter(
            x=stats_hora['hora'],
            y=stats_hora['tasa_error'],
            mode='lines+markers',
            name='Tasa Error',
            line=dict(color='#e74c3c', width=3),
            marker=dict(size=8),
            fill='tozeroy'
        ),
        row=2, col=1
    )
    
    # Línea umbral
    fig.add_hline(
        y=10, line_dash="dash", line_color="red",
        annotation_text="Umbral Crítico",
        row=2, col=1
    )
    
    # Gráfico 4: Histograma
    df_exitosas = df[df['estado'] == 'exitosa']
    fig.add_trace(
        go.Histogram(
            x=df_exitosas['tiempo_procesamiento'],
            nbinsx=50,
            name='Distribución',
            marker_color='#3498db'
        ),
        row=2, col=2
    )
    
    # Actualizar layout
    fig.update_xaxes(title_text="Hora del Día", row=1, col=1)
    fig.update_yaxes(title_text="Tiempo (seg)", row=1, col=1)
    
    fig.update_xaxes(title_text="Hora del Día", row=1, col=2)
    fig.update_yaxes(title_text="# Transacciones", row=1, col=2)
    
    fig.update_xaxes(title_text="Hora del Día", row=2, col=1)
    fig.update_yaxes(title_text="Tasa Error (%)", row=2, col=1)
    
    fig.update_xaxes(title_text="Tiempo (seg)", row=2, col=2)
    fig.update_yaxes(title_text="Frecuencia", row=2, col=2)
    
    fig.update_layout(
        title_text="Dashboard de Operaciones Crypto - Análisis de Performance",
        title_font_size=20,
        showlegend=False,
        height=800,
        template='plotly_white'
    )
    
    fig.write_html('visualizations/08_dashboard_interactivo.html')
    
    print(" Guardado: visualizations/08_dashboard_interactivo.html")

def main():
    """Función principal"""
    print("="*80)
    print("CRYPTOOPS ANALYZER - GENERACIÓN DE VISUALIZACIONES")
    print("="*80)
    print(f"Inicio: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Cargar datos
    df_transacciones, df_metricas = cargar_datos()
    
    # Crear directorio de visualizaciones
    os.makedirs('visualizations', exist_ok=True)
    
    # Generar visualizaciones
    viz1_heatmap_transacciones(df_transacciones)
    viz2_tiempo_procesamiento_hora(df_transacciones)
    viz3_comparacion_hora_pico_normal(df_transacciones)
    viz4_distribucion_tiempos(df_transacciones)
    viz5_performance_por_cripto(df_transacciones)
    viz6_evolucion_temporal(df_transacciones)
    viz7_motivos_fallo(df_transacciones)
    viz8_dashboard_interactivo(df_transacciones, df_metricas)
    
    print(f"\n{'='*80}")
    print(" VISUALIZACIONES COMPLETADAS EXITOSAMENTE")
    print(f"{'='*80}")
    print(f"Total de visualizaciones generadas: 8")
    print(f"Ubicación: visualizations/")

if __name__ == "__main__":
    main()