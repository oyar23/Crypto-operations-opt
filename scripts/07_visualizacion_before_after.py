"""
Visualización del impacto Before/After de las optimizaciones
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
    f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
)

def crear_visualizacion_before_after():
    """Crea visualización comparativa Before/After"""
    
    # Cargar datos reales
    df = pd.read_sql_query(
        "SELECT tiempo_procesamiento, estado FROM transacciones WHERE timestamp_inicio >= '2024-07-01'",
        engine
    )
    
    tiempo_promedio_actual = df[df['estado'].isin(['exitosa', 'fallida'])]['tiempo_procesamiento'].mean()
    tasa_error_actual = (df['estado'] == 'fallida').sum() / len(df) * 100
    
    # Proyecciones optimizadas
    reduccion_tiempo = 0.227  # 22.7%
    reduccion_error = 0.467   # 46.7%
    
    tiempo_promedio_optimizado = tiempo_promedio_actual * (1 - reduccion_tiempo)
    tasa_error_optimizada = tasa_error_actual * (1 - reduccion_error)
    
    # Crear visualización
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    colors_before = '#e74c3c'
    colors_after = '#2ecc71'
    
    # Gráfico 1: Tiempo Promedio de Procesamiento
    ax1 = axes[0, 0]
    categorias = ['BEFORE\n(Actual)', 'AFTER\n(Optimizado)']
    valores = [tiempo_promedio_actual, tiempo_promedio_optimizado]
    colors = [colors_before, colors_after]
    
    bars = ax1.bar(categorias, valores, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
    ax1.set_ylabel('Tiempo Promedio (segundos)', fontsize=12, fontweight='bold')
    ax1.set_title('Tiempo de Procesamiento', fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Añadir valores y porcentaje de mejora
    for i, (bar, v) in enumerate(zip(bars, valores)):
        ax1.text(bar.get_x() + bar.get_width()/2, v + 2, f'{v:.0f}s',
                ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # Flecha de mejora
    ax1.annotate('', xy=(1, tiempo_promedio_optimizado), xytext=(0, tiempo_promedio_actual),
                arrowprops=dict(arrowstyle='->', lw=3, color='green'))
    ax1.text(0.5, (tiempo_promedio_actual + tiempo_promedio_optimizado)/2,
            f'-{reduccion_tiempo*100:.1f}%',
            ha='center', fontsize=13, fontweight='bold', color='green',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', edgecolor='green', linewidth=2))
    
    # Gráfico 2: Tasa de Error
    ax2 = axes[0, 1]
    valores_error = [tasa_error_actual, tasa_error_optimizada]
    
    bars = ax2.bar(categorias, valores_error, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
    ax2.set_ylabel('Tasa de Error (%)', fontsize=12, fontweight='bold')
    ax2.set_title('Tasa de Error', fontsize=14, fontweight='bold', pad=20)
    ax2.grid(True, alpha=0.3, axis='y')
    
    for i, (bar, v) in enumerate(zip(bars, valores_error)):
        ax2.text(bar.get_x() + bar.get_width()/2, v + 0.2, f'{v:.2f}%',
                ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # Flecha de mejora
    ax2.annotate('', xy=(1, tasa_error_optimizada), xytext=(0, tasa_error_actual),
                arrowprops=dict(arrowstyle='->', lw=3, color='green'))
    ax2.text(0.5, (tasa_error_actual + tasa_error_optimizada)/2,
            f'-{reduccion_error*100:.1f}%',
            ha='center', fontsize=13, fontweight='bold', color='green',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', edgecolor='green', linewidth=2))
    
    # Gráfico 3: Distribución de Tiempos (Before vs After simulado)
    ax3 = axes[1, 0]
    
    # Simular distribución optimizada
    tiempos_before = df[df['estado'].isin(['exitosa', 'fallida'])]['tiempo_procesamiento'].sample(1000)
    tiempos_after = tiempos_before * (1 - reduccion_tiempo)
    
    ax3.hist(tiempos_before, bins=40, alpha=0.6, color=colors_before, label='BEFORE', edgecolor='black')
    ax3.hist(tiempos_after, bins=40, alpha=0.6, color=colors_after, label='AFTER', edgecolor='black')
    
    ax3.axvline(tiempo_promedio_actual, color=colors_before, linestyle='--', linewidth=2, label=f'Media BEFORE: {tiempo_promedio_actual:.0f}s')
    ax3.axvline(tiempo_promedio_optimizado, color=colors_after, linestyle='--', linewidth=2, label=f'Media AFTER: {tiempo_promedio_optimizado:.0f}s')
    
    ax3.set_xlabel('Tiempo de Procesamiento (segundos)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Frecuencia', fontsize=12, fontweight='bold')
    ax3.set_title('Distribución de Tiempos', fontsize=14, fontweight='bold', pad=20)
    ax3.legend(fontsize=10)
    ax3.grid(True, alpha=0.3)
    
    # Gráfico 4: Resumen de Mejoras
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    mejoras_texto = f"""
        RESUMEN DE MEJORAS PROYECTADAS
    
          Tiempo de Procesamiento:
       • BEFORE: {tiempo_promedio_actual:.0f} segundos
       • AFTER:  {tiempo_promedio_optimizado:.0f} segundos
       • MEJORA: -{(tiempo_promedio_actual - tiempo_promedio_optimizado):.0f}s ({reduccion_tiempo*100:.1f}% reducción)
    
     Tasa de Error:
       • BEFORE: {tasa_error_actual:.2f}%
       • AFTER:  {tasa_error_optimizada:.2f}%
       • MEJORA: {reduccion_error*100:.1f}% reducción
    
     IMPACTO EN OPERACIONES:
       • ~{len(df):,} transacciones procesadas
       • ~{(tiempo_promedio_actual - tiempo_promedio_optimizado) * len(df) / 3600:.1f} horas ahorradas
       • Capacidad aumentada en ~{1/(1-reduccion_tiempo) - 1:.0%}
    
     OPTIMIZACIONES CLAVE:
       1. Validación automática (30% impacto)
       2. Sistema de colas (15% impacto)
       3. Batch processing (10% impacto)
    """
    
    ax4.text(0.1, 0.9, mejoras_texto, fontsize=11, verticalalignment='top',
            family='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
    
    plt.suptitle('Impacto de Optimizaciones: BEFORE vs AFTER', 
                fontsize=18, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig('visualizations/10_impacto_before_after.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(" Visualización guardada: visualizations/10_impacto_before_after.png")

if __name__ == "__main__":
    crear_visualizacion_before_after()