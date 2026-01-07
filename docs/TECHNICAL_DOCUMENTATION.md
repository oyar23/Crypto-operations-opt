# 📚 Documentación Técnica - CryptoOps Analyzer

## Arquitectura del Sistema

### Componentes Principales

#### 1. Base de Datos (PostgreSQL)

**Tablas Principales:**
- `usuarios`: Información de clientes
- `transacciones`: Registro detallado de operaciones
- `metricas_operativas`: Agregaciones por hora
- `validaciones`: Histórico de validaciones
- `logs_sistema`: Eventos y errores

**Triggers:**
- `trigger_actualizar_tiempo_procesamiento`: Calcula automáticamente tiempo de procesamiento
- `trigger_actualizar_estadisticas_usuario`: Mantiene estadísticas de usuario actualizadas

**Vistas:**
- `vista_resumen_usuarios`: Resumen de actividad por usuario
- `vista_metricas_tiempo_real`: Métricas de última hora
- `vista_top_criptos`: Top criptomonedas por volumen

#### 2. Capa de Análisis (Python)

**Scripts Principales:**

scripts/
├── 02_generar_datos.py              # Generación de datos
├── 03_ejecutar_analisis_sql.py      # Análisis SQL
├── 04_visualizaciones.py            # Visualizaciones estáticas
├── 05_analisis_cuellos_botella.py   # Identificación de bottlenecks
├── 06_optimizacion_batch_processing.py # Simulación de optimizaciones
├── 07_visualizacion_before_after.py # Comparativa visual
└── 08_dashboard_ejecutivo.py        # Dashboard interactivo

### Flujo de Datos

[PostgreSQL] --> [SQLAlchemy] --> [Pandas] --> [Análisis] --> [Visualizaciones]
↑                                                              ↓
|                                                         [Dashboard]
└─────────────────── [Faker] ──────────────────────────────────┘
(Generación de datos)

## Decisiones de Diseño

### Base de Datos

**¿Por qué PostgreSQL?**
- Soporte robusto para funciones y triggers
- Excelente performance en consultas analíticas
- ACID compliance para integridad de datos
- Funciones de ventana para análisis temporal

**Índices Creados:**
```sql
-- Optimización de consultas por timestamp
CREATE INDEX idx_transacciones_timestamp_inicio 
ON transacciones(timestamp_inicio);

-- Búsqueda por usuario
CREATE INDEX idx_transacciones_user_id 
ON transacciones(user_id);

-- Análisis por estado
CREATE INDEX idx_transacciones_estado 
ON transacciones(estado);

-- Análisis por hora (compuesto)
CREATE INDEX idx_transacciones_hora_estado 
ON transacciones(EXTRACT(HOUR FROM timestamp_inicio), estado);
```

### Generación de Datos

**Distribuciones Aplicadas:**
- Tiempo de procesamiento: Normal con μ ajustable según hora
- Score de fraude: Uniforme con bump para transacciones grandes
- Fechas de registro: Exponencial (más usuarios recientes)
- Monto de transacciones: Log-normal (realista para finanzas)

**Patrones Simulados:**
- Hora pico: 18-23h con 3x más carga
- Usuarios verificados: Mayor volumen y menor tasa de error
- Métodos de pago: Diferentes velocidades de procesamiento
- Geolocalización: Argentina 60%, México 20%, Colombia 10%, Chile 10%

### Análisis

**Métricas Clave:**
- Tiempo promedio, mediana, P95, P99
- Tasa de error por segmento
- Volumen y throughput
- Capacidad y saturación

**Segmentaciones:**
- Por hora del día
- Por día de la semana
- Por tipo de operación
- Por criptomoneda
- Por método de pago
- Por nivel de verificación

## Performance y Optimización

### Queries SQL Optimizadas

**Ejemplo: Análisis por Hora**
```sql
-- MALO (sin índice, scan completo)
SELECT EXTRACT(HOUR FROM timestamp_inicio) as hora,
       AVG(tiempo_procesamiento) as avg_tiempo
FROM transacciones
GROUP BY hora;

-- BUENO (con índice compuesto)
CREATE INDEX idx_hora ON transacciones((EXTRACT(HOUR FROM timestamp_inicio)));

-- MEJOR (tabla precalculada)
SELECT hora, tiempo_promedio_procesamiento
FROM metricas_operativas;
```

### Manejo de Grandes Volúmenes

**Técnicas Aplicadas:**
- Batch processing para inserciones (5,000 registros por lote)
- Uso de `execute_batch` en psycopg2
- Precálculo de métricas agregadas
- Sampling para visualizaciones (cuando apropiado)

## Testing y Validación

### Validaciones Implementadas

1. **Integridad de Datos**
```python
   assert df['monto_usd'].min() >= 0, "Montos negativos detectados"
   assert df['tiempo_procesamiento'].min() >= 0, "Tiempos negativos"
```

2. **Consistencia Temporal**
```python
   assert (df['timestamp_completado'] >= df['timestamp_inicio']).all()
```

3. **Distribuciones Esperadas**
```python
   assert 0.4 <= (df['pais'] == 'Argentina').mean() <= 0.7
```

### Casos de Prueba
```bash
# Verificar schema
python scripts/verificar_schema.py

# Validar generación de datos
python -c "
from scripts.02_generar_datos import generar_usuarios
df = generar_usuarios(100)
assert len(df) == 100
print(' Test passed')
"
```

## Troubleshooting

### Problemas Comunes

**1. Error de conexión a PostgreSQL**
```bash
# Verificar que PostgreSQL está corriendo
sudo systemctl status postgresql

# Verificar credenciales en .env
cat .env | grep DB_
```

**2. Out of Memory al generar datos**
```bash
# Reducir tamaño de generación
# En scripts/02_generar_datos.py:
NUM_TRANSACCIONES = 10000  # En lugar de 100000
```

**3. Queries lentas**
```sql
-- Verificar índices
SELECT tablename, indexname 
FROM pg_indexes 
WHERE schemaname = 'public';

-- Analizar query
EXPLAIN ANALYZE SELECT ...;
```

## Mantenimiento

### Backup de Base de Datos
```bash
# Backup completo
pg_dump -U cryptoops_user cryptoops_db > backup_$(date +%Y%m%d).sql

# Restaurar
psql -U cryptoops_user cryptoops_db < backup_20250107.sql
```

### Limpieza de Datos
```sql
-- Eliminar datos antiguos (si aplica)
DELETE FROM transacciones 
WHERE timestamp_inicio < NOW() - INTERVAL '1 year';

-- Vacuum para optimizar
VACUUM ANALYZE transacciones;
```

## Seguridad

### Buenas Prácticas Implementadas

1. **Credenciales en .env**
   - Nunca en código fuente
   - Incluido en .gitignore

2. **Permisos de Base de Datos**
```sql
   -- Usuario específico con permisos limitados
   REVOKE ALL ON SCHEMA public FROM PUBLIC;
   GRANT USAGE ON SCHEMA public TO cryptoops_user;
```

3. **SQL Injection Prevention**
```python
   # MALO
   query = f"SELECT * FROM users WHERE id = {user_id}"
   
   # BUENO
   query = "SELECT * FROM users WHERE id = %s"
   cursor.execute(query, (user_id,))
```

## Recursos Adicionales

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Plotly Documentation](https://plotly.com/python/)

---

Para preguntas técnicas adicionales, consultar código fuente o crear un issue en GitHub.