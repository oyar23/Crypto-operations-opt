# 🚀 CryptoOps Analyzer

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success.svg)

**Sistema de Optimización de Procesos Operativos para Plataformas Crypto**

[Características](#-características-principales) •
[Instalación](#-instalación) •
[Uso](#-uso) •
[Resultados](#-resultados) •
[Documentación](#-documentación)

</div>

---

## 📋 Descripción

**CryptoOps Analyzer** es un proyecto de análisis de datos y optimización de operaciones diseñado para identificar y resolver cuellos de botella en plataformas de intercambio de criptomonedas. Utilizando datos simulados realistas, el proyecto demuestra cómo el análisis sistemático y las automatizaciones inteligentes pueden mejorar significativamente la eficiencia operativa.

### 🎯 Objetivo

Reducir el tiempo de procesamiento de transacciones en un **22-30%** mediante identificación de ineficiencias, automatización de validaciones y optimización de flujos de trabajo.

### 📊 Resultados Clave

| Métrica | Before | After | Mejora |
|---------|--------|-------|--------|
| ⏱️ Tiempo Promedio de Procesamiento | 75s | 58s | **-22.7%** |
| ❌ Tasa de Error (Hora Pico) | 15% | 8% | **-46.7%** |
| 🚀 Transacciones por Hora | 480 | 620 | **+29%** |
| 💰 Tiempo Ahorrado | - | **87.3 horas** | - |

---

## ✨ Características Principales

### 🔍 Análisis Exploratorio Completo
- **100,000+ transacciones** analizadas
- **10,000 usuarios** simulados con patrones realistas
- Identificación de **4 cuellos de botella principales**
- Análisis temporal, geográfico y por segmento

### 🤖 Automatizaciones Inteligentes
- Sistema de **validación automática** para transacciones de bajo riesgo
- **Queue system** con priorización inteligente
- **Batch processing** optimizado
- Reducción del 79% en tiempo de validaciones

### 📈 Visualizaciones Impactantes
- **12+ gráficos** profesionales de alta calidad
- Dashboard interactivo con Plotly
- Heatmaps, distribuciones y análisis temporal
- Comparativas Before/After

### 💡 Propuestas Accionables
- 5 optimizaciones priorizadas por impacto
- Roadmap de implementación detallado
- Análisis costo-beneficio
- Métricas de éxito definidas

---

## 🛠️ Stack Tecnológico

### Backend & Database
- **PostgreSQL 15**: Base de datos relacional con funciones, triggers y vistas
- **SQLAlchemy**: ORM para interacción con base de datos
- **psycopg2**: Driver nativo de PostgreSQL

### Análisis de Datos
- **Pandas**: Manipulación y análisis de datos
- **NumPy**: Operaciones numéricas y estadísticas
- **Faker**: Generación de datos realistas

### Visualización
- **Matplotlib**: Gráficos estáticos de alta calidad
- **Seaborn**: Visualizaciones estadísticas avanzadas
- **Plotly**: Dashboards interactivos

### Desarrollo
- **Jupyter**: Notebooks para análisis exploratorio
- **Git**: Control de versiones
- **Python-dotenv**: Gestión de variables de entorno

---

## 🚀 Instalación

### Prerrequisitos
```bash
# Python 3.10 o superior
python --version

# PostgreSQL 15
psql --version

# Git
git --version
```

### Paso 1: Clonar Repositorio
```bash
git clone https://github.com/oyar23/Crypto-operations-opt.git
cd Crypto-operations-opt
```

### Paso 2: Crear Entorno Virtual
```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
```

### Paso 3: Instalar Dependencias
```bash
pip install -r requirements.txt
```

### Paso 4: Configurar Base de Datos
```bash
# Conectar a PostgreSQL
psql -U postgres

# Crear base de datos
CREATE DATABASE cryptoops_db;
CREATE USER cryptoops_user WITH PASSWORD 'crypto2024secure';
GRANT ALL PRIVILEGES ON DATABASE cryptoops_db TO cryptoops_user;
\q

# Ejecutar schema
psql -U cryptoops_user -d cryptoops_db -f sql/01_schema_creation.sql
```

### Paso 5: Configurar Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cryptoops_db
DB_USER=cryptoops_user
DB_PASSWORD=----
```

### Paso 6: Generar Datos
```bash
# Generar y cargar datos (toma ~10-15 minutos)
python scripts/02_generar_datos.py
```

---

## 💻 Uso

### Análisis Exploratorio
```bash
# Ejecutar análisis SQL
python scripts/03_ejecutar_analisis_sql.py

# Generar visualizaciones
python scripts/04_visualizaciones.py
```

### Identificación de Cuellos de Botella
```bash
# Análisis de cuellos de botella
python scripts/05_analisis_cuellos_botella.py
```

### Simulación de Optimizaciones
```bash
# Simular impacto de optimizaciones
python scripts/06_optimizacion_batch_processing.py

# Generar comparativa Before/After
python scripts/07_visualizacion_before_after.py
```

### Dashboard Ejecutivo
```bash
# Generar dashboard interactivo
python scripts/08_dashboard_ejecutivo.py

# Abrir en navegador
open visualizations/dashboard_completo.html
```

### Notebooks Interactivos
```bash
# Iniciar Jupyter
jupyter notebook

# Abrir notebooks en orden:
# 1. notebooks/01_exploratory_analysis.ipynb
# 2. notebooks/02_bottleneck_analysis.ipynb
```

---

## 📊 Resultados

### Cuellos de Botella Identificados

| Cuello de Botella | Transacciones Afectadas | Tiempo Desperdiciado | Impacto |
|-------------------|-------------------------|---------------------|---------|
| 🕐 **Hora Pico (18-23h)** | 45,231 | 42.1 horas | 25% |
| ✅ **Validaciones Manuales** | 15,789 | 35.2 horas | 30% |
| 💳 **Método de Pago Lento** | 28,456 | 18.7 horas | 15% |
| 🐌 **Transacciones >5min** | 3,214 | 12.8 horas | 20% |
| **TOTAL** | - | **87.3 horas** | - |

### Optimizaciones Propuestas

1. **Validación Automática** (Impacto: 30%)
   - Automatiza validaciones para transacciones <$1,000
   - Reduce tiempo de validación de 95s a 10s
   - Tasa de automatización: 68%

2. **Sistema de Colas por Prioridad** (Impacto: 15%)
   - Prioriza transacciones de alto valor
   - Distribuye carga en hora pico
   - Mejora experiencia de usuarios premium

3. **Batch Processing** (Impacto: 10%)
   - Procesa transacciones en lotes
   - Reduce overhead de sistema
   - Mayor throughput

4. **Optimización de Métodos de Pago** (Impacto: 10%)
   - Identifica métodos problemáticos
   - Propone integraciones más rápidas

5. **Escalado Horizontal** (Impacto: 12%)
   - Añade capacidad en horas críticas
   - Balanceo de carga inteligente

---

## 📁 Estructura del Proyecto

Crypto-operations-opt/
│
├── data/
│   ├── processed/          # Datos procesados y resultados
│   │   ├── usuarios.csv
│   │   ├── transacciones.csv
│   │   ├── metricas_operativas.csv
│   │   ├── analisis_*.csv
│   │   └── reporte_cuellos_botella.csv
│   └── raw/               # Datos crudos (si aplica)
│
├── notebooks/
│   ├── 01_exploratory_analysis.ipynb      # EDA completo
│   └── 02_bottleneck_analysis.ipynb       # Análisis de cuellos de botella
│
├── scripts/
│   ├── 02_generar_datos.py                # Generación de datos
│   ├── 03_ejecutar_analisis_sql.py        # Análisis SQL
│   ├── 04_visualizaciones.py              # Generación de gráficos
│   ├── 05_analisis_cuellos_botella.py     # Análisis de bottlenecks
│   ├── 06_optimizacion_batch_processing.py # Simulación de optimizaciones
│   ├── 07_visualizacion_before_after.py   # Comparativa Before/After
│   └── 08_dashboard_ejecutivo.py          # Dashboard interactivo
│
├── sql/
│   ├── 01_schema_creation.sql             # Creación de tablas
│   └── 02_queries_analisis_basico.sql     # Queries de análisis
│
├── visualizations/
│   ├── 01_heatmap_transacciones.png
│   ├── 02_tiempo_procesamiento_hora.png
│   ├── 03_comparacion_hora_pico.png
│   ├── 04_distribucion_tiempos.png
│   ├── 05_performance_por_cripto.png
│   ├── 06_evolucion_temporal.png
│   ├── 07_motivos_fallo.png
│   ├── 08_dashboard_interactivo.html
│   ├── 09_cuellos_botella_identificados.png
│   ├── 10_impacto_before_after.png
│   └── dashboard_completo.html
│
├── .env.example           # Plantilla de variables de entorno
├── .gitignore
├── README.md              # Este archivo
└── requirements.txt       # Dependencias Python

---

## 📖 Documentación

### Análisis Clave

#### 1. Análisis de Hora Pico

**Hallazgos:**
- Las horas 18-23 procesan 45,231 transacciones (45% del total)
- Tiempo promedio **3x más lento** que hora normal (90s vs 30s)
- Tasa de error **3x mayor** (15% vs 5%)

**Impacto:**
- 42.1 horas de tiempo desperdiciado
- Afecta experiencia del usuario
- Genera carga en soporte

**Solución Propuesta:**
- Sistema de colas con priorización
- Escalado automático en hora pico
- Batch processing para transacciones de baja prioridad

#### 2. Validaciones Manuales

**Hallazgos:**
- 15,789 transacciones (15.8%) requieren validación manual
- Tiempo promedio de validación: 95 segundos
- 68% podrían automatizarse con reglas simples

**Impacto:**
- 35.2 horas en validaciones que podrían ser automáticas
- Cuello de botella operativo
- Escalabilidad limitada

**Solución Propuesta:**
- Sistema de validación automática basado en reglas
- Machine Learning para detección de fraude
- Validación manual solo para casos complejos

#### 3. Performance por Método de Pago

**Hallazgos:**
- Transferencias bancarias: 78s promedio
- Wallets crypto: 25s promedio
- Diferencia de 53 segundos por transacción

**Impacto:**
- 18.7 horas de overhead
- Experiencia inconsistente
- Posible pérdida de usuarios

**Solución Propuesta:**
- Optimizar integración con bancos
- Promover métodos más rápidos
- Implementar caching inteligente

### Metodología

1. **Recolección de Datos**
   - Generación de 100,000 transacciones con patrones realistas
   - Simulación de comportamiento de 10,000 usuarios
   - Distribución temporal y geográfica basada en datos reales

2. **Análisis Exploratorio**
   - SQL para análisis agregado y métricas
   - Python para análisis estadístico profundo
   - Identificación de patrones y anomalías

3. **Identificación de Cuellos de Botella**
   - Análisis de percentiles (P50, P95, P99)
   - Comparación de segmentos (hora pico vs normal, etc.)
   - Cuantificación de impacto

4. **Propuestas de Optimización**
   - Simulación de soluciones
   - Análisis costo-beneficio
   - Priorización por impacto

5. **Visualización y Comunicación**
   - Dashboard interactivo
   - Gráficos de impacto
   - Documentación ejecutiva

---

## Conocimientos aplicados:

### Técnicos
- ✅ Diseño de esquemas de base de datos complejos con PostgreSQL
- ✅ Optimización de queries SQL para análisis de grandes volúmenes
- ✅ Generación de datos realistas con Faker
- ✅ Visualización avanzada con Matplotlib, Seaborn y Plotly
- ✅ Análisis estadístico con Pandas y NumPy

### De Negocio
- ✅ Identificación sistemática de cuellos de botella operativos
- ✅ Cuantificación de impacto y ROI de optimizaciones
- ✅ Priorización de iniciativas por impacto/esfuerzo
- ✅ Comunicación efectiva de hallazgos técnicos a audiencias ejecutivas

### Metodológicos
- ✅ Enfoque data-driven para toma de decisiones
- ✅ Simulación de escenarios para validar hipótesis
- ✅ Iteración basada en métricas
- ✅ Documentación exhaustiva para replicabilidad

---

## 🤝 Contribuciones

Este es un proyecto personal para demostración de habilidades en análisis de datos y optimización de operaciones. Sin embargo, sugerencias y mejoras son bienvenidas.

### Posibles Mejoras Futuras
- [ ] Implementar modelos de Machine Learning para predicción de fallos
- [ ] Añadir análisis de series temporales (ARIMA, Prophet)
- [ ] Integrar con APIs reales de CoinGecko para datos actualizados
- [ ] Crear API REST para consulta de métricas
- [ ] Dockerizar la aplicación completa

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver archivo `LICENSE` para más detalles.

---

## 👤 Autor

**Lautaro Oyarzun**

- 🌐 Portfolio: https://laos-portfolio.vercel.app/
- 💼 LinkedIn: https://www.linkedin.com/in/oyarzunlautaro/
- 📧 Email: devlautarooyarzun@gmail.com
- 🐱 GitHub: [@oyar23](https://github.com/oyar23)

---

## 🙏 Agradecimientos

- Inspiración: Problemas reales en plataformas fintech/crypto
- Datos: Generados con Faker y patrones de uso reales
- Stack: Comunidad open-source de Python y PostgreSQL

---

<div align="center">

**⭐ Si este proyecto te resultó útil, considera darle una estrella en GitHub ⭐**



</div>