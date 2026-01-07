-- ============================================
-- CRYPTOOPS ANALYZER - ANÁLISIS EXPLORATORIO SQL
-- Queries para identificar patrones y métricas clave
-- ============================================

-- ============================================
-- 1. MÉTRICAS GENERALES DEL SISTEMA
-- ============================================

-- 1.1 Overview general de transacciones
SELECT 
    COUNT(*) as total_transacciones,
    COUNT(DISTINCT user_id) as usuarios_activos,
    COUNT(CASE WHEN estado = 'exitosa' THEN 1 END) as transacciones_exitosas,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) as transacciones_fallidas,
    ROUND(COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*), 2) as tasa_error_pct,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN monto_usd END), 2) as ticket_promedio_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg
FROM transacciones
WHERE timestamp_inicio >= '2024-07-01';

-- 1.2 Distribución de usuarios por nivel de verificación
SELECT 
    nivel_verificacion,
    COUNT(*) as num_usuarios,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje,
    COUNT(CASE WHEN estado_cuenta = 'activa' THEN 1 END) as activos,
    ROUND(AVG(total_transacciones), 2) as transacciones_promedio,
    ROUND(AVG(volumen_total_usd), 2) as volumen_promedio_usd
FROM usuarios
GROUP BY nivel_verificacion
ORDER BY num_usuarios DESC;

-- 1.3 Top 10 usuarios más activos
SELECT 
    u.user_id,
    u.username,
    u.pais,
    u.nivel_verificacion,
    u.total_transacciones,
    ROUND(u.volumen_total_usd, 2) as volumen_total_usd,
    u.fecha_ultima_transaccion
FROM usuarios u
WHERE estado_cuenta = 'activa'
ORDER BY total_transacciones DESC
LIMIT 10;

-- ============================================
-- 2. ANÁLISIS DE TIEMPO DE PROCESAMIENTO
-- ============================================

-- 2.1 Tiempo promedio de procesamiento por hora del día
SELECT 
    EXTRACT(HOUR FROM timestamp_inicio) as hora_del_dia,
    COUNT(*) as num_transacciones,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tiempo_procesamiento), 2) as tiempo_mediano_seg,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tiempo_procesamiento), 2) as tiempo_p95_seg,
    MAX(tiempo_procesamiento) as tiempo_maximo_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
WHERE estado IN ('exitosa', 'fallida')
GROUP BY EXTRACT(HOUR FROM timestamp_inicio)
ORDER BY hora_del_dia;

-- 2.2 Identificar transacciones anormalmente lentas (>5 minutos = 300 seg)
SELECT 
    transaction_id,
    user_id,
    tipo_operacion,
    cripto,
    monto_usd,
    tiempo_procesamiento,
    timestamp_inicio,
    estado,
    motivo_fallo,
    requiere_validacion_manual
FROM transacciones
WHERE tiempo_procesamiento > 300
ORDER BY tiempo_procesamiento DESC
LIMIT 100;

-- 2.3 Comparación de tiempos: hora pico vs. hora normal
SELECT 
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp_inicio) BETWEEN 18 AND 23 THEN 'Hora Pico'
        ELSE 'Hora Normal'
    END as periodo,
    COUNT(*) as num_transacciones,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tiempo_procesamiento), 2) as tiempo_p95_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
WHERE estado IN ('exitosa', 'fallida')
GROUP BY 
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp_inicio) BETWEEN 18 AND 23 THEN 'Hora Pico'
        ELSE 'Hora Normal'
    END;

-- ============================================
-- 3. ANÁLISIS POR TIPO DE OPERACIÓN
-- ============================================

-- 3.1 Performance por tipo de operación
SELECT 
    tipo_operacion,
    COUNT(*) as num_transacciones,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN monto_usd END), 2) as monto_promedio_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
GROUP BY tipo_operacion
ORDER BY num_transacciones DESC;

-- 3.2 Análisis de operaciones de swap (las más complejas)
SELECT 
    cripto as cripto_origen,
    cripto_destino,
    COUNT(*) as num_swaps,
    ROUND(AVG(monto_usd), 2) as monto_promedio_usd,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
WHERE tipo_operacion = 'swap'
GROUP BY cripto, cripto_destino
ORDER BY num_swaps DESC
LIMIT 20;

-- ============================================
-- 4. ANÁLISIS POR CRIPTOMONEDA
-- ============================================

-- 4.1 Performance por criptomoneda
SELECT 
    cripto,
    COUNT(*) as num_transacciones,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN monto_usd END), 2) as ticket_promedio_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tiempo_procesamiento) FILTER (WHERE estado = 'exitosa'), 2) as tiempo_p95_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
GROUP BY cripto
ORDER BY volumen_total_usd DESC;

-- 4.2 Comparación BTC vs ETH (las principales)
SELECT 
    cripto,
    COUNT(*) as transacciones,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg,
    COUNT(CASE WHEN tiempo_procesamiento > 120 THEN 1 END) as transacciones_lentas,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
WHERE cripto IN ('BTC', 'ETH')
AND estado IN ('exitosa', 'fallida')
GROUP BY cripto;

-- ============================================
-- 5. ANÁLISIS DE MÉTODOS DE PAGO
-- ============================================

-- 5.1 Performance por método de pago
SELECT 
    metodo_pago,
    COUNT(*) as num_transacciones,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tiempo_procesamiento), 2) as tiempo_p95_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd
FROM transacciones
GROUP BY metodo_pago
ORDER BY num_transacciones DESC;

-- 5.2 Método de pago más problemático
SELECT 
    metodo_pago,
    motivo_fallo,
    COUNT(*) as num_fallos
FROM transacciones
WHERE estado = 'fallida'
GROUP BY metodo_pago, motivo_fallo
ORDER BY num_fallos DESC
LIMIT 20;

-- ============================================
-- 6. ANÁLISIS DE ERRORES Y FALLOS
-- ============================================

-- 6.1 Top motivos de fallo
SELECT 
    motivo_fallo,
    COUNT(*) as num_fallos,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje,
    ROUND(AVG(monto_usd), 2) as monto_promedio_usd
FROM transacciones
WHERE estado = 'fallida'
GROUP BY motivo_fallo
ORDER BY num_fallos DESC;

-- 6.2 Tasa de error por día de la semana
SELECT 
    TO_CHAR(timestamp_inicio, 'Day') as dia_semana,
    EXTRACT(DOW FROM timestamp_inicio) as dia_numero,
    COUNT(*) as total_transacciones,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) as transacciones_fallidas,
    ROUND(COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*), 2) as tasa_error_pct
FROM transacciones
GROUP BY TO_CHAR(timestamp_inicio, 'Day'), EXTRACT(DOW FROM timestamp_inicio)
ORDER BY dia_numero;

-- 6.3 Transacciones con mayor probabilidad de fallo
SELECT 
    tipo_operacion,
    cripto,
    metodo_pago,
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp_inicio) BETWEEN 18 AND 23 THEN 'Hora Pico'
        ELSE 'Hora Normal'
    END as periodo,
    COUNT(*) as total,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) as fallos,
    ROUND(COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*), 2) as tasa_error_pct
FROM transacciones
GROUP BY tipo_operacion, cripto, metodo_pago, 
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp_inicio) BETWEEN 18 AND 23 THEN 'Hora Pico'
        ELSE 'Hora Normal'
    END
HAVING COUNT(*) >= 100  -- Solo combinaciones con suficientes datos
ORDER BY tasa_error_pct DESC
LIMIT 20;

-- ============================================
-- 7. ANÁLISIS DE VALIDACIONES Y FRAUDE
-- ============================================

-- 7.1 Transacciones que requieren validación manual
SELECT 
    COUNT(*) as total_requieren_validacion,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transacciones), 2) as porcentaje,
    ROUND(AVG(monto_usd), 2) as monto_promedio_usd,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_promedio_seg
FROM transacciones
WHERE requiere_validacion_manual = TRUE;

-- 7.2 Análisis de transacciones marcadas como fraude
SELECT 
    COUNT(*) as transacciones_flagged,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) as rechazadas,
    COUNT(CASE WHEN estado = 'exitosa' THEN 1 END) as aprobadas_despues,
    ROUND(AVG(score_fraude), 2) as score_fraude_promedio,
    ROUND(AVG(monto_usd), 2) as monto_promedio_usd,
    COUNT(DISTINCT user_id) as usuarios_unicos
FROM transacciones
WHERE flagged_fraude = TRUE;

-- 7.3 Usuarios con múltiples transacciones marcadas como fraude
SELECT 
    u.user_id,
    u.username,
    u.pais,
    u.nivel_verificacion,
    COUNT(t.transaction_id) as transacciones_flagged,
    ROUND(AVG(t.score_fraude), 2) as score_promedio,
    SUM(t.monto_usd) as monto_total_usd
FROM usuarios u
JOIN transacciones t ON u.user_id = t.user_id
WHERE t.flagged_fraude = TRUE
GROUP BY u.user_id, u.username, u.pais, u.nivel_verificacion
HAVING COUNT(t.transaction_id) >= 3
ORDER BY transacciones_flagged DESC;

-- ============================================
-- 8. ANÁLISIS GEOGRÁFICO
-- ============================================

-- 8.1 Performance por país
SELECT 
    u.pais,
    COUNT(DISTINCT u.user_id) as num_usuarios,
    COUNT(t.transaction_id) as num_transacciones,
    SUM(CASE WHEN t.estado = 'exitosa' THEN t.monto_usd ELSE 0 END) as volumen_total_usd,
    ROUND(AVG(CASE WHEN t.estado = 'exitosa' THEN t.tiempo_procesamiento END), 2) as tiempo_promedio_seg,
    COUNT(CASE WHEN t.estado = 'fallida' THEN 1 END) * 100.0 / COUNT(t.transaction_id) as tasa_error_pct
FROM usuarios u
LEFT JOIN transacciones t ON u.user_id = t.user_id
WHERE u.estado_cuenta = 'activa'
GROUP BY u.pais
ORDER BY volumen_total_usd DESC;

-- ============================================
-- 9. ANÁLISIS TEMPORAL - TENDENCIAS
-- ============================================

-- 9.1 Evolución semanal del volumen
SELECT 
    DATE_TRUNC('week', timestamp_inicio) as semana,
    COUNT(*) as num_transacciones,
    COUNT(DISTINCT user_id) as usuarios_activos,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
GROUP BY DATE_TRUNC('week', timestamp_inicio)
ORDER BY semana;

-- 9.2 Comparación mensual
SELECT 
    TO_CHAR(timestamp_inicio, 'YYYY-MM') as mes,
    COUNT(*) as num_transacciones,
    COUNT(DISTINCT user_id) as usuarios_activos,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg
FROM transacciones
GROUP BY TO_CHAR(timestamp_inicio, 'YYYY-MM')
ORDER BY mes;

-- ============================================
-- 10. QUERIES PARA IDENTIFICAR CUELLOS DE BOTELLA
-- ============================================

-- 10.1 Horas con peor performance
SELECT 
    fecha,
    hora,
    num_transacciones,
    ROUND(tiempo_promedio_procesamiento, 2) as tiempo_promedio_seg,
    ROUND(tiempo_p95_procesamiento, 2) as tiempo_p95_seg,
    ROUND(tasa_error, 2) as tasa_error_pct
FROM metricas_operativas
WHERE tiempo_promedio_procesamiento > (
    SELECT AVG(tiempo_promedio_procesamiento) * 1.5 
    FROM metricas_operativas
)
ORDER BY tiempo_promedio_procesamiento DESC
LIMIT 50;

-- 10.2 Días con mayor tasa de error
SELECT 
    fecha,
    SUM(num_transacciones) as transacciones_totales,
    ROUND(AVG(tiempo_promedio_procesamiento), 2) as tiempo_promedio_seg,
    ROUND(AVG(tasa_error), 2) as tasa_error_promedio_pct,
    MAX(tasa_error) as tasa_error_maxima_pct
FROM metricas_operativas
GROUP BY fecha
HAVING AVG(tasa_error) > 10  -- Días con >10% de error
ORDER BY tasa_error_promedio_pct DESC;

-- 10.3 Análisis de capacidad por hora
WITH stats_por_hora AS (
    SELECT 
        hora,
        AVG(num_transacciones) as transacciones_promedio,
        MAX(num_transacciones) as transacciones_maximas,
        AVG(tiempo_promedio_procesamiento) as tiempo_promedio,
        AVG(tasa_error) as tasa_error_promedio
    FROM metricas_operativas
    GROUP BY hora
)
SELECT 
    hora,
    ROUND(transacciones_promedio, 0) as txn_promedio,
    transacciones_maximas as txn_pico,
    ROUND(tiempo_promedio, 2) as tiempo_promedio_seg,
    ROUND(tasa_error_promedio, 2) as tasa_error_pct,
    CASE 
        WHEN transacciones_maximas > transacciones_promedio * 2 THEN 'Cuello de Botella'
        WHEN tiempo_promedio > 90 THEN 'Lento'
        WHEN tasa_error_promedio > 10 THEN 'Alta Tasa Error'
        ELSE 'Normal'
    END as estado_operativo
FROM stats_por_hora
ORDER BY hora;

-- ============================================
-- 11. OPORTUNIDADES DE OPTIMIZACIÓN
-- ============================================

-- 11.1 Transacciones que podrían automatizarse (patrones comunes)
SELECT 
    tipo_operacion,
    cripto,
    CASE 
        WHEN monto_usd < 100 THEN 'Bajo (<$100)'
        WHEN monto_usd < 1000 THEN 'Medio ($100-$1000)'
        WHEN monto_usd < 5000 THEN 'Alto ($1000-$5000)'
        ELSE 'Muy Alto (>$5000)'
    END as rango_monto,
    COUNT(*) as num_transacciones,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_actual_promedio_seg,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tiempo_procesamiento), 2) as tiempo_mediano_seg,
    COUNT(CASE WHEN requiere_validacion_manual THEN 1 END) as requieren_validacion
FROM transacciones
WHERE estado = 'exitosa'
GROUP BY tipo_operacion, cripto, 
    CASE 
        WHEN monto_usd < 100 THEN 'Bajo (<$100)'
        WHEN monto_usd < 1000 THEN 'Medio ($100-$1000)'
        WHEN monto_usd < 5000 THEN 'Alto ($1000-$5000)'
        ELSE 'Muy Alto (>$5000)'
    END
HAVING COUNT(*) >= 100
ORDER BY num_transacciones DESC;

-- 11.2 Impacto de automatizar validaciones de bajo riesgo
SELECT 
    'Transacciones < $1000 sin fraude' as categoria,
    COUNT(*) as num_transacciones,
    ROUND(AVG(tiempo_procesamiento), 2) as tiempo_actual_seg,
    -- Estimación: reducción de 60% del tiempo si se automatiza
    ROUND(AVG(tiempo_procesamiento) * 0.4, 2) as tiempo_estimado_automatizado_seg,
    ROUND(AVG(tiempo_procesamiento) - AVG(tiempo_procesamiento) * 0.4, 2) as ahorro_tiempo_seg
FROM transacciones
WHERE monto_usd < 1000
AND flagged_fraude = FALSE
AND estado = 'exitosa';

-- ============================================
-- 12. MÉTRICAS CLAVE PARA DASHBOARD
-- ============================================

-- 12.1 KPIs principales (últimos 30 días)
SELECT 
    COUNT(*) as total_transacciones,
    COUNT(DISTINCT user_id) as usuarios_activos,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN monto_usd END), 2) as ticket_promedio_usd,
    ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo_promedio_seg,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tiempo_procesamiento) FILTER (WHERE estado = 'exitosa'), 2) as tiempo_p95_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct,
    SUM(CASE WHEN estado = 'exitosa' THEN comision_usd ELSE 0 END) as comisiones_totales_usd
FROM transacciones
WHERE timestamp_inicio >= NOW() - INTERVAL '30 days';

-- 12.2 Comparativa mes actual vs mes anterior
WITH mes_actual AS (
    SELECT 
        COUNT(*) as txn,
        ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo
    FROM transacciones
    WHERE timestamp_inicio >= DATE_TRUNC('month', NOW())
),
mes_anterior AS (
    SELECT 
        COUNT(*) as txn,
        ROUND(AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END), 2) as tiempo
    FROM transacciones
    WHERE timestamp_inicio >= DATE_TRUNC('month', NOW()) - INTERVAL '1 month'
    AND timestamp_inicio < DATE_TRUNC('month', NOW())
)
SELECT 
    'Mes Actual' as periodo,
    ma.txn as transacciones,
    ma.tiempo as tiempo_promedio_seg,
    ROUND((ma.txn - mant.txn) * 100.0 / mant.txn, 2) as cambio_volumen_pct,
    ROUND((ma.tiempo - mant.tiempo) * 100.0 / mant.tiempo, 2) as cambio_tiempo_pct
FROM mes_actual ma, mes_anterior mant;



-- Mostrar resumen de análisis ejecutados
SELECT 'Análisis SQL completado exitosamente' as mensaje,
       NOW() as timestamp_finalizacion;