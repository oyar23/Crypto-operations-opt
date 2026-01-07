-- Eliminar tablas si existen (para desarrollo)
DROP TABLE IF EXISTS logs_sistema CASCADE;
DROP TABLE IF EXISTS validaciones CASCADE;
DROP TABLE IF EXISTS metricas_operativas CASCADE;
DROP TABLE IF EXISTS transacciones CASCADE;
DROP TABLE IF EXISTS usuarios CASCADE;

-- ============================================
-- TABLA: usuarios
-- Almacena información de usuarios/clientes
-- ============================================

CREATE TABLE usuarios (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    fecha_registro TIMESTAMP NOT NULL DEFAULT NOW(),
    pais VARCHAR(50) NOT NULL,
    ciudad VARCHAR(100),
    nivel_verificacion VARCHAR(20) NOT NULL CHECK (nivel_verificacion IN ('basico', 'intermedio', 'completo')),
    fecha_ultima_verificacion TIMESTAMP,
    estado_cuenta VARCHAR(20) NOT NULL DEFAULT 'activa' CHECK (estado_cuenta IN ('activa', 'suspendida', 'cerrada')),
    total_transacciones INT DEFAULT 0,
    volumen_total_usd DECIMAL(15,2) DEFAULT 0.00,
    fecha_ultima_transaccion TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()

);

-- Índices para optimizar consultas
CREATE INDEX idx_usuarios_pais ON usuarios(pais);
CREATE INDEX idx_usuarios_nivel_verificacion ON usuarios(nivel_verificacion);
CREATE INDEX idx_usuarios_estado_cuenta ON usuarios(estado_cuenta);
CREATE INDEX idx_usuarios_fecha_registro ON usuarios(fecha_registro);

-- ============================================
-- TABLA: transacciones
-- Registro detallado de todas las operaciones
-- ============================================

CREATE TABLE transacciones (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES usuarios(user_id) ON DELETE CASCADE,
    
    -- Información de la operación
    tipo_operacion VARCHAR(20) NOT NULL CHECK (tipo_operacion IN ('compra', 'venta', 'swap', 'transferencia', 'retiro')),
    cripto VARCHAR(10) NOT NULL,
    cripto_destino VARCHAR(10), -- Para swaps
    
    -- Montos
    cantidad_cripto DECIMAL(20,8) NOT NULL,
    precio_unitario_usd DECIMAL(15,2) NOT NULL,
    monto_usd DECIMAL(15,2) NOT NULL,
    comision_usd DECIMAL(10,2) DEFAULT 0.00,
    monto_total_usd DECIMAL(15,2) NOT NULL,
    
    -- Procesamiento
    timestamp_inicio TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_completado TIMESTAMP,
    tiempo_procesamiento INT, -- en segundos
    
    -- Estado y validación
    estado VARCHAR(20) NOT NULL DEFAULT 'pendiente' 
        CHECK (estado IN ('pendiente', 'procesando', 'exitosa', 'fallida', 'cancelada')),
    motivo_fallo TEXT,
    requiere_validacion_manual BOOLEAN DEFAULT FALSE,
    fecha_validacion_manual TIMESTAMP,
    
    -- Método de pago/retiro
    metodo_pago VARCHAR(50) NOT NULL,
    referencia_externa VARCHAR(100),
    
    -- Información de red blockchain (si aplica)
    network VARCHAR(50),
    hash_blockchain VARCHAR(100),
    confirmaciones_blockchain INT DEFAULT 0,
    
    -- Detección de fraude
    score_fraude DECIMAL(5,2), -- 0-100
    flagged_fraude BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    ip_address INET,
    user_agent TEXT,
    dispositivo VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Índices para optimización
CREATE INDEX idx_transacciones_user_id ON transacciones(user_id);
CREATE INDEX idx_transacciones_timestamp_inicio ON transacciones(timestamp_inicio);
CREATE INDEX idx_transacciones_estado ON transacciones(estado);
CREATE INDEX idx_transacciones_tipo_operacion ON transacciones(tipo_operacion);
CREATE INDEX idx_transacciones_cripto ON transacciones(cripto);
CREATE INDEX idx_transacciones_metodo_pago ON transacciones(metodo_pago);
CREATE INDEX idx_transacciones_tiempo_procesamiento ON transacciones(tiempo_procesamiento);
CREATE INDEX idx_transacciones_flagged_fraude ON transacciones(flagged_fraude) WHERE flagged_fraude = TRUE;

-- Índice compuesto para análisis por hora
CREATE INDEX idx_transacciones_hora_estado ON transacciones(
    EXTRACT(HOUR FROM timestamp_inicio), 
    estado
);

-- ============================================
-- TABLA: metricas_operativas
-- Métricas agregadas por hora para monitoreo
-- ============================================

CREATE TABLE metricas_operativas (
    metrica_id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    hora INT NOT NULL CHECK (hora >= 0 AND hora <= 23),
    
    -- Métricas de volumen
    num_transacciones INT DEFAULT 0,
    num_transacciones_exitosas INT DEFAULT 0,
    num_transacciones_fallidas INT DEFAULT 0,
    num_usuarios_activos INT DEFAULT 0,
    
    -- Métricas de tiempo
    tiempo_promedio_procesamiento DECIMAL(10,2), -- segundos
    tiempo_mediano_procesamiento DECIMAL(10,2),
    tiempo_p95_procesamiento DECIMAL(10,2), -- percentil 95
    tiempo_max_procesamiento INT,
    
    -- Métricas de calidad
    tasa_error DECIMAL(5,2), -- porcentaje
    tasa_validacion_manual DECIMAL(5,2), -- porcentaje
    tasa_fraude DECIMAL(5,2), -- porcentaje
    
    -- Métricas de volumen financiero
    volumen_total_usd DECIMAL(15,2),
    volumen_promedio_usd DECIMAL(15,2),
    comisiones_totales_usd DECIMAL(10,2),
    
    -- Métricas por tipo de operación
    num_compras INT DEFAULT 0,
    num_ventas INT DEFAULT 0,
    num_swaps INT DEFAULT 0,
    num_retiros INT DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraint único: una fila por fecha-hora
    CONSTRAINT unique_fecha_hora UNIQUE (fecha, hora)
);

-- Índices
CREATE INDEX idx_metricas_fecha ON metricas_operativas(fecha);
CREATE INDEX idx_metricas_hora ON metricas_operativas(hora);
CREATE INDEX idx_metricas_fecha_hora ON metricas_operativas(fecha, hora);

-- ============================================
-- TABLA: validaciones
-- Registro de validaciones manuales y automáticas
-- ============================================

CREATE TABLE validaciones (
    validacion_id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL REFERENCES transacciones(transaction_id) ON DELETE CASCADE,
    
    tipo_validacion VARCHAR(30) NOT NULL 
        CHECK (tipo_validacion IN ('automatica', 'manual', 'antifraude', 'compliance')),
    
    resultado VARCHAR(20) NOT NULL 
        CHECK (resultado IN ('aprobada', 'rechazada', 'pendiente')),
    
    motivo TEXT,
    validado_por VARCHAR(50), -- 'sistema' o nombre del analista
    
    timestamp_validacion TIMESTAMP DEFAULT NOW(),
    tiempo_validacion INT, -- segundos
    
    -- Detalles de la validación
    reglas_aplicadas TEXT[], -- array de reglas verificadas
    score_confianza DECIMAL(5,2), -- 0-100
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- Índices
CREATE INDEX idx_validaciones_transaction_id ON validaciones(transaction_id);
CREATE INDEX idx_validaciones_tipo ON validaciones(tipo_validacion);
CREATE INDEX idx_validaciones_resultado ON validaciones(resultado);
CREATE INDEX idx_validaciones_timestamp ON validaciones(timestamp_validacion);

-- ============================================
-- TABLA: logs_sistema
-- Registro de eventos y errores del sistema
-- ============================================

CREATE TABLE logs_sistema (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    
    nivel VARCHAR(20) NOT NULL 
        CHECK (nivel IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    
    componente VARCHAR(50) NOT NULL, -- 'procesador_transacciones', 'validador', etc.
    mensaje TEXT NOT NULL,
    
    -- Contexto adicional
    transaction_id INT REFERENCES transacciones(transaction_id),
    user_id INT REFERENCES usuarios(user_id),
    
    detalles_json JSONB, -- información adicional en formato JSON
    stack_trace TEXT,
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- Índices
CREATE INDEX idx_logs_timestamp ON logs_sistema(timestamp);
CREATE INDEX idx_logs_nivel ON logs_sistema(nivel);
CREATE INDEX idx_logs_componente ON logs_sistema(componente);
CREATE INDEX idx_logs_transaction_id ON logs_sistema(transaction_id) WHERE transaction_id IS NOT NULL;

- ============================================
-- VISTAS ÚTILES
-- ============================================

-- Vista: Resumen de transacciones por usuario
CREATE OR REPLACE VIEW vista_resumen_usuarios AS
SELECT 
    u.user_id,
    u.username,
    u.email,
    u.pais,
    u.nivel_verificacion,
    u.estado_cuenta,
    u.fecha_registro,
    COUNT(t.transaction_id) as total_transacciones,
    COUNT(CASE WHEN t.estado = 'exitosa' THEN 1 END) as transacciones_exitosas,
    COUNT(CASE WHEN t.estado = 'fallida' THEN 1 END) as transacciones_fallidas,
    SUM(CASE WHEN t.estado = 'exitosa' THEN t.monto_usd ELSE 0 END) as volumen_total_usd,
    AVG(CASE WHEN t.estado = 'exitosa' THEN t.tiempo_procesamiento END) as tiempo_promedio_procesamiento,
    MAX(t.timestamp_inicio) as fecha_ultima_transaccion
FROM usuarios u
LEFT JOIN transacciones t ON u.user_id = t.user_id
GROUP BY u.user_id, u.username, u.email, u.pais, u.nivel_verificacion, u.estado_cuenta, u.fecha_registro;

-- Vista: Métricas en tiempo real (última hora)
CREATE OR REPLACE VIEW vista_metricas_tiempo_real AS
SELECT 
    COUNT(*) as transacciones_ultima_hora,
    COUNT(CASE WHEN estado = 'exitosa' THEN 1 END) as exitosas,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) as fallidas,
    AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END) as tiempo_promedio,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_usd,
    COUNT(DISTINCT user_id) as usuarios_activos
FROM transacciones
WHERE timestamp_inicio >= NOW() - INTERVAL '1 hour';

-- Vista: Top criptomonedas por volumen
CREATE OR REPLACE VIEW vista_top_criptos AS
SELECT 
    cripto,
    COUNT(*) as num_transacciones,
    SUM(CASE WHEN estado = 'exitosa' THEN monto_usd ELSE 0 END) as volumen_total_usd,
    AVG(CASE WHEN estado = 'exitosa' THEN tiempo_procesamiento END) as tiempo_promedio_seg,
    COUNT(CASE WHEN estado = 'fallida' THEN 1 END) * 100.0 / COUNT(*) as tasa_error_pct
FROM transacciones
WHERE timestamp_inicio >= NOW() - INTERVAL '30 days'
GROUP BY cripto
ORDER BY volumen_total_usd DESC;

-- ============================================
-- FUNCIONES ÚTILES
-- ============================================

-- Función: Calcular tiempo de procesamiento al completar transacción
CREATE OR REPLACE FUNCTION actualizar_tiempo_procesamiento()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.timestamp_completado IS NOT NULL AND OLD.timestamp_completado IS NULL THEN
        NEW.tiempo_procesamiento = EXTRACT(EPOCH FROM (NEW.timestamp_completado - NEW.timestamp_inicio))::INT;
    END IF;
    
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_actualizar_tiempo_procesamiento
    BEFORE UPDATE ON transacciones
    FOR EACH ROW
    EXECUTE FUNCTION actualizar_tiempo_procesamiento();

-- Función: Actualizar estadísticas de usuario
CREATE OR REPLACE FUNCTION actualizar_estadisticas_usuario()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.estado = 'exitosa' AND (OLD.estado IS NULL OR OLD.estado != 'exitosa') THEN
        UPDATE usuarios 
        SET 
            total_transacciones = total_transacciones + 1,
            volumen_total_usd = volumen_total_usd + NEW.monto_usd,
            fecha_ultima_transaccion = NEW.timestamp_completado,
            updated_at = NOW()
        WHERE user_id = NEW.user_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_actualizar_estadisticas_usuario
    AFTER UPDATE ON transacciones
    FOR EACH ROW
    WHEN (NEW.estado = 'exitosa')
    EXECUTE FUNCTION actualizar_estadisticas_usuario();

-- ============================================
-- DATOS DE CONFIGURACIÓN INICIAL
-- ============================================

-- Insertar configuraciones del sistema
CREATE TABLE IF NOT EXISTS configuracion_sistema (
    clave VARCHAR(100) PRIMARY KEY,
    valor TEXT NOT NULL,
    descripcion TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO configuracion_sistema (clave, valor, descripcion) VALUES
('tiempo_timeout_transaccion', '300', 'Tiempo máximo en segundos para procesar una transacción'),
('umbral_fraude', '75', 'Score mínimo para marcar como sospechoso de fraude'),
('tasa_comision_compra', '0.5', 'Porcentaje de comisión para compras'),
('tasa_comision_venta', '0.5', 'Porcentaje de comisión para ventas'),
('limite_diario_basico', '1000', 'Límite diario en USD para usuarios básicos'),
('limite_diario_intermedio', '5000', 'Límite diario en USD para usuarios intermedios'),
('limite_diario_completo', '50000', 'Límite diario en USD para usuarios completos')
ON CONFLICT (clave) DO NOTHING;

-- ============================================
-- GRANTS Y PERMISOS
-- ============================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cryptoops_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cryptoops_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO cryptoops_user;

-- ============================================
-- COMENTARIOS EN TABLAS Y COLUMNAS
-- ============================================

COMMENT ON TABLE usuarios IS 'Tabla principal de usuarios/clientes de la plataforma';
COMMENT ON TABLE transacciones IS 'Registro completo de todas las transacciones crypto';
COMMENT ON TABLE metricas_operativas IS 'Métricas agregadas por hora para análisis de performance';
COMMENT ON TABLE validaciones IS 'Histórico de validaciones automáticas y manuales';
COMMENT ON TABLE logs_sistema IS 'Registro de eventos y errores del sistema';

COMMENT ON COLUMN transacciones.tiempo_procesamiento IS 'Tiempo en segundos desde inicio hasta completado';
COMMENT ON COLUMN transacciones.score_fraude IS 'Score de 0-100 donde >75 es sospechoso';
COMMENT ON COLUMN metricas_operativas.tiempo_p95_procesamiento IS 'Percentil 95 de tiempos de procesamiento';

-- ============================================
-- FINALIZACIÓN
-- ============================================

-- Verificar que todas las tablas se crearon correctamente
DO $$
DECLARE
    tabla_count INT;
BEGIN
    SELECT COUNT(*) INTO tabla_count
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE';
    
    RAISE NOTICE ' Schema creado exitosamente. Total de tablas: %', tabla_count;
END $$;

-- Mostrar resumen de tablas creadas
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as num_columnas
FROM information_schema.tables t
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
ORDER BY table_name;