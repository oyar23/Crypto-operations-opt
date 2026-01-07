"""
Script de verificación del schema de base de datos
Verifica que todas las tablas, índices y constraints estén creados correctamente
"""

import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import DictCursor
from tabulate import tabulate

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"), 
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}
EXPECTED_TABLES = {
    "usuarios",
    "transacciones",
    "metricas_operativas",
    "validaciones",
    "logs_sistema",
    "configuracion_sistema"
}

EXPECTED_VIEWS = {
    "vista_metricas_tiempo_real",
    "vista_top_criptos",
    "vista_resumen_usuarios"
}

EXPECTED_TRIGGERS = {
    "trigger_actualizar_tiempo_procesamiento",
    "trigger_actualizar_estadisticas_usuario"
}

EXPECTED_INDEXES = {
    "usuarios": {
        "idx_usuarios_pais",
        "idx_usuarios_nivel_verificacion",
        "idx_usuarios_estado_cuenta",
        "idx_usuarios_fecha_registro"
    },
    "transacciones": {
        "idx_transacciones_user_id",
        "idx_transacciones_timestamp_inicio",
        "idx_transacciones_estado",
        "idx_transacciones_tipo_operacion",
        "idx_transacciones_cripto",
        "idx_transacciones_metodo_pago"
    }
}

EXPECTED_FKS = {
    ("transacciones", "user_id", "usuarios"),
    ("validaciones", "transaction_id", "transacciones"),
    ("logs_sistema", "transaction_id", "transacciones"),
    ("logs_sistema", "user_id", "usuarios")
}

def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

def main():
    print_section("CRYPTOOPS ANALYZER - VERIFICACIÓN DE SCHEMA")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=DictCursor)

    print(" Conexión exitosa")

    # --- TABLAS ---
    print_section("VERIFICACIÓN DE TABLAS")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema='public'
    """)
    tables = {r["table_name"] for r in cur.fetchall()}
    print("Encontradas:", tables)

    tables_ok = EXPECTED_TABLES.issubset(tables)
    print("Esperadas:", EXPECTED_TABLES)

    # --- VISTAS ---
    print_section("VERIFICACIÓN DE VISTAS")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.views 
        WHERE table_schema='public'
    """)
    views = {r["table_name"] for r in cur.fetchall()}
    print("Encontradas:", views)
    views_ok = EXPECTED_VIEWS.issubset(views)

    # --- TRIGGERS ---
    print_section("VERIFICACIÓN DE TRIGGERS")
    cur.execute("""
        SELECT tgname
        FROM pg_trigger
        WHERE NOT tgisinternal
    """)
    triggers = {r["tgname"] for r in cur.fetchall()}
    print("Encontrados:", triggers)
    triggers_ok = EXPECTED_TRIGGERS.issubset(triggers)

    # --- ÍNDICES ---
    print_section("VERIFICACIÓN DE ÍNDICES")
    cur.execute("""
        SELECT tablename, indexname
        FROM pg_indexes
        WHERE schemaname='public'
    """)
    index_map = {}
    for r in cur.fetchall():
        index_map.setdefault(r["tablename"], set()).add(r["indexname"])

    indexes_ok = True
    for table, expected in EXPECTED_INDEXES.items():
        actual = index_map.get(table, set())
        print(f"{table}: {actual}")
        if not expected.issubset(actual):
            print(f" FALTAN índices en {table}: {expected - actual}")
            indexes_ok = False

    # --- FOREIGN KEYS ---
    print_section("VERIFICACIÓN DE FOREIGN KEYS")
    cur.execute("""
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
    """)
    fks = {(r["table_name"], r["column_name"], r["foreign_table"]) for r in cur.fetchall()}
    print("Encontradas:", fks)
    fks_ok = EXPECTED_FKS.issubset(fks)

    # --- TEST DE INSERCIÓN ---
    print_section("TEST DE INSERCIÓN")
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            INSERT INTO usuarios (username,email,pais,nivel_verificacion)
            VALUES ('test_user','test@test.com','Argentina','basico')
            RETURNING user_id
        """)
        user_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO transacciones (
                user_id, tipo_operacion, cripto,
                cantidad_cripto, precio_unitario_usd,
                monto_usd, monto_total_usd,
                metodo_pago, estado
            )
            VALUES (%s,'compra','BTC',0.1,50000,5000,5000,'tarjeta','exitosa')
            RETURNING transaction_id
        """, (user_id,))
        cur.fetchone()

        cur.execute("ROLLBACK;")
        insert_ok = True
        print(" Inserción + triggers OK")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(" ERROR:", e)
        insert_ok = False

    # --- REPORTE ---
    print_section("REPORTE FINAL")

    checks = {
        "Tablas": tables_ok,
        "Vistas": views_ok,
        "Triggers": triggers_ok,
        "Índices": indexes_ok,
        "Foreign Keys": fks_ok,
        "Inserciones": insert_ok
    }

    for k, v in checks.items():
        print(f"{'yes' if v else 'not'} {k}")

    if all(checks.values()):
        print("\n SCHEMA VÁLIDO – LISTO PARA PRODUCCIÓN")
    else:
        print("\n SCHEMA INCOMPLETO – REVISAR")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
