#!/usr/bin/env python3
"""
Setup PostgreSQL database for price transparency project
"""
import os
import psycopg2

def main():
    print("🔧 Setting up PostgreSQL database...")

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        user='hpt_owner',
        password='hpt_owner_pw',
        dbname='hpt_db'
    )
    print("✅ PostgreSQL connected successfully")

    with conn.cursor() as cur:
        # Create schema if it doesn't exist
        cur.execute('CREATE SCHEMA IF NOT EXISTS hpt;')
        print('✅ Schema hpt created/verified')

        # Execute SQL files (use 02_tables.sql for single table structure)
        sql_files = ['02_tables.sql']  # Only need the single table structure
        for sql_file in sql_files:
            print(f'📄 Executing {sql_file}...')
            with open(f'warehouse/sql/{sql_file}', 'r', encoding='utf-8') as f:
                sql = f.read()
                cur.execute(sql)

        print('✅ Database schema initialized successfully')

        # Verify tables were created
        cur.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname = 'hpt'
            ORDER BY tablename;
        """)
        tables = cur.fetchall()
        print(f'📋 Tables in hpt schema: {len(tables)}')
        for schema, table in tables:
            print(f'  - {schema}.{table}')

        conn.commit()

    conn.close()
    print("🎉 Database setup complete!")

if __name__ == "__main__":
    main()
