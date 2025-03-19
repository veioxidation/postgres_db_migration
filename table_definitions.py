import psycopg2

DB_CONFIG = {
    "dbname": "your_database",
    "user": "your_user",
    "password": "your_password",
    "host": "your_host",
    "port": "5432"
}

def get_create_table_statements():
    """Fetch CREATE TABLE statements using SQL catalogs."""
    query = """
    SELECT 
        'CREATE TABLE ' || c.oid::regclass || ' (' ||
        string_agg(
            a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod),
            ', '
        ) || ');'
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = 'public'
    AND c.relkind = 'r'  
    AND a.attnum > 0
    GROUP BY c.oid;
    """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
                return [row[0] for row in results]
    except Exception as e:
        return f"Error fetching table definitions: {e}"

if __name__ == "__main__":
    create_statements = get_create_table_statements()
    for statement in create_statements:
        print(statement)
