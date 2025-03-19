import psycopg2
import subprocess
import sys

# Source and Destination database configurations
SOURCE_DB = {
    "dbname": "source_db",
    "user": "source_user",
    "password": "source_password",
    "host": "source_host",
    "port": "5432"
}

DEST_DB = {
    "dbname": "dest_db",
    "user": "dest_user",
    "password": "dest_password",
    "host": "dest_host",
    "port": "5432"
}

# Schema to migrate (set to None to migrate all)
SCHEMA = None  # e.g., "public"


def check_connection(db_config):
    """Check database connectivity."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                print(f"Connected to {db_config['dbname']} ✅")
    except Exception as e:
        print(f"Failed to connect to {db_config['dbname']} ❌\nError: {e}")
        sys.exit(1)


def dump_schema():
    """Dumps the schema from the source database."""
    schema_option = f"--schema={SCHEMA}" if SCHEMA else "--schema-only"
    dump_file = "schema_dump.sql"

    cmd = [
        "pg_dump",
        "-h", SOURCE_DB["host"],
        "-U", SOURCE_DB["user"],
        "-d", SOURCE_DB["dbname"],
        schema_option,
        "--no-owner",
        "--no-privileges",
        "-f", dump_file
    ]

    result = subprocess.run(cmd, env={"PGPASSWORD": SOURCE_DB["password"]}, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Schema dumped successfully to {dump_file} ✅")
    else:
        print(f"Schema dump failed ❌\n{result.stderr}")
        sys.exit(1)


def restore_schema():
    """Restores the dumped schema to the destination database."""
    restore_file = "schema_dump.sql"

    cmd = [
        "psql",
        "-h", DEST_DB["host"],
        "-U", DEST_DB["user"],
        "-d", DEST_DB["dbname"],
        "-f", restore_file
    ]

    result = subprocess.run(cmd, env={"PGPASSWORD": DEST_DB["password"]}, capture_output=True, text=True)
    if result.returncode == 0:
        print("Schema restored successfully ✅")
    else:
        print(f"Schema restore failed ❌\n{result.stderr}")
        sys.exit(1)


def get_tables(db_config):
    """Fetch all tables in the specified schema."""
    schema_filter = f"AND schemaname = '{SCHEMA}'" if SCHEMA else ""
    query = f"SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') {schema_filter};"

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return [(row[0], row[1]) for row in cur.fetchall()]
    except Exception as e:
        print(f"Error fetching tables: {e}")
        sys.exit(1)


def migrate_data():
    """Migrates data from the source to the destination database using COPY."""
    tables = get_tables(SOURCE_DB)
    if not tables:
        print("No tables found to migrate.")
        return

    for schema, table in tables:
        file_path = f"{schema}_{table}.csv"

        # Export data from source
        export_cmd = f"""
            PGPASSWORD={SOURCE_DB['password']} psql -h {SOURCE_DB['host']} -U {SOURCE_DB['user']} -d {SOURCE_DB['dbname']} -c "COPY {schema}.{table} TO STDOUT WITH CSV HEADER"
        """
        with open(file_path, "w") as f:
            result = subprocess.run(export_cmd, shell=True, stdout=f, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print(f"Error exporting {schema}.{table} ❌\n{result.stderr}")
                continue
        print(f"Exported {schema}.{table} ✅")

        # Import data to destination
        import_cmd = f"""
            PGPASSWORD={DEST_DB['password']} psql -h {DEST_DB['host']} -U {DEST_DB['user']} -d {DEST_DB['dbname']} -c "COPY {schema}.{table} FROM STDIN WITH CSV HEADER"
        """
        with open(file_path, "r") as f:
            result = subprocess.run(import_cmd, shell=True, stdin=f, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print(f"Error importing {schema}.{table} ❌\n{result.stderr}")
            else:
                print(f"Imported {schema}.{table} ✅")


def verify_migration():
    """Checks row counts before and after migration for verification."""
    tables = get_tables(SOURCE_DB)

    for schema, table in tables:
        query = f"SELECT COUNT(*) FROM {schema}.{table};"

        try:
            with psycopg2.connect(**SOURCE_DB) as src_conn, psycopg2.connect(**DEST_DB) as dest_conn:
                with src_conn.cursor() as src_cur, dest_conn.cursor() as dest_cur:
                    src_cur.execute(query)
                    dest_cur.execute(query)
                    src_count = src_cur.fetchone()[0]
                    dest_count = dest_cur.fetchone()[0]

                    if src_count == dest_count:
                        print(f"✅ {schema}.{table} verified: {src_count} rows")
                    else:
                        print(f"❌ Mismatch in {schema}.{table}: Source={src_count}, Destination={dest_count}")
        except Exception as e:
            print(f"Error verifying {schema}.{table}: {e}")


def get_table_definition_pg_dump(DB_CONFIG, table_name):
    """Fetches the table definition using pg_dump."""
    try:
        result = subprocess.run(
            [
                "pg_dump",
                "-h", DB_CONFIG["host"],
                "-U", DB_CONFIG["user"],
                "-d", DB_CONFIG["dbname"],
                "-t", f"{SCHEMA}.{table_name}",
                "--schema-only"
            ],
            capture_output=True,
            text=True,
            env={"PGPASSWORD": DB_CONFIG["password"]}
        )
        return result.stdout if result.returncode == 0 else f"Error fetching {table_name}"
    except Exception as e:
        return f"Error fetching {table_name}: {e}"


def save_definitions_to_file(output_file="table_definitions.sql"):
    """Retrieves table definitions and saves them to a file."""
    tables = get_tables()
    if not tables:
        print("No tables found.")
        return

    with open(output_file, "w", encoding="utf-8") as f:
        for table in tables:
            ddl = get_table_definition_pg_dump(table)
            f.write(f"-- DDL for {table}\n")
            f.write(ddl + "\n\n")

    print(f"Table definitions saved to {output_file}")


# if __name__ == "__main__":
#     save_definitions_to_file()


def migrate():
    """Runs the entire migration process."""
    print("Checking connections...")
    check_connection(SOURCE_DB)
    check_connection(DEST_DB)

    print("\nDumping and restoring schema...")
    dump_schema()
    restore_schema()

    print("\nMigrating data...")
    migrate_data()

    print("\nVerifying data integrity...")
    verify_migration()

    print("\nMigration completed successfully! 🎉")


if __name__ == "__main__":
    migrate()
