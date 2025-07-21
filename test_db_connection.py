import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from your .env.local file
load_dotenv(".env.local")

def test_db():
    # Build connection string with explicit search_path=public
    conn_string = (
        f"host={os.getenv('DB_IP')} "
        f"dbname={os.getenv('DB_NAME')} "
        f"user={os.getenv('DB_USERNAME')} "
        f"password={os.getenv('DB_PASSWORD')} "
        f"options='-csearch_path=public'"
    )

    print("Trying to connect with:", conn_string)

    try:
        # Connect to the database
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # Check the search path
        cur.execute("SHOW search_path;")
        sp = cur.fetchone()
        print("Current search_path:", sp)

        # Check if 'curtailment' table exists in 'public' schema
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'curtailment'
            );
        """)
        exists = cur.fetchone()[0]
        print("Does 'curtailment' table exist in public schema?", exists)

        if exists:
            # Run a simple test query
            cur.execute("SELECT * FROM public.curtailment LIMIT 3;")
            rows = cur.fetchall()
            print("Sample data from curtailment table:")
            for row in rows:
                print(row)
        else:
            print("Table 'curtailment' does not exist. Please check your DB.")

    except Exception as e:
        print("Error connecting or querying database:", e)

    finally:
        # Clean up
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    test_db()
