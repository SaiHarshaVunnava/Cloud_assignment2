import os, sys, time, json
import psycopg
from psycopg.errors import UndefinedTable

# Environment variables with defaults
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASS = os.getenv("DB_PASS", "secretpw")
DB_NAME = os.getenv("DB_NAME", "appdb")
TOP_N   = int(os.getenv("APP_TOP_N", "5"))

def connect_with_retry(retries=12, delay=1.5, factor=1.5):
    """Try to connect with exponential backoff."""
    last_err = None
    for i in range(retries):
        try:
            return psycopg.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                dbname=DB_NAME,
                connect_timeout=3,
            )
        except Exception as e:
            last_err = e
            wait = min(delay * (factor ** i), 10)
            print(f"Waiting for database... ({e})", file=sys.stderr)
            time.sleep(wait)
    print("Failed to connect to Postgres:", last_err, file=sys.stderr)
    sys.exit(1)

def fetch_summary(conn, top_n):
    with conn.cursor() as cur:
        # Total trips
        cur.execute("SELECT COUNT(*) FROM trips;")
        total_trips = cur.fetchone()[0]

        # Average fare by city (cast to float in SQL to avoid Decimal handling)
        cur.execute("""
            SELECT city, AVG(fare)::float
            FROM trips
            GROUP BY city
            ORDER BY city;
        """)
        by_city = [{"city": c, "avg_fare": a} for (c, a) in cur.fetchall()]

        # Top-N longest trips
        cur.execute("""
            SELECT id, city, minutes, fare::float
            FROM trips
            ORDER BY minutes DESC
            LIMIT %s;
        """, (top_n,))
        top = [{"id": i, "city": c, "minutes": m, "fare": f}
               for (i, c, m, f) in cur.fetchall()]

    return {
        "total_trips": int(total_trips),
        "avg_fare_by_city": by_city,
        "top_by_minutes": top,
    }

def main():
    conn = connect_with_retry()
    try:
        summary = fetch_summary(conn, TOP_N)
    except UndefinedTable:
        print("ERROR: table 'trips' not found. Did init.sql run in the DB container?", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    # Write to /out/summary.json (mounted to host via Docker)
    out_dir = "/out"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "summary.json")
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)

    # Console output
    print("=== Summary ===")
    print(json.dumps(summary, indent=2))
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()