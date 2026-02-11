"""Quick script to verify zones table."""
import os
from sqlalchemy import create_engine, text

PG_USER = os.getenv('PG_USER', 'root')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'root')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'ny_taxi')

CONNECTION_STRING = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = create_engine(CONNECTION_STRING)

print("\n=== Zones Table Information ===")
with engine.connect() as conn:
    # Get count
    result = conn.execute(text("SELECT COUNT(*) FROM zones"))
    print(f"Total zones: {result.fetchone()[0]}")
    
    # Get sample data
    print("\nSample zones:")
    result = conn.execute(text("SELECT * FROM zones ORDER BY \"LocationID\" LIMIT 10"))
    for row in result:
        print(f"  ID: {row[0]:3d} | {row[1]:15s} | {row[2]:35s} | {row[3]}")
    
    # Get borough breakdown
    print("\nZones by borough:")
    result = conn.execute(text('SELECT "Borough", COUNT(*) as count FROM zones GROUP BY "Borough" ORDER BY count DESC'))
    for row in result:
        print(f"  {row[0]:20s}: {row[1]:3d} zones")
