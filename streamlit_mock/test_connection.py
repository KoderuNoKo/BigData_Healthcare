"""
test_connection.py
Simple script to test PostgreSQL connection
"""

import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    """Test PostgreSQL connection with various common configurations"""
    
    # Try different connection configurations
    configs = [
    {
        "name": "Docker Postgres (host access)",
        "host": "localhost",
        "port": "5432",
        "database": "mimic_dw",
        "user": "dev",
        "password": "devpassword"
    },
    {
        "name": "Environment Variables",
        "host": os.getenv("PG_HOST", "localhost"),
        "port": os.getenv("PG_PORT", "5432"),
        "database": os.getenv("PG_DATABASE", "mimic_dw"),
        "user": os.getenv("PG_USER", "dev"),
        "password": os.getenv("PG_PASSWORD", "devpassword")
    }
    ]
    
    print("🔍 Testing PostgreSQL Connections\n")
    print("=" * 50)
    
    successful_connection = None
    
    for config in configs:
        print(f"\n📡 Testing: {config['name']}")
        print(f"   Host: {config['host']}:{config['port']}")
        print(f"   Database: {config['database']}")
        print(f"   User: {config['user']}")
        
        try:
            # Attempt connection
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['user'],
                password=config['password']
            )
            
            # Test with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
            print(f"   ✅ SUCCESS!")
            print(f"   PostgreSQL Version: {version[:50]}...")
            
            # Get database size
            cursor.execute("""
                SELECT pg_database.datname,
                       pg_size_pretty(pg_database_size(pg_database.datname)) AS size
                FROM pg_database
                WHERE datname = %s
            """, (config['database'],))
            
            db_info = cursor.fetchone()
            if db_info:
                print(f"   Database Size: {db_info[1]}")
            
            # Get table count
            cursor.execute("""
                SELECT count(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            table_count = cursor.fetchone()[0]
            print(f"   Tables: {table_count}")
            
            cursor.close()
            conn.close()
            
            if not successful_connection:
                successful_connection = config
                
        except OperationalError as e:
            print(f"   ❌ FAILED: {str(e).split('FATAL:')[0] if 'FATAL:' in str(e) else str(e)[:100]}")
        except Exception as e:
            print(f"   ❌ ERROR: {str(e)[:100]}")
    
    print("\n" + "=" * 50)
    
    if successful_connection:
        print("\n✅ Successfully connected to PostgreSQL!")
        print("\n📝 Add these settings to your .env file or .streamlit/secrets.toml:\n")
        print(f"PG_HOST={successful_connection['host']}")
        print(f"PG_PORT={successful_connection['port']}")
        print(f"PG_DATABASE={successful_connection['database']}")
        print(f"PG_USER={successful_connection['user']}")
        print(f"PG_PASSWORD={successful_connection['password']}")
        
        print("\n🚀 You can now run: streamlit run app.py")
    else:
        print("\n❌ Could not connect to PostgreSQL")
        print("\n🔧 Troubleshooting steps:")
        print("1. Check if PostgreSQL container is running:")
        print("   docker ps -f name=postgres")
        print("\n2. Start the container if it's stopped:")
        print("   docker start postgres")
        print("\n3. Check container logs for errors:")
        print("   docker logs postgres --tail 20")
        print("\n4. Try connecting directly:")
        print("   docker exec -it postgres psql -U postgres")

if __name__ == "__main__":
    test_connection()