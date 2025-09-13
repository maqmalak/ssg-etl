"""
Detailed test script to diagnose database connection issues
"""

import sys
import os
import psycopg2

def test_password_variations():
    """Test different password variations"""
    print("=== Testing Password Variations ===")
    
    # Common password variations
    passwords = [
        "P@akistan12",  # The corrected one we're using
        "P@kistan12",   # The old typo version
        "postgres",     # Default postgres password
        "password",     # Common default
        "P@ssw0rd123",  # Another possible variation
    ]
    
    host = os.getenv("POSTGRES_HOST", "172.16.7.6")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "ssg")
    user = os.getenv("POSTGRES_USER", "postgres")
    
    print(f"Connection parameters:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    print(f"  Environment POSTGRES_PASSWORD: {os.getenv('POSTGRES_PASSWORD', 'Not set')}")
    
    for password in passwords:
        print(f"\nTesting password: {password}")
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            print(f"✓ SUCCESS with password: {password}")
            cursor = conn.cursor()
            cursor.execute("SELECT current_user;")
            current_user = cursor.fetchone()[0]
            print(f"  Connected as user: {current_user}")
            cursor.close()
            conn.close()
            return password  # Return the working password
        except Exception as e:
            print(f"  ✗ FAILED: {str(e)[:100]}...")  # Truncate long error messages
    
    print("✗ No password variation worked")
    return None

def check_environment_variables():
    """Check all relevant environment variables"""
    print("\n=== Environment Variables ===")
    relevant_vars = [
        "POSTGRES_HOST",
        "POSTGRES_PORT", 
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD"
    ]
    
    for var in relevant_vars:
        value = os.getenv(var, "Not set")
        # Mask password for security
        if "PASSWORD" in var:
            masked_value = "*" * len(value) if value != "Not set" else "Not set"
            print(f"{var}: {masked_value}")
        else:
            print(f"{var}: {value}")

if __name__ == "__main__":
    print("=== Detailed Database Connection Diagnosis ===\n")
    
    check_environment_variables()
    print()
    working_password = test_password_variations()
    
    if working_password:
        print(f"\n🎉 Found working password: {working_password}")
        print("Please update your environment variables or configuration to use this password.")
    else:
        print(f"\n❌ No working password found.")
        print("Please check your PostgreSQL server configuration and credentials.")