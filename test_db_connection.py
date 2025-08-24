#!/usr/bin/env python3
"""
Test script to verify database connectivity using db_utils.py
"""

import sys
import os

# Add the dags directory to the Python path
dags_path = os.path.join(os.path.dirname(__file__), 'dags')
sys.path.insert(0, dags_path)

try:
    from db_utils import (
        get_postgres_connection_params,
        get_postgres_jdbc_properties
    )
    print("Successfully imported db_utils")
    
    # Test getting connection parameters
    print("Testing database connection parameters...")
    params = get_postgres_connection_params("postgres_grafana")
    
    print(f"Host: {params['host']}")
    print(f"Port: {params['port']}")
    print(f"Database: {params['database']}")
    print(f"User: {params['user']}")
    print(f"Password: {'*' * len(params['password']) if params['password'] else 'None'}")
    print(f"JDBC URL: {params['jdbc_url']}")
    
    # Test JDBC properties
    jdbc_props = get_postgres_jdbc_properties(params)
    print(f"JDBC Properties: {jdbc_props}")
    
    print("Database connection test completed successfully!")
    
    # Try to import socket to test host resolution
    import socket
    try:
        resolved_ip = socket.gethostbyname(params['host'])
        print(f"Successfully resolved host {params['host']} to IP {resolved_ip}")
    except socket.gaierror as e:
        print(f"Failed to resolve host {params['host']}: {e}")
    
except Exception as e:
    print(f"Error during database connection test: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)