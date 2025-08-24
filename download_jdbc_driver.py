#!/usr/bin/env python3
"""
Script to download and install the PostgreSQL JDBC driver if it's missing
"""

import os
import urllib.request
import shutil

def download_postgresql_jdbc_driver():
    """Download the PostgreSQL JDBC driver if it's not already present"""
    
    # Define the target directory and filename
    target_dir = "/tmp/jdbc-drivers"
    driver_filename = "postgresql-42.4.4.jar"
    target_path = os.path.join(target_dir, driver_filename)
    
    # Check if the driver already exists
    if os.path.exists(target_path):
        print(f"PostgreSQL JDBC driver already exists at {target_path}")
        return target_path
    
    # Create the target directory if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)
    
    # Define the download URL
    download_url = "https://jdbc.postgresql.org/download/postgresql-42.4.4.jar"
    
    try:
        print(f"Downloading PostgreSQL JDBC driver from {download_url}...")
        urllib.request.urlretrieve(download_url, target_path)
        print(f"Successfully downloaded PostgreSQL JDBC driver to {target_path}")
        return target_path
    except Exception as e:
        print(f"Error downloading PostgreSQL JDBC driver: {e}")
        return None

if __name__ == "__main__":
    driver_path = download_postgresql_jdbc_driver()
    if driver_path:
        print(f"PostgreSQL JDBC driver is available at: {driver_path}")
    else:
        print("Failed to download PostgreSQL JDBC driver")