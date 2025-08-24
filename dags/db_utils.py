"""
Database utility functions for the ETL pipeline.
These functions can be used by both Airflow DAGs and Spark scripts.
"""

from __future__ import annotations

import os
import socket
from typing import Dict, Any

from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def get_postgres_connection_params(connection_id: str = "postgres_grafana") -> Dict[str, Any]:
    """
    Get PostgreSQL connection parameters that can be used by Spark.
    
    Args:
        connection_id (str): Airflow connection ID (used to get default values if environment variables aren't set)
        
    Returns:
        Dict[str, Any]: Database connection parameters
    """
    # In a Spark context, we can't use Airflow's BaseHook, so we'll rely on environment variables
    # These would be set in the Airflow connection or environment
    
    try:
        # Try to get from Airflow connection
        connection = BaseHook.get_connection(connection_id)
        host = connection.host
        port = str(connection.port) if connection.port else "5432"
        database = connection.schema
        user = connection.login
        password = connection.password
    except:
        # Default values that match the Airflow connection
        # Try to detect if we're in Docker environment
        host = detect_database_host()
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }


def detect_database_host() -> str:
    """
    Detect the appropriate database host based on the environment.
    
    Returns:
        str: The appropriate database host
    """
    # Check if we're in a Docker environment by trying to resolve common Docker service names
    docker_hosts = ["postgres", "postgres_grafana", "database"]
    
    for host in docker_hosts:
        try:
            socket.gethostbyname(host)
            print(f"Detected Docker environment, using database host: {host}")
            return host
        except socket.gaierror:
            continue
    
    # If we're not in Docker, check environment variable or use localhost
    host = os.getenv("POSTGRES_HOST", "172.16.7.6")
    print(f"Using database host: {host}")
    return host


def get_postgres_jdbc_properties(connection_params: Dict[str, Any]) -> Dict[str, str]:
    """
    Get JDBC properties for connecting to PostgreSQL.
    
    Args:
        connection_params (Dict[str, Any]): Connection parameters from get_postgres_connection_params
        
    Returns:
        Dict[str, str]: JDBC properties
    """
    return {
        "user": connection_params["user"],
        "password": connection_params["password"]
    }