#!/usr/bin/env python3
"""
Test script to verify database host resolution
"""

import socket
import os

def test_host_resolution():
    """Test resolving different database hostnames"""
    hosts_to_test = ["postgres", "postgres_grafana", "localhost", "127.0.0.1", "172.16.7.6"]
    
    for host in hosts_to_test:
        try:
            result = socket.gethostbyname(host)
            print(f"✓ Successfully resolved {host} to {result}")
        except socket.gaierror as e:
            print(f"✗ Failed to resolve {host}: {e}")

if __name__ == "__main__":
    print("Testing database host resolution...")
    test_host_resolution()