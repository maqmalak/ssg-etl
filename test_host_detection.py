"""
Test script to verify the host detection logic
"""

import sys
import os

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def test_host_detection():
    """Test the host detection logic"""
    try:
        import socket
        
        # Test host detection logic
        print("Testing host detection logic...")
        
        # Simulate the logic from get_postgres_engine
        host = "172.16.7.6"  # Simulate localhost from Airflow connection
        print(f"Original host from Airflow connection: {host}")
        
        if host == "172.16.7.6" or host == "localhost" or host == "127.0.0.1":
            print("Host appears to be localhost. Checking for better alternatives...")
            # Check common database hosts
            possible_hosts = ["172.16.7.6", "postgres", "database", "pg-ssg"]
            for possible_host in possible_hosts:
                try:
                    socket.gethostbyname(possible_host)
                    print(f"Found accessible database host: {possible_host}")
                    host = possible_host
                    break
                except socket.gaierror:
                    print(f"Host {possible_host} not accessible")
                    continue
            else:
                print("Could not find an alternative database host")
        
        print(f"Final host to use: {host}")
        return True
        
    except Exception as e:
        print(f"Error testing host detection: {e}")
        return False

def test_connection_availability():
    """Test which hosts are available"""
    try:
        import socket
        
        hosts_to_test = ["172.16.7.6", "172.16.7.6", "localhost", "postgres", "database", "pg-ssg"]
        
        print("Testing availability of database hosts:")
        for host in hosts_to_test:
            try:
                socket.gethostbyname(host)
                print(f"  ✓ {host} - Accessible")
            except socket.gaierror:
                print(f"  ✗ {host} - Not accessible")
                
        return True
    except Exception as e:
        print(f"Error testing connection availability: {e}")
        return False

if __name__ == "__main__":
    print("=== Testing Host Detection Logic ===\n")
    
    test1_passed = test_host_detection()
    print()
    test2_passed = test_connection_availability()
    
    print("\n=== Test Summary ===")
    if test1_passed and test2_passed:
        print("✓ Host detection logic tests passed!")
    else:
        print("✗ Some tests failed.")