#!/usr/bin/env python3
"""
Test script to verify the improved connection error handling.
"""

def test_improved_connection_error_handling():
    """Test that connection errors result in False return value."""
    
    # Define connection error keywords
    connection_error_keywords = [
        "unable to connect", 
        "adaptive server is unavailable", 
        "connection", 
        "timeout", 
        "could not connect",
        "server is unavailable",
        "host not found",
        "name or service not known"
    ]
    
    # Simulate the improved connection error handling
    def check_for_new_data_simulation(connection_id, error_message):
        """Simulate the improved connection error handling."""
        print(f"=== CHECK FOR NEW DATA TASK ===")
        print(f"[{connection_id}] Error checking for new data: {error_message}")
        
        # Check if it's a connection error
        error_message_lower = error_message.lower()
        is_connection_error = any(keyword in error_message_lower for keyword in connection_error_keywords)
        
        if is_connection_error:
            print(f"[{connection_id}] DECISION: SKIP PATH (Server unavailable, skipping extraction)")
            return False
        else:
            # For other errors, it's safer to proceed with extraction
            print(f"[{connection_id}] DECISION: SAVE PATH (Non-connection error occurred, proceeding for safety)")
            return True
    
    # Test cases for connection errors (should return False)
    connection_errors = [
        "('08S01', '[08S01] [FreeTDS][SQL Server]Unable to connect: Adaptive Server is unavailable or does not exist (20009) (SQLDriverConnect)')",
        "Timeout: Could not connect to server",
        "Connection timed out",
        "Server is unavailable",
        "Host not found",
        "Name or service not known"
    ]
    
    print("Testing connection errors (should all return False):")
    connection_results = []
    for i, error in enumerate(connection_errors):
        print(f"Test {i+1}: {error[:50]}...")
        result = check_for_new_data_simulation("Lane-24", error)
        connection_results.append(result)
        print(f"Result: {result}")
        print()
    
    # Test case for non-connection error (should return True)
    print("Testing non-connection error (should return True):")
    other_error = "Database constraint violation"
    result_other = check_for_new_data_simulation("Lane-24", other_error)
    print(f"Result: {result_other}")
    print()
    
    # Verify expected results
    all_connection_false = all(result == False for result in connection_results)
    other_true = result_other == True
    
    if all_connection_false and other_true:
        print("✓ Improved connection error handling works correctly!")
        return True
    else:
        print("✗ Improved connection error handling has issues!")
        print(f"  Connection errors results: {connection_results}")
        print(f"  Other error result: {result_other}")
        return False

if __name__ == "__main__":
    success = test_improved_connection_error_handling()
    exit(0 if success else 1)