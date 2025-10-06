#!/usr/bin/env python3
\"\"\"
Simple test script to validate syntax of the modified DAG file
\"\"\"

import ast
import sys

def test_syntax():
    \"\"\"Test that the modified DAG file has correct Python syntax\"\"\"
    try:
        print(\"Testing syntax of hanger_line_21_to_23.py...\")
        with open('/home/maqmalak/ETL/ssg-etl/dags/hanger_line_21_to_23.py', 'r') as file:
            content = file.read()
        
        # Parse the file to check for syntax errors
        ast.parse(content)
        print(\"✓ Syntax is valid!\")
        
        # Check that the new functions are present
        has_source_check = 'check_source_connection' in content
        has_target_check = 'check_target_connection' in content
        has_updated_decide = 'source_conn_ok: bool, target_conn_ok: bool' in content
        
        print(f\"✓ Source connection check function found: {has_source_check}\")
        print(f\"✓ Target connection check function found: {has_target_check}\")
        print(f\"✓ Updated decision function found: {has_updated_decide}\")
        
        if has_source_check and has_target_check and has_updated_decide:
            print(\"✓ All expected modifications are present in the file\")
            return True
        else:
            print(\"✗ Some expected modifications are missing\")
            return False
            
    except SyntaxError as e:
        print(f\"✗ Syntax error in the file: {e}\")
        return False
    except Exception as e:
        print(f\"✗ Error testing file: {e}\")
        return False

if __name__ == \"__main__\":
    print(\"Testing connection check implementation...\")
    success = test_syntax()
    
    if success:
        print(\"\\n✓ All tests passed! The connection check implementation was applied correctly.\")
    else:
        print(\"\\n✗ Tests failed! Please check the implementation.\")
        sys.exit(1)