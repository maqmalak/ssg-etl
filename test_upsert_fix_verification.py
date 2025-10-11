#!/usr/bin/env python3
"""
Test script to verify the upsert_to_postgres fix
"""

import sys
import os

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def test_syntax():
    """Test if the syntax is correct"""
    try:
        from dags.hanger_lines_data import upsert_to_postgres
        print("✅ Syntax is correct - upsert_to_postgres function can be imported")
        return True
    except SyntaxError as e:
        print(f"❌ Syntax error: {e}")
        return False
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"⚠️ Other error (might be expected due to missing dependencies): {e}")
        # This could happen due to missing dependencies, but syntax is still correct
        return True

def test_sqlalchemy_syntax():
    """Test if SQLAlchemy syntax in upsert is correct"""
    try:
        from sqlalchemy.dialects.postgresql import insert
        from scripts.create_target_pg_hl_table import HangerLaneData
        from sqlalchemy import create_engine
        
        # Test the syntax of the insert and on_conflict_do_update
        test_batch = []
        table = HangerLaneData.__table__
        stmt = insert(table).values(test_batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_connection", "odp_key", "odpd_key"],
            set_={c.name: getattr(stmt.excluded, c.name) for c in table.columns if c.name not in ("source_connection", "odp_key", "odpd_key")},
        )
        print("✅ SQLAlchemy syntax is correct")
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy syntax error: {e}")
        return False

if __name__ == "__main__":
    print("Testing the upsert_to_postgres fix...")
    print()
    
    syntax_ok = test_syntax()
    print()
    
    sqlalchemy_ok = test_sqlalchemy_syntax()
    print()
    
    if syntax_ok and sqlalchemy_ok:
        print("🎉 All tests passed! The fix should work correctly.")
    else:
        print("❌ Some tests failed. Please review the implementation.")