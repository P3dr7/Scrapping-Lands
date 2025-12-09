#!/usr/bin/env python3
"""Check parks with websites."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    # Count parks with websites
    r = conn.execute(text("SELECT COUNT(*) FROM parks_master WHERE website IS NOT NULL AND website != ''"))
    print(f"Parques com website: {r.fetchone()[0]}")
    
    # Count by state
    r = conn.execute(text("""
        SELECT state, COUNT(*) 
        FROM parks_master 
        WHERE website IS NOT NULL AND website != ''
        GROUP BY state
        ORDER BY COUNT(*) DESC
    """))
    print("\nPor estado:")
    for row in r:
        print(f"  {row[0]}: {row[1]}")
    
    # Check contacts table
    r = conn.execute(text("SELECT COUNT(*) FROM contacts"))
    print(f"\nContatos atuais: {r.fetchone()[0]}")
