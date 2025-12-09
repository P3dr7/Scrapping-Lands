"""Script simples para verificar tabelas no banco."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_db_session
from sqlalchemy import text

with get_db_session() as session:
    # Verificar colunas de contacts
    print("Colunas de contacts:")
    result = session.execute(text("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name='contacts'
        ORDER BY ordinal_position
    """))
    for row in result.fetchall():
        print(f"  - {row[0]}: {row[1]}")
    
    # Verificar colunas de companies
    print("\nColunas de companies:")
    result = session.execute(text("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name='companies'
        ORDER BY ordinal_position
    """))
    for row in result.fetchall():
        print(f"  - {row[0]}: {row[1]}")
