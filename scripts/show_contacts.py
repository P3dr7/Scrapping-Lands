#!/usr/bin/env python3
"""Resumo dos contatos no banco."""
import sys
sys.path.insert(0, '.')
from sqlalchemy import text
from src.database import get_engine

engine = get_engine()
with engine.connect() as conn:
    # Contagem de contatos
    result = conn.execute(text('SELECT COUNT(*) FROM contacts'))
    total = result.scalar()
    print(f'Total de contatos: {total}')
    
    # Contatos por tipo
    result = conn.execute(text('''
        SELECT contact_type, COUNT(*) 
        FROM contacts 
        GROUP BY contact_type
    '''))
    print('\nPor tipo:')
    for row in result:
        print(f'  {row[0]}: {row[1]}')
    
    # Emails vs telefones
    result = conn.execute(text('''
        SELECT 
            COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as emails,
            COUNT(CASE WHEN phone IS NOT NULL THEN 1 END) as phones
        FROM contacts
    '''))
    row = result.fetchone()
    print(f'\nEmails: {row[0]}')
    print(f'Telefones: {row[1]}')
    
    # Exemplos
    result = conn.execute(text('''
        SELECT c.email, c.phone, pm.name
        FROM contacts c
        JOIN parks_master pm ON c.park_id = pm.id
        LIMIT 5
    '''))
    print('\nExemplos:')
    for row in result:
        email = row[0] or ""
        phone = row[1] or ""
        print(f'  {row[2]}: {email} / {phone}')
