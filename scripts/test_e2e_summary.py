#!/usr/bin/env python3
"""
Teste End-to-End - Resumo de Produção.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import text
from src.database import get_engine

def main():
    engine = get_engine()
    with engine.connect() as conn:
        print('='*60)
        print('  TESTE END-TO-END - RESULTADOS DE PRODUÇÃO')
        print('='*60)
        
        # Contagens
        tables = {
            'parks_raw': 'Parques Brutos (Google Places)',
            'parks_master': 'Parques Deduplicados',
            'owners': 'Proprietários Identificados',
            'companies': 'Empresas Vinculadas', 
            'contacts': 'Contatos Coletados'
        }
        
        print('\n📊 BANCO DE DADOS:')
        for t, desc in tables.items():
            result = conn.execute(text(f'SELECT COUNT(*) FROM {t}'))
            print(f'   {desc}: {result.scalar()}')
        
        # Contatos por tipo
        result = conn.execute(text('''
            SELECT source, COUNT(*) 
            FROM contacts 
            GROUP BY source
        '''))
        print('\n📞 CONTATOS POR FONTE:')
        for row in result:
            print(f'   {row[0]}: {row[1]}')
        
        # Emails e telefones
        result = conn.execute(text('''
            SELECT 
                COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as emails,
                COUNT(CASE WHEN phone IS NOT NULL THEN 1 END) as phones
            FROM contacts
        '''))
        row = result.fetchone()
        print(f'\n   Emails coletados: {row[0]}')
        print(f'   Telefones coletados: {row[1]}')

    # Verificar CSV
    print('\n📁 ARQUIVO CSV GERADO:')
    csv_files = [f for f in os.listdir('output') if f.endswith('.csv')]
    latest = sorted(csv_files)[-1]
    df = pd.read_csv(f'output/{latest}')
    print(f'   Arquivo: {latest}')
    print(f'   Tamanho: {os.path.getsize(f"output/{latest}")/1024:.1f} KB')
    print(f'   Registros: {len(df)}')
    print(f'   Colunas: {len(df.columns)}')

    # Amostras com contato
    with_contact = df[(df['primary_email'].notna()) | (df['primary_phone'].notna())]
    print(f'\n✅ LEADS COM CONTATO: {len(with_contact)}/{len(df)}')

    print('\n📋 EXEMPLOS DE LEADS COM CONTATO:')
    sample = with_contact[['park_name', 'primary_email', 'primary_phone']].head(5)
    for _, row in sample.iterrows():
        print(f'   {row["park_name"]}')
        if pd.notna(row['primary_email']):
            print(f'      Email: {row["primary_email"]}')
        if pd.notna(row['primary_phone']):
            print(f'      Tel: {row["primary_phone"]}')

    print('\n' + '='*60)
    print('  TESTE END-TO-END CONCLUÍDO COM SUCESSO!')
    print('='*60)


if __name__ == "__main__":
    main()
