#!/usr/bin/env python3
"""
Script para executar migrações SQL.
"""
import os
import sys

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.database import get_engine
from loguru import logger


def run_migration(migration_file: str):
    """Executa um arquivo de migração SQL."""
    
    # Lê o arquivo SQL
    with open(migration_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Divide em statements individuais (separados por ;)
    # Mas mantém blocos DO $$ ... $$ juntos
    statements = []
    current = []
    in_block = False
    
    for line in sql_content.split('\n'):
        stripped = line.strip()
        
        # Detecta início de bloco DO $$
        if stripped.startswith('DO $$') or stripped.startswith('DO $'):
            in_block = True
            current.append(line)
        elif in_block:
            current.append(line)
            # Detecta fim de bloco
            if stripped.endswith('$$;'):
                statements.append('\n'.join(current))
                current = []
                in_block = False
        else:
            current.append(line)
            # Detecta fim de statement normal
            if stripped.endswith(';') and not stripped.startswith('--'):
                stmt = '\n'.join(current).strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
                current = []
    
    # Adiciona qualquer statement pendente
    if current:
        stmt = '\n'.join(current).strip()
        if stmt:
            statements.append(stmt)
    
    # Filtra statements vazios e comentários puros
    statements = [s for s in statements if s.strip() and not s.strip().startswith('--')]
    
    engine = get_engine()
    
    logger.info(f"Executando migração: {migration_file}")
    logger.info(f"Total de statements: {len(statements)}")
    
    success_count = 0
    error_count = 0
    
    with engine.connect() as conn:
        for i, stmt in enumerate(statements, 1):
            # Pula comentários puros
            lines = [l for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
            if not lines:
                continue
                
            # Mostra preview do statement
            preview = lines[0][:80] if lines else "???"
            logger.debug(f"[{i}/{len(statements)}] {preview}...")
            
            try:
                conn.execute(text(stmt))
                success_count += 1
            except Exception as e:
                error_msg = str(e)
                # Ignora erros de "já existe"
                if 'already exists' in error_msg or 'já existe' in error_msg:
                    logger.debug(f"  -> Já existe, pulando")
                    success_count += 1
                else:
                    logger.error(f"Erro no statement {i}: {e}")
                    logger.error(f"Statement: {stmt[:200]}")
                    error_count += 1
        
        conn.commit()
    
    logger.info(f"Migração concluída: {success_count} sucesso, {error_count} erros")
    return error_count == 0


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Executa migrações SQL")
    parser.add_argument("file", nargs="?", default="migrations/002_corporate_registry.sql",
                        help="Arquivo de migração a executar")
    args = parser.parse_args()
    
    # Caminho absoluto
    if not os.path.isabs(args.file):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        args.file = os.path.join(project_root, args.file)
    
    if not os.path.exists(args.file):
        logger.error(f"Arquivo não encontrado: {args.file}")
        sys.exit(1)
    
    success = run_migration(args.file)
    sys.exit(0 if success else 1)
