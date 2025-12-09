#!/usr/bin/env python3
"""
Script para criar owners a partir dos nomes dos parques.

Como não temos acesso aos County Assessor Records, usamos uma abordagem 
alternativa: assumir que o nome do parque é frequentemente o nome da 
entidade proprietária (LLC, Inc, etc) ou um DBA (doing business as).

Este script:
1. Analisa nomes de parques para detectar entidades corporativas
2. Cria registros na tabela owners
3. Vincula parks_master aos owners criados

Author: BellaTerra Intelligence
Date: December 2025
"""

import sys
import re
from pathlib import Path
from datetime import datetime
from loguru import logger
from sqlalchemy import text

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_db_session, test_connection


# Padrões para detectar entidades corporativas
CORPORATE_PATTERNS = [
    r'\bLLC\b',
    r'\bL\.L\.C\.?\b',
    r'\bINC\.?\b',
    r'\bINCORPORATED\b',
    r'\bCORP\.?\b',
    r'\bCORPORATION\b',
    r'\bLTD\.?\b',
    r'\bLIMITED\b',
    r'\bLP\b',
    r'\bL\.P\.?\b',
    r'\bLLP\b',
    r'\bL\.L\.P\.?\b',
    r'\bPC\b',
    r'\bPLC\b',
    r'\bTRUST\b',
    r'\bHOLDINGS?\b',
    r'\bPROPERTIES\b',
    r'\bINVESTMENTS?\b',
    r'\bENTERPRISES?\b',
    r'\bGROUP\b',
    r'\bPARTNERS?\b',
    r'\bASSOCIATES?\b',
    r'\bCOMPAN(Y|IES)\b',
    r'\bCO\.?\b',
    r'\bMGMT\b',
    r'\bMANAGEMENT\b',
]

def is_likely_corporate(name: str) -> bool:
    """Verifica se o nome sugere uma entidade corporativa."""
    name_upper = name.upper()
    for pattern in CORPORATE_PATTERNS:
        if re.search(pattern, name_upper):
            return True
    return False


def extract_entity_type(name: str) -> str:
    """Extrai o tipo de entidade do nome."""
    name_upper = name.upper()
    
    if re.search(r'\bLLC\b|\bL\.L\.C\.?\b', name_upper):
        return 'LLC'
    elif re.search(r'\bINC\.?\b|\bINCORPORATED\b', name_upper):
        return 'Corporation'
    elif re.search(r'\bCORP\.?\b|\bCORPORATION\b', name_upper):
        return 'Corporation'
    elif re.search(r'\bLTD\.?\b|\bLIMITED\b', name_upper):
        return 'Limited'
    elif re.search(r'\bLP\b|\bL\.P\.?\b', name_upper):
        return 'LP'
    elif re.search(r'\bLLP\b|\bL\.L\.P\.?\b', name_upper):
        return 'LLP'
    elif re.search(r'\bTRUST\b', name_upper):
        return 'Trust'
    else:
        return 'Business'  # DBA/Trade name


def create_owners_from_parks():
    """Cria registros de owners a partir dos nomes dos parques."""
    
    logger.info("=" * 70)
    logger.info("CRIANDO OWNERS A PARTIR DOS PARQUES")
    logger.info("=" * 70)
    
    with get_db_session() as session:
        # Buscar todos os parques que ainda não têm owner
        result = session.execute(text("""
            SELECT id, master_id, name, address, city, state, zip_code, phone, email
            FROM parks_master
            WHERE owner_id IS NULL
            ORDER BY state, city, name
        """))
        
        parks = result.fetchall()
        logger.info(f"📊 Total de parques sem owner: {len(parks)}")
        
        if not parks:
            logger.info("✅ Todos os parques já têm owner vinculado")
            return
        
        created = 0
        linked = 0
        
        for park in parks:
            park_id = park[0]
            master_id = park[1]
            name = park[2]
            address = park[3]
            city = park[4]
            state = park[5]
            zip_code = park[6]
            phone = park[7]
            email = park[8]
            
            # Determinar se é corporativo ou individual (assumimos business name)
            is_corporate = is_likely_corporate(name)
            entity_type = extract_entity_type(name) if is_corporate else None
            
            try:
                # Verificar se já existe um owner com este nome
                existing = session.execute(text("""
                    SELECT id FROM owners WHERE full_name = :name LIMIT 1
                """), {'name': name}).fetchone()
                
                if existing:
                    owner_id = existing[0]
                else:
                    # Criar novo owner
                    result = session.execute(text("""
                        INSERT INTO owners (
                            full_name, 
                            mailing_address, mailing_city, mailing_state, mailing_zip,
                            phone, email,
                            is_individual, is_verified,
                            source, source_reference,
                            mail_eligible,
                            sos_lookup_status
                        ) VALUES (
                            :full_name,
                            :address, :city, :state, :zip,
                            :phone, :email,
                            :is_individual, FALSE,
                            'park_name', :master_id,
                            TRUE,
                            :sos_status
                        )
                        RETURNING id
                    """), {
                        'full_name': name,
                        'address': address,
                        'city': city,
                        'state': state,
                        'zip': zip_code,
                        'phone': phone,
                        'email': email,
                        'is_individual': not is_corporate,
                        'master_id': str(master_id),
                        'sos_status': 'pending' if is_corporate else 'not_applicable'
                    })
                    
                    owner_id = result.fetchone()[0]
                    created += 1
                
                # Vincular park ao owner
                session.execute(text("""
                    UPDATE parks_master SET owner_id = :owner_id WHERE id = :park_id
                """), {'owner_id': owner_id, 'park_id': park_id})
                
                linked += 1
                
                if linked % 100 == 0:
                    logger.info(f"  Progresso: {linked}/{len(parks)} parques vinculados")
                    session.commit()
                
            except Exception as e:
                logger.error(f"Erro ao processar {name}: {e}")
                continue
        
        session.commit()
        
        logger.info("")
        logger.info("=" * 70)
        logger.success("✅ PROCESSAMENTO CONCLUÍDO")
        logger.info("=" * 70)
        logger.info(f"  Owners criados: {created}")
        logger.info(f"  Parques vinculados: {linked}")
        
        # Estatísticas
        result = session.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN is_individual = FALSE THEN 1 END) as corporate,
                COUNT(CASE WHEN is_individual = TRUE THEN 1 END) as individual,
                COUNT(CASE WHEN sos_lookup_status = 'pending' THEN 1 END) as pending_sos
            FROM owners
        """))
        stats = result.fetchone()
        
        logger.info("")
        logger.info("📊 Estatísticas de Owners:")
        logger.info(f"  Total: {stats[0]}")
        logger.info(f"  Corporativos (para SOS lookup): {stats[1]}")
        logger.info(f"  Individuais/DBA: {stats[2]}")
        logger.info(f"  Pendentes de SOS: {stats[3]}")


def main():
    print("=" * 70)
    print("FASE 3 ALTERNATIVA: OWNERS A PARTIR DOS NOMES DOS PARQUES")
    print("=" * 70)
    print()
    
    # Configurar logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    
    # Testar conexão
    print("1. Testando conexão com banco...")
    if not test_connection():
        print("   ❌ Falha na conexão")
        return
    print("   ✅ Conexão OK")
    print()
    
    # Criar owners
    create_owners_from_parks()


if __name__ == "__main__":
    main()
