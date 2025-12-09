#!/usr/bin/env python3
"""
Script de execução para Fase 5: Enriquecimento de Contatos Digitais.

Este script itera sobre parques e empresas no banco de dados,
buscando informações de contato (emails e telefones) através de:
1. Scraping ético de websites (respeitando robots.txt)
2. APIs de enriquecimento (Hunter.io, Apollo, etc.)

Uso:
    python scripts/enrich_contacts.py [OPTIONS]

Options:
    --limit N           Processar apenas N registros (default: sem limite)
    --source TYPE       Fonte a usar: 'website', 'api', 'all' (default: 'all')
    --skip-websites     Pular scraping de websites
    --skip-apis         Pular enriquecimento via APIs
    --reprocess         Reprocessar registros já processados
    --dry-run           Apenas simular, não salvar no banco
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Optional

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker

from src.database import get_engine
from src.enrichment.contact_finder import (
    ContactEnrichmentOrchestrator,
    WebsiteContactScraper,
    HunterIoService,
    ExtractedContact,
    ScrapeResult,
)


def setup_logging():
    """Configura logging para o script."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO",
    )
    logger.add(
        "logs/enrich_contacts_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="DEBUG",
        rotation="1 day",
    )


def get_parks_for_contact_enrichment(
    session, limit: Optional[int] = None, reprocess: bool = False
) -> list:
    """
    Busca parques que precisam de enriquecimento de contatos.
    
    Args:
        session: Sessão SQLAlchemy
        limit: Limite de registros
        reprocess: Se True, inclui parques já processados
        
    Returns:
        Lista de dicionários com dados dos parques
    """
    # Busca parques que têm website ou precisam de contatos
    query = """
        SELECT 
            pm.id,
            pm.name,
            pm.phone,
            pm.website,
            pm.address,
            pm.city,
            pm.latitude,
            pm.longitude,
            COALESCE(
                (SELECT COUNT(*) FROM contacts c WHERE c.park_id = pm.id),
                0
            ) as contact_count
        FROM parks_master pm
        WHERE pm.website IS NOT NULL 
          AND pm.website != ''
    """
    
    if not reprocess:
        # Excluir parques que já têm contatos suficientes
        query += """
          AND COALESCE(
                (SELECT COUNT(*) FROM contacts c WHERE c.park_id = pm.id),
                0
            ) = 0
        """
    
    query += " ORDER BY pm.name"
    
    if limit:
        query += f" LIMIT {limit}"
    
    result = session.execute(text(query))
    parks = []
    for row in result:
        parks.append({
            "id": row[0],
            "name": row[1],
            "phone": row[2],
            "website": row[3],
            "address": row[4],
            "city": row[5],
            "latitude": row[6],
            "longitude": row[7],
            "contact_count": row[8],
        })
    
    return parks


def get_companies_for_contact_enrichment(
    session, limit: Optional[int] = None, reprocess: bool = False
) -> list:
    """
    Busca empresas que precisam de enriquecimento de contatos.
    
    Args:
        session: Sessão SQLAlchemy
        limit: Limite de registros
        reprocess: Se True, inclui empresas já processadas
        
    Returns:
        Lista de dicionários com dados das empresas
    """
    query = """
        SELECT 
            c.id,
            c.legal_name,
            c.registered_agent_name,
            c.registered_agent_address,
            c.principals,
            c.sos_status,
            COALESCE(
                (SELECT COUNT(*) FROM contacts ct WHERE ct.company_id = c.id),
                0
            ) as contact_count
        FROM companies c
        WHERE c.sos_status = 'Active'
    """
    
    if not reprocess:
        query += """
          AND COALESCE(
                (SELECT COUNT(*) FROM contacts ct WHERE ct.company_id = c.id),
                0
            ) = 0
        """
    
    query += " ORDER BY c.legal_name"
    
    if limit:
        query += f" LIMIT {limit}"
    
    result = session.execute(text(query))
    companies = []
    for row in result:
        companies.append({
            "id": row[0],
            "legal_name": row[1],
            "registered_agent_name": row[2],
            "registered_agent_address": row[3],
            "principals": row[4],
            "sos_status": row[5],
            "contact_count": row[6],
        })
    
    return companies


def save_contacts_to_db(
    session,
    contacts: list[ExtractedContact],
    park_id: Optional[int] = None,
    company_id: Optional[int] = None,
    owner_id: Optional[int] = None,
    dry_run: bool = False,
) -> int:
    """
    Salva contatos encontrados no banco de dados.
    
    Args:
        session: Sessão SQLAlchemy
        contacts: Lista de ExtractedContact
        park_id: ID do parque (opcional)
        company_id: ID da empresa (opcional)
        owner_id: ID do owner (opcional)
        dry_run: Se True, não salva no banco
        
    Returns:
        Número de contatos salvos
    """
    saved_count = 0
    
    for contact in contacts:
        # Verifica se o contato já existe
        check_query = """
            SELECT id FROM contacts 
            WHERE (email = :email OR (:email IS NULL AND email IS NULL))
              AND (phone = :phone OR (:phone IS NULL AND phone IS NULL))
              AND (park_id = :park_id OR (:park_id IS NULL AND park_id IS NULL))
              AND (company_id = :company_id OR (:company_id IS NULL AND company_id IS NULL))
        """
        
        existing = session.execute(
            text(check_query),
            {
                "email": contact.email,
                "phone": contact.phone,
                "park_id": park_id,
                "company_id": company_id,
            }
        ).fetchone()
        
        if existing:
            logger.debug(f"Contato já existe: {contact.email or contact.phone}")
            continue
        
        if dry_run:
            logger.info(f"[DRY-RUN] Salvaria contato: {contact}")
            saved_count += 1
            continue
        
        # Insere o novo contato
        insert_query = """
            INSERT INTO contacts (
                park_id, company_id, owner_id,
                contact_type, email, phone, person_name, person_title,
                source, source_url, confidence_level,
                is_valid, created_at, updated_at
            ) VALUES (
                :park_id, :company_id, :owner_id,
                :contact_type, :email, :phone, :person_name, :person_title,
                :source, :source_url, :confidence_level,
                :is_valid, :created_at, :updated_at
            )
        """
        
        session.execute(
            text(insert_query),
            {
                "park_id": park_id,
                "company_id": company_id,
                "owner_id": owner_id,
                "contact_type": contact.contact_type.value if hasattr(contact.contact_type, 'value') else str(contact.contact_type),
                "email": contact.email,
                "phone": contact.phone,
                "person_name": contact.person_name,
                "person_title": contact.person_title,
                "source": contact.source.value if hasattr(contact.source, 'value') else str(contact.source),
                "source_url": contact.source_url,
                "confidence_level": contact.confidence,
                "is_valid": True,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
        )
        saved_count += 1
    
    if not dry_run and saved_count > 0:
        session.commit()
    
    return saved_count


def process_parks_websites(
    session,
    scraper: WebsiteContactScraper,
    parks: list,
    dry_run: bool = False,
) -> dict:
    """
    Processa websites de parques para extrair contatos.
    
    Returns:
        Estatísticas do processamento
    """
    stats = {
        "processed": 0,
        "contacts_found": 0,
        "contacts_saved": 0,
        "errors": 0,
    }
    
    for park in parks:
        website = park.get("website")
        if not website:
            continue
        
        logger.info(f"Processando website: {park['name']} ({website})")
        stats["processed"] += 1
        
        try:
            result: ScrapeResult = scraper.scrape_website(website, park["name"])
            
            if result.success and result.contacts:
                stats["contacts_found"] += len(result.contacts)
                logger.info(f"  Encontrados {len(result.contacts)} contatos")
                saved = save_contacts_to_db(
                    session,
                    result.contacts,
                    park_id=park["id"],
                    dry_run=dry_run,
                )
                stats["contacts_saved"] += saved
            elif result.success:
                logger.info("  Scraping OK mas nenhum contato encontrado")
            else:
                logger.warning(f"  Scraping falhou: {result.error_message}")
                
        except Exception as e:
            logger.error(f"  Erro ao processar {website}: {e}")
            stats["errors"] += 1
    
    return stats


def process_companies_api(
    session,
    hunter_service: Optional[HunterIoService],
    companies: list,
    dry_run: bool = False,
) -> dict:
    """
    Processa empresas para buscar contatos via APIs.
    
    Returns:
        Estatísticas do processamento
    """
    stats = {
        "processed": 0,
        "contacts_found": 0,
        "contacts_saved": 0,
        "errors": 0,
        "skipped_no_api": 0,
    }
    
    if not hunter_service or not hunter_service.is_configured:
        logger.warning("Hunter.io não configurado, pulando enriquecimento via API")
        stats["skipped_no_api"] = len(companies)
        return stats
    
    for company in companies:
        legal_name = company.get("legal_name", "")
        logger.info(f"Buscando contatos API para: {legal_name}")
        stats["processed"] += 1
        
        try:
            # Tenta extrair domínio do nome da empresa
            # Isso é uma simplificação - idealmente teríamos o domínio real
            domain = _guess_domain_from_name(legal_name)
            
            if domain:
                contacts = hunter_service.enrich(domain=domain, company=legal_name)
                stats["contacts_found"] += len(contacts)
                
                if contacts:
                    logger.info(f"  Encontrados {len(contacts)} contatos")
                    saved = save_contacts_to_db(
                        session,
                        contacts,
                        company_id=company["id"],
                        dry_run=dry_run,
                    )
                    stats["contacts_saved"] += saved
                else:
                    logger.info("  Nenhum contato encontrado")
            else:
                logger.info("  Não foi possível determinar domínio")
                
        except Exception as e:
            logger.error(f"  Erro ao buscar contatos: {e}")
            stats["errors"] += 1
    
    return stats


def _guess_domain_from_name(company_name: str) -> Optional[str]:
    """
    Tenta adivinhar o domínio de uma empresa pelo nome.
    
    Esta é uma função simplificada. Em produção, seria melhor
    usar uma API de busca de domínios ou ter essa informação
    pré-cadastrada.
    """
    if not company_name:
        return None
    
    # Remove sufixos comuns
    name = company_name.lower()
    for suffix in ["llc", "inc", "corp", "co", "ltd", "llp", "lp"]:
        name = name.replace(f" {suffix}", "")
        name = name.replace(f", {suffix}", "")
        name = name.replace(f",{suffix}", "")
    
    # Remove caracteres especiais
    import re
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = name.strip()
    
    # Se o nome é muito curto ou genérico, não tenta
    if len(name) < 3:
        return None
    
    # Transforma em possível domínio
    domain = name.replace(" ", "")
    return f"{domain}.com"


def print_stats(stats: dict, title: str):
    """Imprime estatísticas de forma formatada."""
    logger.info(f"\n{'='*50}")
    logger.info(f"  {title}")
    logger.info(f"{'='*50}")
    for key, value in stats.items():
        logger.info(f"  {key.replace('_', ' ').title()}: {value}")
    logger.info(f"{'='*50}\n")


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Enriquecimento de contatos digitais (Fase 5)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Processar apenas N registros",
    )
    parser.add_argument(
        "--source",
        choices=["website", "api", "all"],
        default="all",
        help="Fonte a usar para enriquecimento",
    )
    parser.add_argument(
        "--skip-websites",
        action="store_true",
        help="Pular scraping de websites",
    )
    parser.add_argument(
        "--skip-apis",
        action="store_true",
        help="Pular enriquecimento via APIs",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Reprocessar registros já processados",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas simular, não salvar no banco",
    )
    
    args = parser.parse_args()
    
    # Setup
    setup_logging()
    logger.info("="*60)
    logger.info("  FASE 5: ENRIQUECIMENTO DE CONTATOS DIGITAIS")
    logger.info("="*60)
    
    if args.dry_run:
        logger.warning("MODO DRY-RUN: Nenhuma alteração será salva no banco")
    
    # Conecta ao banco
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Verifica se a tabela contacts existe
        try:
            session.execute(text("SELECT 1 FROM contacts LIMIT 1"))
        except Exception:
            logger.error("Tabela 'contacts' não existe!")
            logger.error("Execute: python scripts/run_migration.py 003_contacts.sql")
            return 1
        
        total_stats = {
            "parks_processed": 0,
            "companies_processed": 0,
            "total_contacts_found": 0,
            "total_contacts_saved": 0,
            "total_errors": 0,
        }
        
        # 1. Scraping de websites de parques
        if args.source in ["website", "all"] and not args.skip_websites:
            logger.info("\n📍 Fase 5.1: Scraping de Websites de Parques")
            logger.info("-"*40)
            
            parks = get_parks_for_contact_enrichment(
                session, 
                limit=args.limit,
                reprocess=args.reprocess,
            )
            logger.info(f"Encontrados {len(parks)} parques com website para processar")
            
            if parks:
                scraper = WebsiteContactScraper()
                website_stats = process_parks_websites(
                    session, scraper, parks, dry_run=args.dry_run
                )
                print_stats(website_stats, "Estatísticas - Websites")
                
                total_stats["parks_processed"] = website_stats["processed"]
                total_stats["total_contacts_found"] += website_stats["contacts_found"]
                total_stats["total_contacts_saved"] += website_stats["contacts_saved"]
                total_stats["total_errors"] += website_stats["errors"]
        
        # 2. Enriquecimento via APIs
        if args.source in ["api", "all"] and not args.skip_apis:
            logger.info("\n📍 Fase 5.2: Enriquecimento via APIs")
            logger.info("-"*40)
            
            companies = get_companies_for_contact_enrichment(
                session,
                limit=args.limit,
                reprocess=args.reprocess,
            )
            logger.info(f"Encontradas {len(companies)} empresas para processar")
            
            if companies:
                hunter_service = HunterIoService()
                api_stats = process_companies_api(
                    session, hunter_service, companies, dry_run=args.dry_run
                )
                print_stats(api_stats, "Estatísticas - APIs")
                
                total_stats["companies_processed"] = api_stats["processed"]
                total_stats["total_contacts_found"] += api_stats["contacts_found"]
                total_stats["total_contacts_saved"] += api_stats["contacts_saved"]
                total_stats["total_errors"] += api_stats["errors"]
        
        # Resumo final
        print_stats(total_stats, "RESUMO FINAL - FASE 5")
        
        # Consulta total de contatos no banco
        result = session.execute(text("SELECT COUNT(*) FROM contacts"))
        total_contacts = result.scalar()
        logger.info(f"Total de contatos no banco: {total_contacts}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erro durante execução: {e}")
        return 1
        
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
