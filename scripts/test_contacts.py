#!/usr/bin/env python3
"""
Script de teste para verificar a tabela contacts e testar o WebsiteContactScraper.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.database import get_engine
from src.enrichment.contact_finder import WebsiteContactScraper, HunterIoService, ExtractedContact
from loguru import logger


def test_table_structure():
    """Verifica se a tabela contacts foi criada corretamente."""
    print("\n" + "="*60)
    print("  TESTE 1: Estrutura da tabela contacts")
    print("="*60)
    
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'contacts' 
            ORDER BY ordinal_position
        """))
        
        columns = list(result)
        if columns:
            print("\n✅ Tabela contacts existe!")
            print("\nColunas:")
            for row in columns:
                print(f"  - {row[0]}: {row[1]}")
            return True
        else:
            print("\n❌ Tabela contacts NÃO existe!")
            return False


def test_scrape_log_table():
    """Verifica se a tabela contact_scrape_log foi criada."""
    print("\n" + "="*60)
    print("  TESTE 2: Tabela contact_scrape_log")
    print("="*60)
    
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'contact_scrape_log'
        """))
        exists = result.scalar() > 0
        
        if exists:
            print("\n✅ Tabela contact_scrape_log existe!")
            return True
        else:
            print("\n❌ Tabela contact_scrape_log NÃO existe!")
            return False


def test_website_scraper():
    """Testa o WebsiteContactScraper com um site de exemplo."""
    print("\n" + "="*60)
    print("  TESTE 3: WebsiteContactScraper")
    print("="*60)
    
    scraper = WebsiteContactScraper()
    
    # Busca um parque com website no banco
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT name, website 
            FROM parks_master 
            WHERE website IS NOT NULL 
              AND website != '' 
            LIMIT 5
        """))
        parks = list(result)
    
    if not parks:
        print("\n⚠️ Nenhum parque com website encontrado no banco")
        return False
    
    print(f"\nParques com website encontrados: {len(parks)}")
    
    # Testa com o primeiro parque
    park_name, website = parks[0]
    print(f"\nTestando scraping de: {park_name}")
    print(f"URL: {website}")
    
    try:
        result = scraper.scrape_website(website, park_name)
        
        if result.success and result.contacts:
            print(f"\n✅ Sucesso! Encontrados {len(result.contacts)} contatos:")
            for contact in result.contacts[:5]:  # Mostra até 5
                print(f"  - Email: {contact.email}")
                print(f"    Telefone: {contact.phone}")
                print(f"    Confiança: {contact.confidence}")
                print(f"    Fonte: {contact.source}")
                print()
        elif result.success:
            print(f"\n✅ Scraping bem sucedido, mas sem contatos encontrados")
            print(f"   Páginas processadas: {result.pages_scraped}")
        else:
            print(f"\n⚠️ Scraping falhou: {result.error_message}")
        
        return True
    except Exception as e:
        print(f"\n❌ Erro no scraping: {e}")
        return False


def test_hunter_service():
    """Verifica se o HunterIoService está configurado."""
    print("\n" + "="*60)
    print("  TESTE 4: HunterIoService")
    print("="*60)
    
    service = HunterIoService()
    
    if service.is_configured:
        print("\n✅ Hunter.io configurado com API key")
    else:
        print("\n⚠️ Hunter.io NÃO configurado (HUNTER_API_KEY não definida)")
        print("   Isso é esperado se você não tem uma conta Hunter.io")
    
    return True  # Não é erro não ter configurado


def test_parks_with_websites():
    """Lista parques com websites para processamento."""
    print("\n" + "="*60)
    print("  TESTE 5: Parques disponíveis para enriquecimento")
    print("="*60)
    
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM parks_master 
            WHERE website IS NOT NULL AND website != ''
        """))
        count = result.scalar()
        
        print(f"\n📊 {count} parques têm website definido")
        
        # Mostra exemplos
        result = conn.execute(text("""
            SELECT name, website, city 
            FROM parks_master 
            WHERE website IS NOT NULL AND website != ''
            LIMIT 10
        """))
        
        print("\nExemplos:")
        for row in result:
            print(f"  - {row[0]} ({row[2]})")
            print(f"    {row[1]}")
    
    return True


def main():
    """Executa todos os testes."""
    logger.remove()  # Remove logs do SQLAlchemy para output limpo
    
    print("\n" + "="*60)
    print("  TESTES DA FASE 5: CONTATOS")
    print("="*60)
    
    results = []
    results.append(("Estrutura contacts", test_table_structure()))
    results.append(("Tabela scrape_log", test_scrape_log_table()))
    results.append(("Hunter.io config", test_hunter_service()))
    results.append(("Parks com website", test_parks_with_websites()))
    results.append(("Website Scraper", test_website_scraper()))
    
    print("\n" + "="*60)
    print("  RESUMO DOS TESTES")
    print("="*60)
    
    passed = 0
    for name, result in results:
        status = "✅" if result else "❌"
        print(f"  {status} {name}")
        if result:
            passed += 1
    
    print(f"\n  Total: {passed}/{len(results)} testes passaram")
    
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
