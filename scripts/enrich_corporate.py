"""
Script de Execução - Fase 4: Enriquecimento Corporativo

Este script busca informações de entidades corporativas (LLCs, Corps, etc.)
no Indiana Secretary of State (SOS) e enriquece a tabela companies.

Uso:
    python scripts/enrich_corporate.py [--limit N] [--mock] [--test]

Opções:
    --limit N   Processar apenas N owners
    --mock      Usar mock searcher (não acessa site real)
    --test      Executar apenas testes de detecção

Autor: BellaTerra Intelligence
Data: 2025-12
"""

import sys
import argparse
from pathlib import Path
from loguru import logger

# Adicionar projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.enrichment.corporate_registry import (
    CorporateEnricher,
    IndianaSOSSearcher,
    is_corporate_entity,
    extract_entity_type,
)
from src.database import test_connection, get_engine


def test_entity_detection() -> bool:
    """Testa detecção de entidades corporativas."""
    test_cases = [
        ("SMITH MOBILE HOME PARK LLC", True),
        ("ABC HOLDINGS INC", True),
        ("JOHN SMITH", False),
        ("MARY JONES TRUST", True),
        ("GOLDEN PROPERTIES LLC", True),
        ("J & M INVESTMENTS", True),
        ("ESTATE OF JOHN DOE", False),
        ("ROBERT JOHNSON JR", False),
        ("MIDWEST MANUFACTURED HOUSING CORP", True),
        ("SUNSHINE RV PARK LP", True),
        ("THOMAS FAMILY TRUST", True),
        ("WILLIAMS ENTERPRISES LLC", True),
        ("BOB MILLER", False),
    ]
    
    print("\n" + "="*60)
    print("TESTE DE DETECÇÃO DE ENTIDADES")
    print("="*60)
    
    passed = 0
    for name, expected in test_cases:
        result = is_corporate_entity(name)
        entity_type = extract_entity_type(name)
        status = "PASS" if result == expected else "FAIL"
        if result == expected:
            passed += 1
        print(f"  {status}: {name:40} -> {result} (tipo: {entity_type or 'N/A'})")
    
    print(f"\nResultado: {passed}/{len(test_cases)} passaram")
    return passed == len(test_cases)


def setup_logging():
    """Configura logging."""
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    
    logger.add(
        log_dir / "corporate_enrichment_{time}.log",
        rotation="1 day",
        retention="30 days",
        level="DEBUG"
    )


def run_migration():
    """Executa a migração SQL."""
    migration_file = project_root / "migrations" / "002_corporate_registry.sql"
    
    if not migration_file.exists():
        logger.error(f"Arquivo de migração não encontrado: {migration_file}")
        return False
    
    logger.info("Executando migração SQL...")
    
    try:
        from sqlalchemy import text
        engine = get_engine()
        
        with engine.connect() as conn:
            with open(migration_file, 'r', encoding='utf-8') as f:
                sql = f.read()
            
            # Executar cada statement separadamente
            statements = sql.split(';')
            for stmt in statements:
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    try:
                        conn.execute(text(stmt))
                    except Exception as e:
                        # Ignorar erros de "já existe"
                        if 'already exists' not in str(e).lower():
                            logger.warning(f"Erro no statement: {e}")
            
            conn.commit()
        
        logger.info("Migração concluída!")
        return True
        
    except Exception as e:
        logger.error(f"Erro na migração: {e}")
        return False


def check_prerequisites():
    """Verifica pré-requisitos."""
    print("\n" + "="*70)
    print("VERIFICAÇÃO DE PRÉ-REQUISITOS")
    print("="*70)
    
    # 1. Conexão com banco
    print("\n1. Testando conexão com banco de dados...")
    if not test_connection():
        print("   ❌ Falha na conexão. Verifique .env")
        return False
    print("   ✅ Conexão OK")
    
    # 2. Verificar se há owners para processar
    print("\n2. Verificando owners na base...")
    try:
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM owners")).fetchone()[0]
            print(f"   Total owners: {total}")
            
            if total == 0:
                print("   ⚠️ Nenhum owner encontrado. Execute a Fase 3 primeiro.")
                return False
            
            # Verificar coluna sos_lookup_status
            try:
                pending = conn.execute(text("""
                    SELECT COUNT(*) FROM owners 
                    WHERE sos_lookup_status = 'pending' OR sos_lookup_status IS NULL
                """)).fetchone()[0]
                print(f"   Pendentes de SOS lookup: {pending}")
            except Exception:
                print("   ⚠️ Coluna sos_lookup_status não existe. Executando migração...")
                if not run_migration():
                    return False
                pending = total
            
            print("   ✅ Owners OK")
            
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return False
    
    # 3. Testar detecção de entidades
    print("\n3. Testando detecção de entidades corporativas...")
    if test_entity_detection():
        print("   ✅ Detecção OK")
    else:
        print("   ⚠️ Alguns testes falharam (pode continuar)")
    
    return True


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description="Fase 4: Enriquecimento Corporativo"
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=None,
        help='Número máximo de owners a processar'
    )
    parser.add_argument(
        '--mock',
        action='store_true',
        help='Usar mock searcher (não acessa site real)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Executar apenas testes'
    )
    parser.add_argument(
        '--migrate',
        action='store_true',
        help='Executar apenas migração SQL'
    )
    
    args = parser.parse_args()
    
    setup_logging()
    
    print("\n" + "="*70)
    print("FASE 4: ENRIQUECIMENTO CORPORATIVO")
    print("Identificação de pessoas reais por trás de entidades jurídicas")
    print("="*70)
    
    # Apenas testes
    if args.test:
        print("\nExecutando testes de detecção...")
        success = test_entity_detection()
        return 0 if success else 1
    
    # Apenas migração
    if args.migrate:
        print("\nExecutando migração...")
        success = run_migration()
        return 0 if success else 1
    
    # Verificar pré-requisitos
    if not check_prerequisites():
        return 1
    
    # Criar searcher com o modo apropriado
    if args.mock:
        print("\n⚠️ MODO MOCK - Usando dados simulados")
        searcher = IndianaSOSSearcher(
            min_delay=0.1,
            max_delay=0.3,
            mock_mode=True
        )
    else:
        print("\n🌐 MODO PRODUÇÃO - Acessando Indiana SOS")
        searcher = IndianaSOSSearcher(
            min_delay=2.0,
            max_delay=5.0,
            max_retries=3,
            timeout=30,
            mock_mode=False
        )
    
    # Confirmar execução
    print("\n" + "-"*70)
    if args.limit:
        print(f"Será processado um limite de {args.limit} owners.")
    else:
        print("Serão processados TODOS os owners pendentes.")
    
    print("\nEste processo pode demorar dependendo do número de registros.")
    if not args.mock:
        print("O site do Indiana SOS pode bloquear IPs com muitas requisições.")
    
    response = input("\nDeseja continuar? [s/N]: ").strip().lower()
    if response != 's':
        print("Operação cancelada.")
        return 0
    
    # Executar enriquecimento
    print("\n" + "="*70)
    print("INICIANDO PROCESSAMENTO...")
    print("="*70)
    
    enricher = CorporateEnricher(
        sos_searcher=searcher,
        batch_size=10
    )
    
    try:
        stats = enricher.process_pending_owners(limit=args.limit)
        
        print("\n" + "="*70)
        print("PROCESSAMENTO CONCLUÍDO!")
        print("="*70)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrompido pelo usuário")
        return 1
    except Exception as e:
        logger.exception("Erro fatal")
        print(f"\n❌ Erro: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
