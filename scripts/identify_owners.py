"""
Script de Execução - Fase 3: Identificação de Proprietários
=============================================================

Este script coordena a busca de proprietários legais através dos registros
fiscais dos condados (County Assessor Records).

PREPARAÇÃO:
-----------
1. Certificar que parks_master está populado (Fase 2 concluída)
2. Baixar GeoJSON de condados de Indiana
3. Configurar credenciais (se usar APIs pagas)

MODOS DE EXECUÇÃO:
------------------
1. MOCK (desenvolvimento/testes):
   - Usa dados fictícios
   - Não consome APIs
   - Rápido para validar fluxo
   
2. PRODUÇÃO (cuidado!):
   - Acessa County Assessor systems
   - Consome quota de APIs
   - Rate limiting agressivo

PROTEÇÕES:
----------
- Delays entre requests (3-5s)
- Checkpoints a cada 10 parques
- Logs detalhados
- Retry com backoff exponencial
- Statísticas de sucesso/falha

MODO NÃO-INTERATIVO:
--------------------
python identify_owners.py --auto --mock --limit 100

Author: BellaTerra Intelligence Team
Date: December 2025
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

from loguru import logger
from sqlalchemy import text

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_db_session, test_connection
from src.owners.orchestrator import OwnerLookupOrchestrator
from src.owners.county_mapper import CountyMapper, download_indiana_counties_geojson, create_mock_geojson


def setup_logging():
    """Configura logging para console e arquivo."""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"owner_lookup_script_{timestamp}.log"
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    logger.add(
        log_file,
        level="DEBUG",
        rotation="100 MB",
        retention="30 days"
    )
    
    return log_file


def check_prerequisites():
    """
    Verifica pré-requisitos antes de iniciar.
    
    Returns:
        bool: True se tudo OK, False se falta algo
    """
    logger.info("🔍 Verificando pré-requisitos...")
    
    issues = []
    
    # 1. Conexão com banco
    logger.info("  1. Testando conexão com banco de dados...")
    if not test_connection():
        issues.append("❌ Falha na conexão com banco de dados")
    else:
        logger.info("     ✅ Conexão OK")
    
    # 2. Tabela parks_master populada
    logger.info("  2. Verificando parks_master...")
    try:
        with get_db_session() as session:
            result = session.execute(text("SELECT COUNT(*) FROM parks_master")).fetchone()
            count = result[0]
            
            if count == 0:
                issues.append("❌ Tabela parks_master está vazia. Execute a Fase 2 primeiro!")
            else:
                logger.info(f"     ✅ {count} parques encontrados")
                
                # Verificar quantos já têm proprietário
                result_with_owner = session.execute(
                    text("SELECT COUNT(*) FROM parks_master WHERE owner_id IS NOT NULL")
                ).fetchone()
                with_owner = result_with_owner[0]
                
                logger.info(f"     📊 {with_owner} já têm proprietário identificado")
                logger.info(f"     📊 {count - with_owner} precisam ser processados")
    
    except Exception as e:
        issues.append(f"❌ Erro ao verificar parks_master: {e}")
    
    # 3. GeoJSON de condados
    logger.info("  3. Verificando GeoJSON de condados...")
    project_root = Path(__file__).parent.parent
    geojson_path = project_root / "data" / "geo" / "indiana_counties.geojson"
    
    if not geojson_path.exists():
        logger.warning(f"     ⚠️ GeoJSON não encontrado: {geojson_path}")
        logger.info("     💡 Será criado um MOCK para testes")
    else:
        logger.info(f"     ✅ GeoJSON encontrado: {geojson_path}")
    
    # 4. Variáveis de ambiente (opcional para Google Search)
    logger.info("  4. Verificando configuração de APIs...")
    
    google_api_key = os.getenv('GOOGLE_CUSTOM_SEARCH_API_KEY')
    google_search_id = os.getenv('GOOGLE_CUSTOM_SEARCH_ENGINE_ID')
    
    if not google_api_key or not google_search_id:
        logger.warning("     ⚠️ Google Custom Search API não configurado")
        logger.info("        Será usado MockFetcher para desenvolvimento")
        logger.info("        Para configurar: adicione em .env:")
        logger.info("          GOOGLE_CUSTOM_SEARCH_API_KEY=sua_chave")
        logger.info("          GOOGLE_CUSTOM_SEARCH_ENGINE_ID=seu_id")
    else:
        logger.info("     ✅ Google Custom Search API configurado")
    
    # Resumo
    logger.info("")
    if issues:
        logger.error("❌ PRÉ-REQUISITOS NÃO ATENDIDOS:")
        for issue in issues:
            logger.error(f"  {issue}")
        return False
    else:
        logger.info("✅ Todos os pré-requisitos atendidos!")
        return True


def download_geojson_if_needed():
    """Download do GeoJSON se não existir."""
    project_root = Path(__file__).parent.parent
    geojson_path = project_root / "data" / "geo" / "indiana_counties.geojson"
    
    if geojson_path.exists():
        logger.info(f"✅ GeoJSON já existe: {geojson_path}")
        return True
    
    logger.info("📥 GeoJSON não encontrado. Opções:")
    logger.info("  1. Download do US Census (completo e preciso)")
    logger.info("  2. Criar MOCK simplificado (apenas para testes)")
    logger.info("  3. Continuar sem GeoJSON (usará fallback geopy - lento)")
    
    choice = input("\nEscolha (1/2/3) [1]: ").strip() or "1"
    
    if choice == "1":
        try:
            logger.info("📥 Baixando GeoJSON do US Census...")
            path = download_indiana_counties_geojson()
            logger.info(f"✅ Download concluído: {path}")
            return True
        except Exception as e:
            logger.error(f"❌ Erro no download: {e}")
            logger.info("💡 Criando MOCK como fallback...")
            create_mock_geojson()
            return True
    
    elif choice == "2":
        logger.info("🧪 Criando MOCK GeoJSON...")
        create_mock_geojson()
        logger.info("✅ MOCK criado (apenas para testes!)")
        return True
    
    else:
        logger.warning("⚠️ Continuando sem GeoJSON (usará geopy como fallback)")
        return True


def get_processing_config():
    """
    Obtém configuração do usuário para processamento.
    
    Returns:
        dict: Configuração
    """
    logger.info("")
    logger.info("=" * 80)
    logger.info("CONFIGURAÇÃO DO PROCESSAMENTO")
    logger.info("=" * 80)
    
    # Modo (MOCK vs PRODUÇÃO)
    logger.info("")
    logger.info("MODO DE EXECUÇÃO:")
    logger.info("  1. MOCK (desenvolvimento) - Dados fictícios, sem consumir APIs")
    logger.info("  2. PRODUÇÃO - Acessa County Assessor systems (CUIDADO!)")
    
    mode_choice = input("\nEscolha o modo (1/2) [1]: ").strip() or "1"
    use_mock = (mode_choice == "1")
    
    if use_mock:
        logger.info("✅ Modo MOCK selecionado (desenvolvimento)")
    else:
        logger.warning("⚠️ Modo PRODUÇÃO selecionado!")
        logger.warning("   - Vai acessar County Assessor systems")
        logger.warning("   - Pode consumir quota de APIs")
        logger.warning("   - Rate limiting ativo (3-5s entre requests)")
        
        confirm = input("\nTem certeza? (sim/não) [não]: ").strip().lower()
        if confirm not in ['sim', 's', 'yes', 'y']:
            logger.info("❌ Operação cancelada pelo usuário")
            sys.exit(0)
    
    # Limite de parques
    logger.info("")
    logger.info("LIMITE DE PARQUES:")
    logger.info("  - Digite um número para processar apenas N parques (teste)")
    logger.info("  - Deixe em branco para processar TODOS")
    
    limit_input = input("\nLimite (deixe em branco para todos): ").strip()
    limit = int(limit_input) if limit_input else None
    
    if limit:
        logger.info(f"✅ Processará até {limit} parques")
    else:
        logger.info("✅ Processará TODOS os parques pendentes")
    
    # Delay entre requests
    if use_mock:
        delay = 0.5  # Rápido para MOCK
    else:
        logger.info("")
        logger.info("DELAY ENTRE REQUESTS:")
        logger.info("  Recomendado: 3-5 segundos (evita bloqueios)")
        
        delay_input = input("\nDelay em segundos [3.0]: ").strip()
        delay = float(delay_input) if delay_input else 3.0
    
    logger.info(f"✅ Delay configurado: {delay}s")
    
    return {
        'use_mock': use_mock,
        'limit': limit,
        'delay': delay
    }


def main():
    """Função principal do script."""
    # Parse argumentos CLI
    parser = argparse.ArgumentParser(description="Fase 3: Identificação de Proprietários")
    parser.add_argument('--auto', action='store_true', help='Modo não-interativo (sem prompts)')
    parser.add_argument('--mock', action='store_true', help='Usar dados MOCK (desenvolvimento)')
    parser.add_argument('--production', action='store_true', help='Modo produção (acessar County Assessors)')
    parser.add_argument('--limit', type=int, default=None, help='Limite de parques a processar')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay entre requests (segundos)')
    args = parser.parse_args()
    
    print("=" * 80)
    print("FASE 3: IDENTIFICAÇÃO DE PROPRIETÁRIOS")
    print("County Assessor Records Lookup")
    print("=" * 80)
    print()
    
    # Setup logging
    log_file = setup_logging()
    logger.info(f"📝 Log file: {log_file}")
    
    # Verificar pré-requisitos
    if not check_prerequisites():
        logger.error("\n❌ Corrija os problemas acima antes de continuar")
        sys.exit(1)
    
    logger.info("")
    
    # Modo automático ou interativo
    if args.auto:
        # Criar mock GeoJSON se não existir
        geojson_path = Path(__file__).parent.parent / "data" / "geo" / "indiana_counties.geojson"
        if not geojson_path.exists():
            logger.info("📦 Criando GeoJSON mock automaticamente...")
            create_mock_geojson()
        
        # Configuração automática
        use_mock = args.mock or (not args.production)  # Default é mock
        config = {
            'use_mock': use_mock,
            'limit': args.limit,
            'delay': args.delay if not use_mock else 0.1
        }
        logger.info(f"✅ Modo automático: {'MOCK' if use_mock else 'PRODUÇÃO'}, limit={args.limit}, delay={config['delay']}s")
    else:
        # Download GeoJSON se necessário
        download_geojson_if_needed()
        
        # Obter configuração do usuário (interativo)
        config = get_processing_config()
        
        # Confirmação final
        logger.info("")
        logger.info("=" * 80)
        logger.info("RESUMO DA CONFIGURAÇÃO")
        logger.info("=" * 80)
        logger.info(f"  Modo: {'MOCK (desenvolvimento)' if config['use_mock'] else 'PRODUÇÃO'}")
        logger.info(f"  Limite: {config['limit'] if config['limit'] else 'TODOS os parques'}")
        logger.info(f"  Delay: {config['delay']}s entre requests")
        logger.info(f"  Log: {log_file}")
        logger.info("=" * 80)
        
        input("\nPressione ENTER para iniciar ou Ctrl+C para cancelar...")
    
    # Criar orchestrator
    logger.info("")
    logger.info("🚀 Iniciando orchestrator...")
    
    orchestrator = OwnerLookupOrchestrator(
        use_mock=config['use_mock'],
        max_retries=3,
        delay_between_requests=config['delay'],
        checkpoint_interval=10
    )
    
    # Processar
    try:
        orchestrator.process_all_parks(limit=config['limit'])
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 80)
        logger.info(f"📝 Logs detalhados: {log_file}")
        logger.info("")
        logger.info("PRÓXIMOS PASSOS:")
        logger.info("  1. Revisar registros marcados como 'needs_manual_review'")
        logger.info("  2. Validar endereços para mala direta")
        logger.info("  3. Executar Fase 4: Exportação para mailing")
        logger.info("=" * 80)
    
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Processamento interrompido pelo usuário (Ctrl+C)")
        logger.info("💾 Progresso até agora foi salvo no banco de dados")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"\n❌ ERRO CRÍTICO: {e}")
        logger.exception("Stack trace completo:")
        sys.exit(1)


if __name__ == "__main__":
    main()
