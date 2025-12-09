"""
Script para processar dados de parks_raw e criar registros consolidados em parks_master.
Executa deduplicação e consolidação de dados.
"""
import sys
from pathlib import Path
from loguru import logger

# Adicionar projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.processing.deduplication import process_parks_raw_to_master
from src.database import test_connection


def main():
    """Execução principal."""
    
    # Configurar logging
    logger.add(
        "logs/process_master_{time}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    logger.info("="*70)
    logger.info("SCRIPT DE PROCESSAMENTO: parks_raw → parks_master")
    logger.info("="*70)
    
    # Testar conexão
    logger.info("\n1. Testando conexão com o banco de dados...")
    if not test_connection():
        logger.error("Falha na conexão. Verifique o arquivo .env")
        return False
    
    logger.info("✓ Conexão estabelecida")
    
    # Confirmar execução
    print("\n" + "="*70)
    print("PROCESSAMENTO DE DEDUPLICAÇÃO E CONSOLIDAÇÃO")
    print("="*70)
    print("\nEste script irá:")
    print("  1. Carregar todos os registros não processados de parks_raw")
    print("  2. Normalizar endereços usando algoritmo de parsing")
    print("  3. Agrupar registros por ZIP code e proximidade geográfica")
    print("  4. Detectar duplicatas usando similaridade de nomes (>85%)")
    print("  5. Consolidar dados de múltiplas fontes em registros master")
    print("  6. Inserir registros únicos em parks_master")
    print("\nAlgoritmo de deduplicação:")
    print("  - Blocking por ZIP code + raio de 500m")
    print("  - Similaridade de nome: fuzzy matching (RapidFuzz)")
    print("  - Prioridade de fontes: Google Places > OSM > Yelp")
    print("  - Consolidação: melhor valor de cada fonte")
    
    print("\n" + "="*70)
    
    confirm = input("\nDeseja continuar? (s/N): ").strip().lower()
    
    if confirm != 's':
        logger.info("Processamento cancelado pelo usuário")
        print("\nProcessamento cancelado.")
        return False
    
    # Executar processamento
    logger.info("\n2. Iniciando processamento...")
    
    try:
        master_records = process_parks_raw_to_master()
        
        if master_records:
            logger.success(f"\n✓ Processamento concluído com sucesso!")
            logger.info(f"  {len(master_records)} registros master criados")
            
            # Estatísticas finais
            from database import get_db_session
            from sqlalchemy import text
            
            with get_db_session() as session:
                # Total de parks_master
                result = session.execute(text("SELECT COUNT(*) FROM parks_master"))
                total_master = result.scalar()
                
                # Total de parks_raw
                result = session.execute(text("SELECT COUNT(*) FROM parks_raw"))
                total_raw = result.scalar()
                
                # Registros processados
                result = session.execute(text(
                    "SELECT COUNT(*) FROM parks_raw WHERE is_processed = TRUE"
                ))
                processed = result.scalar()
                
                # Registros que precisam revisão
                result = session.execute(text(
                    "SELECT COUNT(*) FROM parks_master WHERE needs_manual_review = TRUE"
                ))
                needs_review = result.scalar()
            
            print("\n" + "="*70)
            print("ESTATÍSTICAS FINAIS")
            print("="*70)
            print(f"Total de registros em parks_raw: {total_raw}")
            print(f"Registros processados: {processed}")
            print(f"Total de registros em parks_master: {total_master}")
            print(f"Taxa de deduplicação: {(1 - total_master/max(total_raw, 1)):.1%}")
            print(f"Registros que precisam revisão manual: {needs_review}")
            print("="*70)
            
            logger.info("\nPróximos passos:")
            logger.info("  1. Revisar registros marcados com needs_manual_review = TRUE")
            logger.info("  2. Iniciar processo de identificação de proprietários")
            logger.info("  3. Popular tabelas owners e companies")
            
            return True
        else:
            logger.warning("Nenhum registro foi processado")
            return False
            
    except Exception as e:
        logger.error(f"\n✗ Erro durante processamento: {e}")
        logger.error("Detalhes do erro:", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
