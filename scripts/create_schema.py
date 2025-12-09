"""
Script para criar o schema do banco de dados.
Executa o arquivo schema.sql no PostgreSQL/PostGIS.
"""
import sys
from pathlib import Path
from loguru import logger

# Adicionar src ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from database import get_db_session, test_connection
from sqlalchemy import text


def create_schema():
    """Cria o schema do banco de dados executando schema.sql."""
    
    logger.info("="*60)
    logger.info("CRIANDO SCHEMA DO BANCO DE DADOS")
    logger.info("="*60)
    
    # Testar conexão básica
    logger.info("\n1. Testando conexão com o banco de dados...")
    try:
        with get_db_session() as session:
            result = session.execute(text("SELECT version()"))
            version = result.scalar()
            logger.info(f"Conexão bem-sucedida: {version}")
    except Exception as e:
        logger.error(f"Falha na conexão: {e}")
        return False
    
    # Ativar PostGIS
    logger.info("\n2. Ativando extensão PostGIS...")
    try:
        with get_db_session() as session:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            session.commit()
            logger.success("✓ Extensão PostGIS ativada!")
    except Exception as e:
        logger.warning(f"Aviso ao ativar PostGIS: {e}")
        logger.info("Continuando mesmo assim...")
    
    # Ler arquivo SQL
    schema_file = project_root / "src" / "schema.sql"
    
    if not schema_file.exists():
        logger.error(f"Arquivo schema.sql não encontrado: {schema_file}")
        return False
    
    logger.info(f"\n3. Lendo schema SQL de: {schema_file}")
    
    with open(schema_file, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    # Executar SQL
    logger.info("\n4. Executando comandos SQL...")
    
    try:
        with get_db_session() as session:
            # Executar o schema completo
            session.execute(text(schema_sql))
            session.commit()
            
            logger.success("✓ Schema criado com sucesso!")
            
            # Verificar tabelas criadas
            result = session.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """))
            
            tables = [row[0] for row in result]
            
            logger.info("\n5. Tabelas criadas:")
            for table in tables:
                logger.info(f"   ✓ {table}")
            
            # Verificar extensão PostGIS
            result = session.execute(text("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname = 'postgis'
            """))
            
            postgis = result.fetchone()
            if postgis:
                logger.info(f"\n6. Extensão PostGIS: v{postgis[1]} ✓")
            else:
                logger.warning("\n6. Extensão PostGIS não encontrada!")
            
            logger.info("\n" + "="*60)
            logger.success("SCHEMA CRIADO COM SUCESSO!")
            logger.info("="*60)
            
            return True
            
    except Exception as e:
        logger.error(f"\n✗ Erro ao criar schema: {e}")
        logger.error("Detalhes do erro:", exc_info=True)
        return False


def main():
    """Execução principal."""
    
    # Configurar logging
    logger.add(
        "logs/create_schema_{time}.log",
        rotation="1 day",
        level="DEBUG"
    )
    
    success = create_schema()
    
    if success:
        logger.info("\nPróximo passo: Execute 'python scripts/populate_parks_raw.py'")
    else:
        logger.error("\nCorreja os erros acima e tente novamente")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
