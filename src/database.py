"""
Módulo de conexão com PostgreSQL/PostGIS.
Carrega credenciais de variáveis de ambiente para segurança.
"""
import os
from typing import Optional
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
from loguru import logger

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


class DatabaseConfig:
    """Configuração do banco de dados a partir de variáveis de ambiente."""
    
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME", "mhp_intelligence")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "")
        
        if not self.password:
            logger.warning("DB_PASSWORD não definida no .env")
    
    @property
    def connection_string(self) -> str:
        """Retorna a connection string do PostgreSQL."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
    
    @property
    def connection_string_async(self) -> str:
        """Retorna a connection string para conexões assíncronas."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


# Engine global (singleton pattern)
_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


def get_engine(echo: bool = False) -> Engine:
    """
    Retorna a engine SQLAlchemy (singleton).
    
    Args:
        echo: Se True, loga todas as queries SQL
        
    Returns:
        Engine do SQLAlchemy
    """
    global _engine
    
    if _engine is None:
        config = DatabaseConfig()
        _engine = create_engine(
            config.connection_string,
            echo=echo,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,  # Verifica conexões antes de usar
        )
        logger.info(f"Engine criada: {config.host}:{config.port}/{config.database}")
    
    return _engine


def get_session_maker() -> sessionmaker:
    """
    Retorna o sessionmaker configurado.
    
    Returns:
        sessionmaker do SQLAlchemy
    """
    global _SessionLocal
    
    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
        )
    
    return _SessionLocal


@contextmanager
def get_db_session():
    """
    Context manager para sessões do banco de dados.
    
    Uso:
        with get_db_session() as session:
            result = session.execute(text("SELECT 1"))
    """
    SessionLocal = get_session_maker()
    session: Session = SessionLocal()
    
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Erro na sessão do banco: {e}")
        raise
    finally:
        session.close()


def test_connection() -> bool:
    """
    Testa a conexão com o banco de dados.
    
    Returns:
        True se conectou com sucesso, False caso contrário
    """
    try:
        with get_db_session() as session:
            result = session.execute(text("SELECT version()"))
            version = result.scalar()
            logger.info(f"Conexão bem-sucedida: {version}")
            
            # Verifica se PostGIS está instalado
            result = session.execute(text("SELECT PostGIS_version()"))
            postgis_version = result.scalar()
            logger.info(f"PostGIS version: {postgis_version}")
            
            return True
    except Exception as e:
        logger.error(f"Falha ao conectar ao banco: {e}")
        return False


def initialize_postgis():
    """
    Inicializa a extensão PostGIS no banco de dados.
    Execute este comando uma vez após criar o banco.
    """
    try:
        with get_db_session() as session:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            logger.info("Extensão PostGIS inicializada com sucesso")
    except Exception as e:
        logger.error(f"Erro ao inicializar PostGIS: {e}")
        raise


if __name__ == "__main__":
    # Teste de conexão
    logger.info("Testando conexão com o banco de dados...")
    if test_connection():
        logger.success("✓ Conexão estabelecida com sucesso!")
    else:
        logger.error("✗ Falha na conexão")
