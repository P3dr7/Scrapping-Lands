#!/usr/bin/env python3
"""
Script de Exportação Final - Gera CSV pronto para Mala Direta.

Este script consolida todas as fases do pipeline e exporta um arquivo
CSV formatado para impressão de etiquetas e cartas.

Uso:
    python scripts/run_export.py [OPTIONS]

Options:
    --output-dir DIR    Diretório de saída (default: output)
    --no-qa             Desabilitar filtros de qualidade
    --min-tier TIER     Tier mínimo a incluir (A, B, ou C)
    --separate-tiers    Gerar arquivos separados por tier
    --quality-report    Exibir relatório de qualidade dos dados
"""

import argparse
import os
import sys
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger

from src.database import get_engine
from src.export.export_manager import ExportManager, LeadTier


def setup_logging():
    """Configura logging para o script."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO",
    )
    logger.add(
        "logs/export_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="DEBUG",
        rotation="1 day",
    )


def print_banner():
    """Imprime banner do sistema."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   🏠 BELATERRA INTELLIGENCE - EXPORTAÇÃO FINAL               ║
║   Sistema de Mapeamento de MHP/RV Parks - Indiana            ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Exportação final de leads para mala direta"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Diretório de saída para os arquivos CSV",
    )
    parser.add_argument(
        "--no-qa",
        action="store_true",
        help="Desabilitar filtros de controle de qualidade",
    )
    parser.add_argument(
        "--min-tier",
        type=str,
        choices=['A', 'B', 'C'],
        default=None,
        help="Tier mínimo a incluir no export (A, B, ou C)",
    )
    parser.add_argument(
        "--separate-tiers",
        action="store_true",
        help="Gerar arquivos separados por tier",
    )
    parser.add_argument(
        "--quality-report",
        action="store_true",
        help="Exibir relatório detalhado de qualidade dos dados",
    )
    
    args = parser.parse_args()
    
    # Setup
    setup_logging()
    print_banner()
    
    # Conecta ao banco
    logger.info("Conectando ao banco de dados...")
    engine = get_engine()
    
    # Inicializa o ExportManager
    manager = ExportManager(engine)
    
    try:
        # Relatório de qualidade (opcional)
        if args.quality_report:
            logger.info("Gerando relatório de qualidade...")
            report = manager.get_quality_report()
            print("\n📊 RELATÓRIO DE QUALIDADE DOS DADOS")
            print("="*50)
            print(report.to_string(index=False))
            print("="*50 + "\n")
        
        # Exportação
        if args.separate_tiers:
            logger.info("Exportando leads separados por tier...")
            files = manager.export_by_tier(output_dir=args.output_dir)
            
            print("\n✅ Arquivos gerados:")
            for tier, filepath in files.items():
                print(f"   Tier {tier}: {filepath}")
        else:
            filepath, stats = manager.export_leads(
                output_dir=args.output_dir,
                apply_qa=not args.no_qa,
                min_tier=args.min_tier,
            )
        
        # Mensagem final
        print("\n" + "="*60)
        print("  ✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*60)
        print(f"\n  Próximos passos:")
        print(f"  1. Abra o arquivo CSV no Excel")
        print(f"  2. Configure a mala direta")
        print(f"  3. Priorize os leads Tier A para contato telefônico")
        print(f"  4. Use Tier B para envio de cartas")
        print(f"  5. Tier C é fallback para endereço do parque")
        print()
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erro durante exportação: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
