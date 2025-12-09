"""
Export Module - Controle de Qualidade e Exportação para CSV.

Este módulo consolida todas as tabelas do sistema em um arquivo
plano otimizado para campanhas de mala direta.
"""

from .export_manager import ExportManager, LeadTier

__all__ = ['ExportManager', 'LeadTier']
