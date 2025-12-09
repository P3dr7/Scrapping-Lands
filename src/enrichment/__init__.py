"""
Módulo de Enriquecimento de Dados - Fase 4

Responsável por identificar pessoas reais por trás de entidades jurídicas.
"""

from .corporate_registry import (
    is_corporate_entity,
    IndianaSOSSearcher,
    CorporateEnricher,
)

__all__ = [
    'is_corporate_entity',
    'IndianaSOSSearcher', 
    'CorporateEnricher',
]
