#!/usr/bin/env python3
"""
Script de teste para buscar uma empresa no portal INBiz.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.enrichment.corporate_registry import IndianaSOSSearcher, is_corporate_entity


def test_search(mock_mode: bool = False):
    """Testa uma busca no portal INBiz."""
    
    print("\n" + "="*70)
    print("TESTE DE BUSCA NO PORTAL INBiz")
    print(f"Modo: {'MOCK (dados simulados)' if mock_mode else 'PRODUÇÃO (APIs reais)'}")
    print("="*70 + "\n")
    
    # Teste simples de entidade conhecida
    test_names = [
        "INDY MANUFACTURED HOUSING LLC",  # Nome típico de MHP
        "SUNSTONE PROPERTIES INC",        # Corporation
        "MIDWEST MHP LLC",                # Outro nome típico
        "JOHN DOE",                       # Pessoa física (não é corporativo)
    ]
    
    searcher = IndianaSOSSearcher(mock_mode=mock_mode)
    
    for name in test_names:
        print(f"\n{'='*60}")
        print(f"Buscando: {name}")
        print(f"É entidade corporativa? {is_corporate_entity(name)}")
        print("="*60)
        
        result = searcher.search_business(name)
        
        if result:
            print(f"✓ ENCONTRADO!")
            print(f"  Business ID: {result.business_id}")
            print(f"  Nome: {result.business_name}")
            print(f"  Status: {result.status}")
            print(f"  Tipo: {result.entity_type}")
            print(f"  Data Formação: {result.formation_date}")
            if result.registered_agent:
                print(f"  Registered Agent: {result.registered_agent.name}")
                print(f"  Endereço Agent: {result.registered_agent.full_address()}")
            if result.principals:
                print(f"  Principals ({len(result.principals)}):")
                for p in result.principals[:3]:  # Mostra até 3
                    print(f"    - {p.name} ({p.title})")
        else:
            print("✗ Não encontrado ou não é entidade corporativa")
    
    print("\n" + "="*70)
    print(f"Estatísticas: {searcher.stats}")
    print("Teste concluído!")
    print("="*70 + "\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Testa busca no INBiz")
    parser.add_argument("--mock", action="store_true", help="Usar modo mock (dados simulados)")
    args = parser.parse_args()
    
    test_search(mock_mode=args.mock)
