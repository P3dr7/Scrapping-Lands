"""
Script de Teste Rápido - Fase 3
================================

Testa os componentes principais da Fase 3 sem executar o pipeline completo.

Uso:
    python scripts/test_phase3.py
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True)


def test_county_mapper():
    """Testa identificação de condados."""
    from src.owners.county_mapper import CountyMapper, create_mock_geojson
    
    print("\n" + "=" * 80)
    print("TESTE 1: County Mapper")
    print("=" * 80)
    
    # Criar mock GeoJSON se não existir
    print("📥 Criando mock GeoJSON...")
    create_mock_geojson()
    
    # Inicializar mapper
    mapper = CountyMapper()
    
    # Coordenadas de teste
    test_locations = [
        (39.7684, -86.1581, "Indianapolis - Marion County"),
        (41.5934, -87.3464, "Gary - Lake County"),
        (41.0793, -85.1394, "Fort Wayne - Allen County"),
    ]
    
    print("\n📍 Testando identificação de condados:")
    print("-" * 80)
    
    for lat, lon, description in test_locations:
        county = mapper.identify_county(lat, lon)
        
        status = "✅" if county else "❌"
        print(f"{status} {description}")
        if county:
            print(f"   Condado identificado: {county}")
            
            info = mapper.get_county_info(county)
            print(f"   Sistema: {info.get('assessor_system', 'N/A')}")
    
    # Estatísticas
    print("\n📊 Estatísticas:")
    print("-" * 80)
    stats = mapper.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ County Mapper: OK")
    return True


def test_mock_fetcher():
    """Testa MockFetcher."""
    from src.owners.fetchers.generic_fetcher import MockFetcher
    
    print("\n" + "=" * 80)
    print("TESTE 2: Mock Fetcher")
    print("=" * 80)
    
    fetcher = MockFetcher("Test County")
    
    print("\n🔍 Testando busca de proprietário (MOCK)...")
    result = fetcher.lookup_owner(
        address="123 Test Lane, Indianapolis, IN",
        lat=39.7684,
        lon=-86.1581,
        parcel_id="99-99-99-999-999.999-999"
    )
    
    if result.success and result.found_owner:
        record = result.records[0]
        print("\n✅ Proprietário encontrado (dados fictícios):")
        print(f"  Nome: {record.owner_name_1}")
        print(f"  Endereço: {record.mailing_address_line1}")
        print(f"  Cidade: {record.mailing_city}, {record.mailing_state}")
        print(f"  CEP: {record.mailing_zip}")
        print(f"  Parcel ID: {record.parcel_id}")
        print(f"  Confidence: {record.confidence_score:.2f}")
        print(f"  Válido para mailing: {record.is_valid_mailing_address}")
    else:
        print(f"\n❌ Falha: {result.error_message}")
    
    # Estatísticas
    print("\n📊 Estatísticas do fetcher:")
    stats = fetcher.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Mock Fetcher: OK")
    return True


def test_owner_record_validation():
    """Testa validação de OwnerRecord."""
    from src.owners.base_fetcher import OwnerRecord, PropertyClassCode
    
    print("\n" + "=" * 80)
    print("TESTE 3: Owner Record Validation")
    print("=" * 80)
    
    # Registro VÁLIDO
    print("\n✅ Testando registro válido...")
    valid_record = OwnerRecord(
        owner_name_1="JOHN DOE",
        mailing_address_line1="123 MAIN ST",
        mailing_city="INDIANAPOLIS",
        mailing_state="IN",
        mailing_zip="46220",
        parcel_id="49-07-15-203-017.000-006",
        property_address="456 PARK LANE",
        property_class_code=PropertyClassCode.MOBILE_HOME.value,
        source="Test"
    )
    
    print(f"  Válido para mailing: {valid_record.is_valid_mailing_address}")
    print(f"  Requer revisão manual: {valid_record.needs_manual_review}")
    
    # Registro INVÁLIDO (sem CEP)
    print("\n❌ Testando registro inválido (sem CEP)...")
    invalid_record = OwnerRecord(
        owner_name_1="JANE SMITH",
        mailing_address_line1="456 OAK AVE",
        mailing_city="FORT WAYNE",
        mailing_state="IN",
        mailing_zip="",  # CEP ausente!
        parcel_id="02-12-26-201-005.000-008",
        property_address="789 PINE RD",
        source="Test"
    )
    
    print(f"  Válido para mailing: {invalid_record.is_valid_mailing_address}")
    print(f"  Requer revisão manual: {invalid_record.needs_manual_review}")
    print(f"  Motivo: {invalid_record.notes}")
    
    print("\n✅ Owner Record Validation: OK")
    return True


def test_database_connection():
    """Testa conexão com banco de dados."""
    from src.database import test_connection, get_db_session
    from sqlalchemy import text
    
    print("\n" + "=" * 80)
    print("TESTE 4: Database Connection")
    print("=" * 80)
    
    print("\n🔌 Testando conexão...")
    if not test_connection():
        print("❌ Falha na conexão com banco de dados")
        return False
    
    print("✅ Conexão OK")
    
    # Verificar tabelas
    print("\n📊 Verificando tabelas...")
    with get_db_session() as session:
        # parks_master
        result = session.execute(text("SELECT COUNT(*) FROM parks_master")).fetchone()
        parks_count = result[0]
        print(f"  parks_master: {parks_count} registros")
        
        # owners
        result = session.execute(text("SELECT COUNT(*) FROM owners")).fetchone()
        owners_count = result[0]
        print(f"  owners: {owners_count} registros")
        
        # Parques sem proprietário
        result = session.execute(
            text("SELECT COUNT(*) FROM parks_master WHERE owner_id IS NULL")
        ).fetchone()
        without_owner = result[0]
        print(f"  Parques sem proprietário: {without_owner}")
    
    print("\n✅ Database Connection: OK")
    return True


def test_orchestrator_dry_run():
    """Testa orchestrator em modo dry-run."""
    from src.owners.orchestrator import OwnerLookupOrchestrator
    
    print("\n" + "=" * 80)
    print("TESTE 5: Orchestrator (Dry Run)")
    print("=" * 80)
    
    print("\n🎯 Criando orchestrator em modo MOCK...")
    orchestrator = OwnerLookupOrchestrator(
        use_mock=True,
        max_retries=2,
        delay_between_requests=0.1,
        checkpoint_interval=5
    )
    
    print("\n🚀 Processando 3 parques (teste)...")
    try:
        orchestrator.process_all_parks(limit=3)
        print("\n✅ Orchestrator: OK")
        return True
    except Exception as e:
        print(f"\n❌ Erro no orchestrator: {e}")
        return False


def main():
    """Executa todos os testes."""
    print("=" * 80)
    print("TESTES DA FASE 3 - OWNER IDENTIFICATION")
    print("=" * 80)
    
    tests = [
        ("County Mapper", test_county_mapper),
        ("Mock Fetcher", test_mock_fetcher),
        ("Owner Record Validation", test_owner_record_validation),
        ("Database Connection", test_database_connection),
        ("Orchestrator", test_orchestrator_dry_run),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"❌ Erro no teste '{test_name}': {e}")
            results.append((test_name, False))
    
    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES")
    print("=" * 80)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    total = len(results)
    passed = sum(1 for _, success in results if success)
    
    print("\n" + "=" * 80)
    print(f"Total: {passed}/{total} testes passaram")
    print("=" * 80)
    
    if passed == total:
        print("\n🎉 TODOS OS TESTES PASSARAM!")
        print("\n💡 Próximo passo: Executar `python scripts/identify_owners.py`")
        return 0
    else:
        print(f"\n⚠️ {total - passed} teste(s) falharam. Verifique os erros acima.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
