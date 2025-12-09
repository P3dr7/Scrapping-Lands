"""
Módulo de deduplicação e consolidação de dados.
Processa dados de parks_raw e gera registros limpos em parks_master.
"""
import re
import uuid
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from loguru import logger
from sqlalchemy import text
import usaddress

from ..database import get_db_session


@dataclass
class NormalizedAddress:
    """Representa um endereço normalizado."""
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    street_type: Optional[str] = None  # St, Ave, Blvd, etc
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    full_normalized: Optional[str] = None
    parse_success: bool = False


class AddressNormalizer:
    """Normaliza endereços para comparação consistente."""
    
    # Mapeamento de abreviações comuns
    STREET_TYPE_MAP = {
        'street': 'st', 'str': 'st', 'strt': 'st',
        'avenue': 'ave', 'av': 'ave', 'avn': 'ave',
        'boulevard': 'blvd', 'blv': 'blvd', 'boul': 'blvd',
        'road': 'rd', 'roa': 'rd',
        'drive': 'dr', 'drv': 'dr', 'driv': 'dr',
        'lane': 'ln', 'lan': 'ln',
        'court': 'ct', 'crt': 'ct',
        'circle': 'cir', 'circ': 'cir',
        'place': 'pl', 'plc': 'pl',
        'highway': 'hwy', 'hwy': 'hwy', 'hiway': 'hwy',
        'parkway': 'pkwy', 'pkw': 'pkwy', 'pky': 'pkwy',
        'trail': 'trl', 'trls': 'trl',
    }
    
    @staticmethod
    def normalize_street_type(street_type: str) -> str:
        """Normaliza tipo de rua (Street -> St, Avenue -> Ave, etc)."""
        if not street_type:
            return ''
        
        street_type_lower = street_type.lower().strip('.')
        return AddressNormalizer.STREET_TYPE_MAP.get(street_type_lower, street_type_lower)
    
    @staticmethod
    def clean_address_string(address: str) -> str:
        """Limpa string de endereço para processamento."""
        if not address:
            return ''
        
        # Remover caracteres especiais excessivos
        address = re.sub(r'[^\w\s,.-]', '', address)
        
        # Normalizar espaços
        address = re.sub(r'\s+', ' ', address)
        
        # Remover vírgulas múltiplas
        address = re.sub(r',+', ',', address)
        
        return address.strip()
    
    @staticmethod
    def parse_address(address_str: str) -> NormalizedAddress:
        """
        Parseia endereço usando usaddress library.
        
        Args:
            address_str: Endereço completo como string
            
        Returns:
            NormalizedAddress com componentes extraídos
        """
        if not address_str or pd.isna(address_str):
            return NormalizedAddress()
        
        # Limpar endereço
        cleaned = AddressNormalizer.clean_address_string(str(address_str))
        
        if not cleaned:
            return NormalizedAddress()
        
        try:
            # Parsear com usaddress
            parsed, address_type = usaddress.tag(cleaned)
            
            # Extrair componentes
            street_number = parsed.get('AddressNumber', '')
            
            # Montar nome da rua
            street_parts = []
            for key in ['StreetNamePreDirectional', 'StreetName', 'StreetNamePostType']:
                if key in parsed:
                    street_parts.append(parsed[key])
            
            street_name = ' '.join(street_parts) if street_parts else ''
            
            # Tipo de rua
            street_type = parsed.get('StreetNamePostType', '')
            if street_type:
                street_type = AddressNormalizer.normalize_street_type(street_type)
            
            city = parsed.get('PlaceName', '')
            state = parsed.get('StateName', '')
            zip_code = parsed.get('ZipCode', '')
            
            # Criar versão normalizada completa
            normalized_parts = []
            if street_number:
                normalized_parts.append(street_number)
            if street_name:
                normalized_parts.append(street_name.lower())
            if street_type and street_type not in street_name.lower():
                normalized_parts.append(street_type)
            
            full_normalized = ' '.join(normalized_parts)
            
            return NormalizedAddress(
                street_number=street_number or None,
                street_name=street_name or None,
                street_type=street_type or None,
                city=city or None,
                state=state or None,
                zip_code=zip_code or None,
                full_normalized=full_normalized if full_normalized else None,
                parse_success=True
            )
            
        except Exception as e:
            logger.debug(f"Falha ao parsear endereço '{address_str}': {e}")
            
            # Fallback: normalização básica
            basic_normalized = cleaned.lower()
            basic_normalized = re.sub(r'\bstreet\b', 'st', basic_normalized)
            basic_normalized = re.sub(r'\bavenue\b', 'ave', basic_normalized)
            basic_normalized = re.sub(r'\bboulevard\b', 'blvd', basic_normalized)
            basic_normalized = re.sub(r'\broad\b', 'rd', basic_normalized)
            
            return NormalizedAddress(
                full_normalized=basic_normalized,
                parse_success=False
            )


class GeographicBlocker:
    """Agrupa registros por proximidade geográfica."""
    
    @staticmethod
    def calculate_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calcula distância entre dois pontos em metros (fórmula de Haversine).
        
        Args:
            lat1, lon1: Coordenadas do primeiro ponto
            lat2, lon2: Coordenadas do segundo ponto
            
        Returns:
            Distância em metros
        """
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371000  # Raio da Terra em metros
        
        lat1_rad = radians(lat1)
        lat2_rad = radians(lat2)
        delta_lat = radians(lat2 - lat1)
        delta_lon = radians(lon2 - lon1)
        
        a = sin(delta_lat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    @staticmethod
    def block_by_zip_and_proximity(df: pd.DataFrame, proximity_meters: float = 500) -> Dict[str, List[int]]:
        """
        Agrupa registros por ZIP code e proximidade geográfica.
        
        Args:
            df: DataFrame com dados de parks_raw
            proximity_meters: Raio máximo em metros para considerar proximidade
            
        Returns:
            Dicionário {block_id: [indices]}
        """
        blocks = defaultdict(list)
        
        # Primeiro blocking: por ZIP code
        for zip_code, group in df.groupby('zip_code', dropna=False):
            if pd.notna(zip_code) and zip_code:
                block_key = f"zip_{zip_code}"
                blocks[block_key].extend(group.index.tolist())
        
        # Segundo blocking: registros sem ZIP mas com coordenadas próximas
        no_zip = df[df['zip_code'].isna() | (df['zip_code'] == '')]
        
        if len(no_zip) > 0:
            # Agrupar por proximidade geográfica
            processed = set()
            
            for idx in no_zip.index:
                if idx in processed:
                    continue
                
                row = no_zip.loc[idx]
                
                if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                    # Sem coordenadas, bloco individual
                    blocks[f"no_geo_{idx}"].append(idx)
                    processed.add(idx)
                    continue
                
                # Encontrar todos os vizinhos próximos
                block_members = [idx]
                processed.add(idx)
                
                for other_idx in no_zip.index:
                    if other_idx in processed:
                        continue
                    
                    other_row = no_zip.loc[other_idx]
                    
                    if pd.isna(other_row['latitude']) or pd.isna(other_row['longitude']):
                        continue
                    
                    distance = GeographicBlocker.calculate_distance_meters(
                        float(row['latitude']), float(row['longitude']),
                        float(other_row['latitude']), float(other_row['longitude'])
                    )
                    
                    if distance <= proximity_meters:
                        block_members.append(other_idx)
                        processed.add(other_idx)
                
                if block_members:
                    blocks[f"geo_cluster_{idx}"].extend(block_members)
        
        logger.info(f"Criados {len(blocks)} blocos para processamento")
        
        return dict(blocks)


class DuplicateDetector:
    """Detecta duplicatas usando similaridade de string e proximidade geográfica."""
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """Normaliza nome do parque para comparação."""
        if not name or pd.isna(name):
            return ''
        
        name = str(name).lower()
        
        # Remover palavras comuns
        stopwords = ['rv', 'park', 'mobile', 'home', 'trailer', 'campground', 
                     'resort', 'the', 'a', 'an', 'and', '&']
        
        words = name.split()
        words = [w for w in words if w not in stopwords]
        
        # Remover pontuação
        name = ' '.join(words)
        name = re.sub(r'[^\w\s]', '', name)
        
        return name.strip()
    
    @staticmethod
    def are_duplicates(
        row1: pd.Series,
        row2: pd.Series,
        name_threshold: float = 85.0,
        distance_threshold: float = 500
    ) -> Tuple[bool, float]:
        """
        Verifica se dois registros são duplicatas.
        
        Args:
            row1, row2: Linhas do DataFrame
            name_threshold: Similaridade mínima do nome (0-100)
            distance_threshold: Distância máxima em metros
            
        Returns:
            (is_duplicate, confidence_score)
        """
        # Comparar nomes
        name1 = DuplicateDetector.normalize_name(row1.get('name', ''))
        name2 = DuplicateDetector.normalize_name(row2.get('name', ''))
        
        if not name1 or not name2:
            return False, 0.0
        
        name_similarity = fuzz.token_sort_ratio(name1, name2)
        
        if name_similarity < name_threshold:
            return False, 0.0
        
        # Se nomes são muito similares, verificar localização
        has_coords1 = pd.notna(row1.get('latitude')) and pd.notna(row1.get('longitude'))
        has_coords2 = pd.notna(row2.get('latitude')) and pd.notna(row2.get('longitude'))
        
        if has_coords1 and has_coords2:
            distance = GeographicBlocker.calculate_distance_meters(
                float(row1['latitude']), float(row1['longitude']),
                float(row2['latitude']), float(row2['longitude'])
            )
            
            if distance > distance_threshold:
                # Muito longe, provavelmente não é duplicata
                return False, 0.0
            
            # Calcular confidence baseado em nome e distância
            distance_score = max(0, 100 - (distance / distance_threshold * 100))
            confidence = (name_similarity * 0.7) + (distance_score * 0.3)
            
            return True, confidence / 100.0
        
        # Sem coordenadas, basear apenas no nome e endereço
        addr1_norm = row1.get('address_normalized', '')
        addr2_norm = row2.get('address_normalized', '')
        
        if addr1_norm and addr2_norm:
            addr_similarity = fuzz.ratio(addr1_norm, addr2_norm)
            
            if addr_similarity > 80:
                confidence = (name_similarity * 0.6) + (addr_similarity * 0.4)
                return True, confidence / 100.0
        
        # Nome muito similar mas sem confirmação geográfica
        if name_similarity >= 95:
            return True, name_similarity / 100.0
        
        return False, 0.0
    
    @staticmethod
    def find_duplicate_groups(df: pd.DataFrame, block_indices: List[int]) -> List[List[int]]:
        """
        Encontra grupos de duplicatas dentro de um bloco.
        
        Args:
            df: DataFrame completo
            block_indices: Índices do bloco a processar
            
        Returns:
            Lista de grupos, onde cada grupo é uma lista de índices duplicados
        """
        if len(block_indices) <= 1:
            return [[idx] for idx in block_indices]
        
        # Grafo de duplicatas
        groups = []
        processed = set()
        
        block_df = df.loc[block_indices]
        
        for idx in block_indices:
            if idx in processed:
                continue
            
            group = [idx]
            processed.add(idx)
            
            row1 = block_df.loc[idx]
            
            # Comparar com todos os outros não processados no bloco
            for other_idx in block_indices:
                if other_idx in processed:
                    continue
                
                row2 = block_df.loc[other_idx]
                
                is_dup, confidence = DuplicateDetector.are_duplicates(row1, row2)
                
                if is_dup:
                    group.append(other_idx)
                    processed.add(other_idx)
            
            groups.append(group)
        
        return groups


class MasterRecordBuilder:
    """Constrói registros consolidados (master) a partir de duplicatas."""
    
    # Prioridade de fontes (maior = melhor)
    SOURCE_PRIORITY = {
        'google_places': 3,
        'osm': 2,
        'yelp': 1,
        'manual': 0
    }
    
    @staticmethod
    def select_best_value(*values, source_priority: List[str] = None) -> Any:
        """
        Seleciona o melhor valor entre várias opções.
        
        Args:
            values: Valores a comparar
            source_priority: Lista de fontes em ordem de prioridade
            
        Returns:
            Melhor valor não-nulo
        """
        for val in values:
            if pd.notna(val) and val != '' and val is not None:
                return val
        return None
    
    @staticmethod
    def consolidate_duplicate_group(df: pd.DataFrame, group_indices: List[int]) -> Dict[str, Any]:
        """
        Consolida um grupo de registros duplicados em um único registro master.
        
        Args:
            df: DataFrame completo
            group_indices: Índices dos registros duplicados
            
        Returns:
            Dicionário com dados consolidados
        """
        group_df = df.loc[group_indices].copy()
        
        # Ordenar por prioridade de fonte
        group_df['_source_priority'] = group_df['source'].map(
            lambda x: MasterRecordBuilder.SOURCE_PRIORITY.get(x, 0)
        )
        group_df = group_df.sort_values('_source_priority', ascending=False)
        
        # Registro principal (fonte com maior prioridade)
        primary = group_df.iloc[0]
        
        # Consolidar dados
        master = {
            'master_id': str(uuid.uuid4()),
            
            # Nome: preferir o mais completo
            'name': MasterRecordBuilder.select_best_value(
                *group_df['name'].tolist()
            ),
            
            # Tipo de parque: preferir mais específico
            'park_type': MasterRecordBuilder.select_best_value(
                *group_df['park_type'].tolist()
            ),
            
            # Nomes alternativos
            'alternative_names': [
                str(name) for name in group_df['name'].unique() 
                if pd.notna(name) and name != ''
            ],
            
            # Endereço: preferir mais completo
            'address': MasterRecordBuilder.select_best_value(
                *group_df['address'].tolist()
            ),
            'city': MasterRecordBuilder.select_best_value(
                *group_df['city'].tolist()
            ),
            'state': MasterRecordBuilder.select_best_value(
                *group_df['state'].tolist()
            ),
            'zip_code': MasterRecordBuilder.select_best_value(
                *group_df['zip_code'].tolist()
            ),
            'county': MasterRecordBuilder.select_best_value(
                *group_df['county'].tolist()
            ),
            
            # Coordenadas: média ponderada ou melhor fonte
            'latitude': None,
            'longitude': None,
            'location_confidence': 0.0,
            
            # Contato: preferir Google Places
            'phone': MasterRecordBuilder.select_best_value(
                *group_df[group_df['source'] == 'google_places']['phone'].tolist(),
                *group_df['phone'].tolist()
            ),
            'website': MasterRecordBuilder.select_best_value(
                *group_df[group_df['source'] == 'google_places']['website'].tolist(),
                *group_df['website'].tolist()
            ),
            'email': MasterRecordBuilder.select_best_value(
                *group_df['email'].tolist()
            ),
            
            # Status
            'business_status': MasterRecordBuilder.select_best_value(
                *group_df['business_status'].tolist()
            ) or 'OPERATIONAL',
            
            # Avaliações: média
            'avg_rating': None,
            'total_reviews': 0,
            
            # Metadados
            'source_ids': [],
            'confidence_score': 0.0,
            'data_quality_flags': {},
            'needs_manual_review': False
        }
        
        # Calcular coordenadas (média de fontes confiáveis)
        valid_coords = group_df[
            group_df['latitude'].notna() & group_df['longitude'].notna()
        ]
        
        if len(valid_coords) > 0:
            master['latitude'] = float(valid_coords['latitude'].mean())
            master['longitude'] = float(valid_coords['longitude'].mean())
            master['location_confidence'] = min(1.0, len(valid_coords) / len(group_df))
        
        # Avaliações
        valid_ratings = group_df[group_df['rating'].notna()]
        if len(valid_ratings) > 0:
            master['avg_rating'] = float(valid_ratings['rating'].mean())
            master['total_reviews'] = int(group_df['total_reviews'].sum())
        
        # Source IDs
        master['source_ids'] = [
            {'source': row['source'], 'external_id': row['external_id']}
            for _, row in group_df.iterrows()
            if pd.notna(row['external_id'])
        ]
        
        # Confidence score baseado em número de fontes e qualidade dos dados
        num_sources = len(group_df['source'].unique())
        has_coords = 1.0 if master['latitude'] else 0.0
        has_contact = 0.5 if (master['phone'] or master['website']) else 0.0
        
        master['confidence_score'] = min(1.0, (
            (num_sources / 3) * 0.4 +
            has_coords * 0.4 +
            has_contact * 0.2
        ))
        
        # Flags de qualidade
        master['data_quality_flags'] = {
            'num_sources': num_sources,
            'has_coordinates': bool(master['latitude']),
            'has_contact_info': bool(master['phone'] or master['website']),
            'has_reviews': master['total_reviews'] > 0
        }
        
        # Marcar para revisão manual se dados essenciais faltam
        master['needs_manual_review'] = (
            not master['latitude'] or
            not master['address'] or
            master['confidence_score'] < 0.5
        )
        
        return master


def process_parks_raw_to_master():
    """
    Pipeline completo de deduplicação e consolidação.
    
    Executa:
    1. Carrega dados de parks_raw
    2. Normaliza endereços
    3. Agrupa por ZIP/proximidade
    4. Detecta duplicatas
    5. Consolida registros
    6. Insere em parks_master
    """
    logger.info("="*60)
    logger.info("INICIANDO PROCESSAMENTO: parks_raw → parks_master")
    logger.info("="*60)
    
    # 1. Carregar dados brutos
    logger.info("\n1. Carregando dados de parks_raw...")
    
    with get_db_session() as session:
        result = session.execute(text("""
            SELECT 
                id, external_id, source, name, park_type,
                address, city, state, zip_code, county,
                latitude, longitude,
                phone, website, email,
                business_status, rating, total_reviews,
                raw_data, tags
            FROM parks_raw
            WHERE is_processed = FALSE
            ORDER BY id
        """))
        
        rows = result.fetchall()
        columns = result.keys()
    
    if not rows:
        logger.warning("Nenhum registro encontrado em parks_raw para processar")
        return
    
    df = pd.DataFrame(rows, columns=columns)
    logger.info(f"Carregados {len(df)} registros de parks_raw")
    
    # 2. Normalizar endereços
    logger.info("\n2. Normalizando endereços...")
    
    normalizer = AddressNormalizer()
    
    def normalize_address_row(row):
        normalized = normalizer.parse_address(row['address'])
        return normalized.full_normalized if normalized.full_normalized else row['address']
    
    df['address_normalized'] = df.apply(normalize_address_row, axis=1)
    
    logger.info(f"✓ {len(df[df['address_normalized'].notna()])} endereços normalizados")
    
    # 3. Blocking
    logger.info("\n3. Criando blocos geográficos...")
    
    blocker = GeographicBlocker()
    blocks = blocker.block_by_zip_and_proximity(df, proximity_meters=500)
    
    # 4. Detectar duplicatas
    logger.info("\n4. Detectando duplicatas...")
    
    detector = DuplicateDetector()
    all_duplicate_groups = []
    
    for block_id, block_indices in blocks.items():
        groups = detector.find_duplicate_groups(df, block_indices)
        all_duplicate_groups.extend(groups)
    
    logger.info(f"✓ Encontrados {len(all_duplicate_groups)} grupos únicos")
    
    # 5. Consolidar registros
    logger.info("\n5. Consolidando registros master...")
    
    builder = MasterRecordBuilder()
    master_records = []
    
    for group in all_duplicate_groups:
        master = builder.consolidate_duplicate_group(df, group)
        master_records.append(master)
    
    logger.info(f"✓ Criados {len(master_records)} registros master")
    
    # Estatísticas
    needs_review = sum(1 for m in master_records if m['needs_manual_review'])
    avg_confidence = sum(m['confidence_score'] for m in master_records) / len(master_records)
    
    logger.info(f"  - Confiança média: {avg_confidence:.2%}")
    logger.info(f"  - Requerem revisão manual: {needs_review}")
    
    # 6. Inserir em parks_master
    logger.info("\n6. Inserindo registros em parks_master...")
    
    insert_count = 0
    error_count = 0
    
    with get_db_session() as session:
        for master in master_records:
            try:
                import json
                
                session.execute(text("""
                    INSERT INTO parks_master (
                        master_id, name, park_type, alternative_names,
                        address, city, state, zip_code, county,
                        latitude, longitude, geom, location_confidence,
                        phone, website, email,
                        business_status, avg_rating, total_reviews,
                        source_ids, confidence_score, data_quality_flags,
                        needs_manual_review
                    ) VALUES (
                        :master_id, :name, :park_type, CAST(:alternative_names AS text[]),
                        :address, :city, :state, :zip_code, :county,
                        :latitude, :longitude,
                        CAST(ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326) AS geography),
                        :location_confidence,
                        :phone, :website, :email,
                        :business_status, :avg_rating, :total_reviews,
                        CAST(:source_ids AS jsonb), :confidence_score, CAST(:data_quality_flags AS jsonb),
                        :needs_manual_review
                    )
                    ON CONFLICT (master_id) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'master_id': master['master_id'],
                    'name': master['name'],
                    'park_type': master['park_type'],
                    'alternative_names': master['alternative_names'],
                    'address': master['address'],
                    'city': master['city'],
                    'state': master['state'],
                    'zip_code': master['zip_code'],
                    'county': master['county'],
                    'latitude': master['latitude'],
                    'longitude': master['longitude'],
                    'location_confidence': master['location_confidence'],
                    'phone': master['phone'],
                    'website': master['website'],
                    'email': master['email'],
                    'business_status': master['business_status'],
                    'avg_rating': master['avg_rating'],
                    'total_reviews': master['total_reviews'],
                    'source_ids': json.dumps(master['source_ids']),
                    'confidence_score': master['confidence_score'],
                    'data_quality_flags': json.dumps(master['data_quality_flags']),
                    'needs_manual_review': master['needs_manual_review']
                })
                
                insert_count += 1
                
            except Exception as e:
                logger.error(f"Erro ao inserir master {master.get('name', 'N/A')}: {e}")
                error_count += 1
        
        # Marcar registros como processados
        session.execute(text("""
            UPDATE parks_raw
            SET is_processed = TRUE
            WHERE id IN :ids
        """), {'ids': tuple(df['id'].tolist())})
        
        session.commit()
    
    logger.info(f"✓ {insert_count} registros inseridos, {error_count} erros")
    
    # Resumo final
    logger.info("\n" + "="*60)
    logger.success("PROCESSAMENTO CONCLUÍDO")
    logger.info("="*60)
    logger.info(f"Registros brutos processados: {len(df)}")
    logger.info(f"Registros master criados: {insert_count}")
    logger.info(f"Taxa de deduplicação: {(1 - insert_count/len(df)):.1%}")
    logger.info("="*60)
    
    return master_records


if __name__ == "__main__":
    # Exemplo de uso
    logger.add(
        "logs/deduplication_{time}.log",
        rotation="1 day",
        level="DEBUG"
    )
    
    process_parks_raw_to_master()
