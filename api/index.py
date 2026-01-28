#!/usr/bin/env python3
"""
BellaTerra Intelligence - API para Vercel
Dashboard para visualização de dados de MHP/RV Parks.
"""

import os
import sys
from datetime import datetime
from flask import Flask, render_template, jsonify, request

# Adiciona o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.insert(0, root_dir)

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração Flask para Vercel
app = Flask(__name__, 
    template_folder=os.path.join(root_dir, 'web', 'templates'),
    static_folder=os.path.join(root_dir, 'web', 'static')
)
app.secret_key = os.getenv('SECRET_KEY', 'bellaterra-secret-key-2025')

# Cache do engine
_engine_cache = None

def get_engine():
    """Cria engine de conexão com banco PostgreSQL."""
    global _engine_cache
    if _engine_cache is None:
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME", "mhp_intelligence")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "")
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        _engine_cache = create_engine(connection_string, pool_pre_ping=True)
    return _engine_cache


def get_db_stats():
    """Retorna estatísticas do banco de dados."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            stats = {}
            
            # Total de parques raw
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM parks_raw"))
                stats['parks_raw'] = r.scalar() or 0
            except Exception:
                stats['parks_raw'] = 0
            
            # Total de parques master
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM parks_master"))
                stats['parks_master'] = r.scalar() or 0
            except Exception:
                stats['parks_master'] = 0
            
            # Por estado
            try:
                r = conn.execute(text("""
                    SELECT state, COUNT(*) 
                    FROM parks_master 
                    WHERE state IS NOT NULL
                    GROUP BY state 
                    ORDER BY COUNT(*) DESC
                """))
                stats['by_state'] = {row[0]: row[1] for row in r}
            except Exception:
                stats['by_state'] = {}
            
            # Com website
            try:
                r = conn.execute(text("""
                    SELECT COUNT(*) FROM parks_master 
                    WHERE website IS NOT NULL AND website != ''
                """))
                stats['with_website'] = r.scalar() or 0
            except Exception:
                stats['with_website'] = 0
            
            # Com telefone
            try:
                r = conn.execute(text("""
                    SELECT COUNT(*) FROM parks_master 
                    WHERE phone IS NOT NULL AND phone != ''
                """))
                stats['with_phone'] = r.scalar() or 0
            except Exception:
                stats['with_phone'] = 0
            
            # Owners
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM owners"))
                stats['owners'] = r.scalar() or 0
            except Exception:
                stats['owners'] = 0
            
            # Owners corporativos (is_individual = false)
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM owners WHERE is_individual = false"))
                stats['corporate_owners'] = r.scalar() or 0
            except Exception:
                stats['corporate_owners'] = 0
            
            # Companies
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM companies"))
                stats['companies'] = r.scalar() or 0
            except Exception:
                stats['companies'] = 0
            
            # Contatos
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM contacts"))
                stats['contacts'] = r.scalar() or 0
            except Exception:
                stats['contacts'] = 0
            
            # Contatos com email
            try:
                r = conn.execute(text("""
                    SELECT COUNT(*) FROM contacts 
                    WHERE email IS NOT NULL AND email != ''
                """))
                stats['contacts_with_email'] = r.scalar() or 0
            except Exception:
                stats['contacts_with_email'] = 0
            
            return stats
    except Exception as e:
        return {'error': str(e), 'parks_raw': 0, 'parks_master': 0, 'by_state': {}, 
                'with_website': 0, 'with_phone': 0, 'owners': 0, 'corporate_owners': 0,
                'companies': 0, 'contacts': 0, 'contacts_with_email': 0}


def get_states_list():
    """Retorna lista de estados disponíveis."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT DISTINCT state, COUNT(*) as cnt
                FROM parks_master 
                WHERE state IS NOT NULL
                GROUP BY state
                ORDER BY cnt DESC
            """))
            return [{'code': row[0], 'count': row[1]} for row in r]
    except Exception:
        return []


def get_counties_for_state(state_code: str):
    """Retorna lista de condados para um estado."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT DISTINCT county, COUNT(*) as cnt
                FROM parks_master 
                WHERE state = :state AND county IS NOT NULL
                GROUP BY county
                ORDER BY county
            """), {'state': state_code})
            return [{'name': row[0], 'count': row[1]} for row in r]
    except Exception:
        return []


# ============================================================================
# ROTAS
# ============================================================================

@app.route('/')
def index():
    """Página principal."""
    return render_template('dashboard.html')


@app.route('/api/stats')
def api_stats():
    """Retorna estatísticas do banco."""
    stats = get_db_stats()
    return jsonify(stats)


@app.route('/api/states')
def api_states():
    """Retorna lista de estados."""
    states = get_states_list()
    return jsonify(states)


@app.route('/api/counties/<state_code>')
def api_counties(state_code):
    """Retorna condados de um estado."""
    counties = get_counties_for_state(state_code)
    return jsonify(counties)


@app.route('/api/pipeline/status')
def api_pipeline_status():
    """Retorna status do pipeline - sempre idle no Vercel."""
    return jsonify({
        'phase1_running': False,
        'phase2_running': False,
        'phase3_running': False,
        'phase4_running': False,
        'phase5_running': False,
        'export_running': False,
        'current_task': None,
        'progress': 0,
        'logs': [],
        'last_update': None,
        'vercel_mode': True,
        'message': 'Pipeline não disponível no modo Vercel. Use localmente para executar.'
    })


@app.route('/api/pipeline/run', methods=['POST'])
def api_run_pipeline():
    """Pipeline não disponível no Vercel."""
    return jsonify({
        'error': 'Pipeline execution not available in Vercel deployment. Please run locally.',
        'vercel_mode': True
    }), 400


@app.route('/api/properties')
def api_properties():
    """Retorna lista de propriedades com paginação."""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        search = request.args.get('search', '').strip()
        
        offset = (page - 1) * limit
        
        engine = get_engine()
        with engine.connect() as conn:
            # Query base
            base_where = "1=1"
            params = {'limit': limit, 'offset': offset}
            
            if search:
                base_where = "(LOWER(name) LIKE :search OR LOWER(city) LIKE :search OR LOWER(state) LIKE :search)"
                params['search'] = f'%{search.lower()}%'
            
            # Total
            count_sql = f"SELECT COUNT(*) FROM parks_master WHERE {base_where}"
            total = conn.execute(text(count_sql), params).scalar() or 0
            
            # Dados - usando colunas corretas: latitude/longitude
            data_sql = f"""
                SELECT id, name, city, state, phone, website, address, latitude, longitude
                FROM parks_master 
                WHERE {base_where}
                ORDER BY name ASC
                LIMIT :limit OFFSET :offset
            """
            rows = conn.execute(text(data_sql), params).fetchall()
            
            items = []
            for row in rows:
                items.append({
                    'id': row[0],
                    'name': row[1],
                    'city': row[2],
                    'state': row[3],
                    'phone': row[4],
                    'website': row[5],
                    'address': row[6],
                    'lat': float(row[7]) if row[7] else None,
                    'lon': float(row[8]) if row[8] else None,
                })
            
            return jsonify({
                'items': items,
                'total': total,
                'page': page,
                'pages': (total + limit - 1) // limit,
            })
    except Exception as e:
        return jsonify({'items': [], 'total': 0, 'error': str(e)})


@app.route('/api/owners')
def api_owners():
    """Retorna lista de proprietários com paginação."""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        search = request.args.get('search', '').strip()
        
        offset = (page - 1) * limit
        
        engine = get_engine()
        with engine.connect() as conn:
            # Query base - usando full_name (coluna correta)
            base_where = "1=1"
            params = {'limit': limit, 'offset': offset}
            
            if search:
                base_where = "LOWER(full_name) LIKE :search"
                params['search'] = f'%{search.lower()}%'
            
            # Total
            count_sql = f"SELECT COUNT(*) FROM owners WHERE {base_where}"
            total = conn.execute(text(count_sql), params).scalar() or 0
            
            # Dados - usando full_name e is_individual (colunas corretas)
            data_sql = f"""
                SELECT 
                    o.id, o.full_name, o.is_individual, o.source,
                    (SELECT COUNT(*) FROM parks_master pm WHERE pm.owner_id = o.id) as property_count
                FROM owners o
                WHERE {base_where}
                ORDER BY o.full_name ASC
                LIMIT :limit OFFSET :offset
            """
            rows = conn.execute(text(data_sql), params).fetchall()
            
            items = []
            for row in rows:
                items.append({
                    'id': row[0],
                    'name': row[1],
                    'is_corporate': not row[2] if row[2] is not None else False,
                    'source': row[3],
                    'property_count': row[4] or 1,
                })
            
            return jsonify({
                'items': items,
                'total': total,
                'page': page,
                'pages': (total + limit - 1) // limit,
            })
    except Exception as e:
        return jsonify({'items': [], 'total': 0, 'error': str(e)})


@app.route('/api/logs')
def api_logs():
    """Retorna logs recentes - vazio no Vercel."""
    return jsonify([])


@app.route('/api/clear-logs', methods=['POST'])
def api_clear_logs():
    """Limpa os logs."""
    return jsonify({'status': 'ok'})


# Handler para Vercel
def handler(event, context):
    return app(event, context)


# Para rodar localmente
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
