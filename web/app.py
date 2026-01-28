#!/usr/bin/env python3
"""
BellaTerra Intelligence - Interface Web Profissional
Dashboard para controle do pipeline de scraping de MHP/RV Parks.
"""

import os
import sys
import json
import threading
from datetime import datetime
from flask import Flask, render_template, jsonify, request

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.database import get_engine

app = Flask(__name__)
app.secret_key = 'bellaterra-secret-key-2025'

# Cache do engine para evitar reconexões frequentes
_engine_cache = None

def get_cached_engine():
    """Retorna engine cacheado."""
    global _engine_cache
    if _engine_cache is None:
        _engine_cache = get_engine()
    return _engine_cache

# Estado global do pipeline
pipeline_state = {
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
}

# Lock para thread safety
state_lock = threading.Lock()


def add_log(message: str, level: str = 'info'):
    """Adiciona mensagem ao log."""
    with state_lock:
        pipeline_state['logs'].append({
            'time': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'message': message
        })
        # Manter apenas os últimos 100 logs
        if len(pipeline_state['logs']) > 100:
            pipeline_state['logs'] = pipeline_state['logs'][-100:]
        pipeline_state['last_update'] = datetime.now().isoformat()


def get_db_stats():
    """Retorna estatísticas do banco de dados."""
    try:
        engine = get_cached_engine()
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
            
            # Owners corporativos
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM owners WHERE is_corporate = true"))
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
        engine = get_cached_engine()
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
        engine = get_cached_engine()
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
    """Retorna status do pipeline."""
    with state_lock:
        return jsonify(pipeline_state)


@app.route('/api/pipeline/run', methods=['POST'])
def api_run_pipeline():
    """Executa uma fase do pipeline."""
    data = request.json
    phase = data.get('phase')
    options = data.get('options', {})
    
    if not phase:
        return jsonify({'error': 'Phase not specified'}), 400
    
    # Verificar se já há algo rodando
    with state_lock:
        running = any([
            pipeline_state['phase1_running'],
            pipeline_state['phase2_running'],
            pipeline_state['phase3_running'],
            pipeline_state['phase4_running'],
            pipeline_state['phase5_running'],
            pipeline_state['export_running'],
        ])
        if running:
            return jsonify({'error': 'Another phase is already running'}), 400
    
    # Iniciar a fase em background
    if phase == 'phase1':
        thread = threading.Thread(target=run_phase1, args=(options,))
    elif phase == 'phase2':
        thread = threading.Thread(target=run_phase2, args=(options,))
    elif phase == 'phase3':
        thread = threading.Thread(target=run_phase3, args=(options,))
    elif phase == 'phase4':
        thread = threading.Thread(target=run_phase4, args=(options,))
    elif phase == 'phase5':
        thread = threading.Thread(target=run_phase5, args=(options,))
    elif phase == 'export':
        thread = threading.Thread(target=run_export, args=(options,))
    else:
        return jsonify({'error': f'Unknown phase: {phase}'}), 400
    
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started', 'phase': phase})


def run_phase1(options):
    """Executa Fase 1: Ingestão."""
    with state_lock:
        pipeline_state['phase1_running'] = True
        pipeline_state['current_task'] = 'Fase 1: Ingestão de Dados'
    
    add_log('🚀 Iniciando Fase 1: Ingestão via Google Places API', 'info')
    
    try:
        import subprocess
        states = options.get('states', ['IN'])
        add_log(f'📍 Estados selecionados: {", ".join(states)}', 'info')
        
        result = subprocess.run(
            ['python', 'scripts/run_ingest.py'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            timeout=3600
        )
        
        if result.returncode == 0:
            add_log('✅ Fase 1 concluída com sucesso!', 'success')
        else:
            add_log(f'⚠️ Fase 1 com avisos', 'warning')
        
    except subprocess.TimeoutExpired:
        add_log('⏰ Fase 1 timeout após 1 hora', 'warning')
    except Exception as e:
        add_log(f'❌ Erro na Fase 1: {str(e)}', 'error')
    
    finally:
        with state_lock:
            pipeline_state['phase1_running'] = False
            pipeline_state['current_task'] = None


def run_phase2(options):
    """Executa Fase 2: Deduplicação."""
    with state_lock:
        pipeline_state['phase2_running'] = True
        pipeline_state['current_task'] = 'Fase 2: Deduplicação'
    
    add_log('🔄 Iniciando Fase 2: Deduplicação', 'info')
    
    try:
        import subprocess
        result = subprocess.run(
            ['python', 'scripts/process_to_master.py'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            timeout=600
        )
        
        if result.returncode == 0:
            add_log('✅ Fase 2 concluída com sucesso!', 'success')
        else:
            add_log(f'❌ Erro na Fase 2', 'error')
            
    except subprocess.TimeoutExpired:
        add_log('⏰ Fase 2 timeout após 10 minutos', 'warning')
    except Exception as e:
        add_log(f'❌ Erro na Fase 2: {str(e)}', 'error')
    
    finally:
        with state_lock:
            pipeline_state['phase2_running'] = False
            pipeline_state['current_task'] = None


def run_phase3(options):
    """Executa Fase 3: Identificação de Owners."""
    with state_lock:
        pipeline_state['phase3_running'] = True
        pipeline_state['current_task'] = 'Fase 3: Identificação de Proprietários'
    
    add_log('👤 Iniciando Fase 3: Criando owners a partir dos parques', 'info')
    
    try:
        import subprocess
        result = subprocess.run(
            ['python', 'scripts/create_owners_from_parks.py'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            timeout=600
        )
        
        if result.returncode == 0:
            add_log('✅ Fase 3 concluída com sucesso!', 'success')
        else:
            add_log(f'❌ Erro na Fase 3', 'error')
            
    except subprocess.TimeoutExpired:
        add_log('⏰ Fase 3 timeout após 10 minutos', 'warning')
    except Exception as e:
        add_log(f'❌ Erro na Fase 3: {str(e)}', 'error')
    
    finally:
        with state_lock:
            pipeline_state['phase3_running'] = False
            pipeline_state['current_task'] = None


def run_phase4(options):
    """Executa Fase 4: Enriquecimento Corporativo."""
    with state_lock:
        pipeline_state['phase4_running'] = True
        pipeline_state['current_task'] = 'Fase 4: Enriquecimento Corporativo'
    
    add_log('🏢 Iniciando Fase 4: Enriquecimento Corporativo', 'info')
    add_log('⚠️ Nota: APIs do SOS podem estar bloqueadas', 'warning')
    
    try:
        import subprocess
        limit = options.get('limit', 50)
        result = subprocess.run(
            ['python', 'scripts/enrich_corporate.py', '--limit', str(limit)],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            timeout=300
        )
        
        if result.returncode == 0:
            add_log('✅ Fase 4 concluída', 'success')
        else:
            add_log('⚠️ Fase 4: APIs podem estar bloqueadas', 'warning')
            
    except subprocess.TimeoutExpired:
        add_log('⏰ Fase 4 timeout - APIs do SOS bloqueando', 'warning')
    except Exception as e:
        add_log(f'❌ Erro na Fase 4: {str(e)}', 'error')
    
    finally:
        with state_lock:
            pipeline_state['phase4_running'] = False
            pipeline_state['current_task'] = None


def run_phase5(options):
    """Executa Fase 5: Enriquecimento de Contatos."""
    with state_lock:
        pipeline_state['phase5_running'] = True
        pipeline_state['current_task'] = 'Fase 5: Scraping de Contatos'
    
    add_log('📧 Iniciando Fase 5: Scraping de contatos dos websites', 'info')
    
    try:
        import subprocess
        limit = options.get('limit')
        cmd = ['python', 'scripts/enrich_contacts.py', '--source', 'website', '--skip-apis']
        if limit:
            cmd.extend(['--limit', str(limit)])
            
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            timeout=3600
        )
        
        if result.returncode == 0:
            add_log('✅ Fase 5 concluída com sucesso!', 'success')
        else:
            add_log('⚠️ Fase 5 concluída com alguns avisos', 'warning')
            
    except subprocess.TimeoutExpired:
        add_log('⏰ Fase 5 timeout após 1 hora', 'warning')
    except Exception as e:
        add_log(f'❌ Erro na Fase 5: {str(e)}', 'error')
    
    finally:
        with state_lock:
            pipeline_state['phase5_running'] = False
            pipeline_state['current_task'] = None


def run_export(options):
    """Executa exportação de CSVs."""
    with state_lock:
        pipeline_state['export_running'] = True
        pipeline_state['current_task'] = 'Exportando CSVs'
    
    states = options.get('states', ['IN'])
    add_log(f'📊 Iniciando exportação para: {", ".join(states)}', 'info')
    
    try:
        import subprocess
        
        # Gerar CSVs
        result = subprocess.run(
            ['python', 'scripts/generate_csvs.py'],
            input="1\n",
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            timeout=300
        )
        
        if result.returncode == 0:
            add_log('✅ CSVs gerados com sucesso!', 'success')
            add_log('📁 Arquivos salvos em output/', 'info')
        else:
            add_log('❌ Erro ao gerar CSVs', 'error')
            
    except subprocess.TimeoutExpired:
        add_log('⏰ Exportação timeout', 'warning')
    except Exception as e:
        add_log(f'❌ Erro na exportação: {str(e)}', 'error')
    
    finally:
        with state_lock:
            pipeline_state['export_running'] = False
            pipeline_state['current_task'] = None


@app.route('/api/properties')
def api_properties():
    """Retorna lista de propriedades com paginação."""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        search = request.args.get('search', '').strip()
        
        offset = (page - 1) * limit
        
        engine = get_cached_engine()
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
        
        engine = get_cached_engine()
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
            # Contagem de propriedades via parks_master.owner_id
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
                    'is_corporate': not row[2] if row[2] is not None else False,  # is_individual inverso
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
    """Retorna logs recentes."""
    with state_lock:
        return jsonify(pipeline_state['logs'][-50:])


@app.route('/api/clear-logs', methods=['POST'])
def api_clear_logs():
    """Limpa os logs."""
    with state_lock:
        pipeline_state['logs'] = []
    add_log('🗑️ Logs limpos', 'info')
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║       BellaTerra Intelligence - Dashboard                ║")
    print("║       Acesse: http://localhost:5000                      ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    app.run(debug=True, host='0.0.0.0', port=5000)
