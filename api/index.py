#!/usr/bin/env python3
"""
BellaTerra Intelligence - Vercel API
Dashboard for MHP/RV Parks data visualization.
"""

import os
import sys

# Path configuration BEFORE importing Flask
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.insert(0, root_dir)

from flask import Flask, render_template, jsonify, request
from sqlalchemy import create_engine, text

# Flask configuration for Vercel
template_path = os.path.join(root_dir, 'web', 'templates')
static_path = os.path.join(root_dir, 'web', 'static')

# Check if directories exist
if not os.path.exists(template_path):
    template_path = None
if not os.path.exists(static_path):
    static_path = None

app = Flask(__name__, 
    template_folder=template_path,
    static_folder=static_path
)
app.secret_key = os.getenv('SECRET_KEY', 'bellaterra-secret-key-2025')

# Disable debug in production
app.config['DEBUG'] = False
app.config['PROPAGATE_EXCEPTIONS'] = True

# Engine cache
_engine_cache = None

def get_engine():
    """Creates PostgreSQL database engine."""
    global _engine_cache
    if _engine_cache is None:
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        
        if not all([host, database, user, password]):
            raise ValueError("Database credentials not configured. Please set DB_HOST, DB_NAME, DB_USER, and DB_PASSWORD environment variables in Vercel.")
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        _engine_cache = create_engine(
            connection_string, 
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={
                "connect_timeout": 10,
                "application_name": "bellaterra_vercel"
            }
        )
    return _engine_cache


def get_db_stats():
    """Returns database statistics."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            stats = {}
            
            # Total parks raw
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM parks_raw"))
                stats['parks_raw'] = r.scalar() or 0
            except Exception:
                stats['parks_raw'] = 0
            
            # Total parks master
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM parks_master"))
                stats['parks_master'] = r.scalar() or 0
            except Exception:
                stats['parks_master'] = 0
            
            # By state
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
            
            # With website
            try:
                r = conn.execute(text("""
                    SELECT COUNT(*) FROM parks_master 
                    WHERE website IS NOT NULL AND website != ''
                """))
                stats['with_website'] = r.scalar() or 0
            except Exception:
                stats['with_website'] = 0
            
            # With phone
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
            
            # Corporate owners (is_individual = false)
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
            
            # Contacts
            try:
                r = conn.execute(text("SELECT COUNT(*) FROM contacts"))
                stats['contacts'] = r.scalar() or 0
            except Exception:
                stats['contacts'] = 0
            
            # Contacts with email
            try:
                r = conn.execute(text("""
                    SELECT COUNT(*) FROM contacts 
                    WHERE email IS NOT NULL AND email != ''
                """))
                stats['contacts_with_email'] = r.scalar() or 0
            except Exception:
                stats['contacts_with_email'] = 0
            
            return stats
    except ValueError as ve:
        return {'error': str(ve), 'db_configured': False, 'parks_raw': 0, 'parks_master': 0, 'by_state': {}, 
                'with_website': 0, 'with_phone': 0, 'owners': 0, 'corporate_owners': 0,
                'companies': 0, 'contacts': 0, 'contacts_with_email': 0}
    except Exception as e:
        return {'error': f'Database connection error: {str(e)}', 'db_configured': True, 'parks_raw': 0, 'parks_master': 0, 'by_state': {}, 
                'with_website': 0, 'with_phone': 0, 'owners': 0, 'corporate_owners': 0,
                'companies': 0, 'contacts': 0, 'contacts_with_email': 0}


def get_states_list():
    """Returns list of available states."""
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
    """Returns list of counties for a state."""
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
# ROUTES
# ============================================================================

@app.route('/')
def index():
    """Main dashboard page."""
    return render_template('dashboard.html')


@app.route('/health')
def health():
    """Health check endpoint."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'db_host': os.getenv('DB_HOST', 'not configured')
        })
    except ValueError as ve:
        return jsonify({
            'status': 'error',
            'database': 'not configured',
            'message': str(ve)
        }), 500
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database': 'connection failed',
            'message': str(e)
        }), 500


@app.route('/api/stats')
def api_stats():
    """Returns database statistics."""
    stats = get_db_stats()
    return jsonify(stats)


@app.route('/api/states')
def api_states():
    """Returns list of states."""
    states = get_states_list()
    return jsonify(states)


@app.route('/api/counties/<state_code>')
def api_counties(state_code):
    """Returns counties for a state."""
    counties = get_counties_for_state(state_code)
    return jsonify(counties)


@app.route('/api/pipeline/status')
def api_pipeline_status():
    """Returns pipeline status - always idle on Vercel."""
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
        'message': 'Pipeline not available in Vercel mode. Run locally to execute.'
    })


@app.route('/api/pipeline/run', methods=['POST'])
def api_run_pipeline():
    """Pipeline not available on Vercel."""
    return jsonify({
        'error': 'Pipeline execution not available in Vercel deployment. Please run locally.',
        'vercel_mode': True
    }), 400


@app.route('/api/properties')
def api_properties():
    """Returns list of properties with pagination."""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        search = request.args.get('search', '').strip()
        
        offset = (page - 1) * limit
        
        engine = get_engine()
        with engine.connect() as conn:
            # Base query
            base_where = "1=1"
            params = {'limit': limit, 'offset': offset}
            
            if search:
                base_where = "(LOWER(name) LIKE :search OR LOWER(city) LIKE :search OR LOWER(state) LIKE :search)"
                params['search'] = f'%{search.lower()}%'
            
            # Total
            count_sql = f"SELECT COUNT(*) FROM parks_master WHERE {base_where}"
            total = conn.execute(text(count_sql), params).scalar() or 0
            
            # Data - using correct columns: latitude/longitude
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
    """Returns list of owners with pagination."""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        search = request.args.get('search', '').strip()
        
        offset = (page - 1) * limit
        
        engine = get_engine()
        with engine.connect() as conn:
            # Base query - using full_name (correct column)
            base_where = "1=1"
            params = {'limit': limit, 'offset': offset}
            
            if search:
                base_where = "LOWER(full_name) LIKE :search"
                params['search'] = f'%{search.lower()}%'
            
            # Total
            count_sql = f"SELECT COUNT(*) FROM owners WHERE {base_where}"
            total = conn.execute(text(count_sql), params).scalar() or 0
            
            # Data - using full_name and is_individual (correct columns)
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
    """Returns recent logs - empty on Vercel."""
    return jsonify([])


@app.route('/api/clear-logs', methods=['POST'])
def api_clear_logs():
    """Clears logs."""
    return jsonify({'status': 'ok'})


# WSGI application for Vercel
# The `app` variable is automatically detected by Vercel

# For local development
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

