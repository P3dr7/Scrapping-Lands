"""Pacote de ingestão de dados de múltiplas fontes."""

from .osm_query import fetch_osm_parks
from .google_places import fetch_google_parks

__all__ = ['fetch_osm_parks', 'fetch_google_places']
