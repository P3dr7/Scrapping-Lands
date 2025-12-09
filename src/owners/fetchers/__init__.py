"""
Fetchers for different county assessor systems.
"""

from src.owners.fetchers.generic_fetcher import (
    GenericWebSearchFetcher,
    MockFetcher,
    get_fetcher_for_county
)

__all__ = [
    'GenericWebSearchFetcher',
    'MockFetcher',
    'get_fetcher_for_county'
]
