"""Supabase database client setup.

Uses the Supabase Python client with the service_role key for
full backend access to the PostgreSQL database.
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

_client: Client | None = None


def get_db() -> Client:
    """Get the Supabase client singleton."""
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client
