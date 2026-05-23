from backend import config
import database_reader
import storage


def supabase_status():
    status = config.supabase_status()

    return {
        "url_configured": status["url_configured"] or bool(storage.SUPABASE_URL),
        "key_configured": status["key_configured"] or bool(storage.SUPABASE_KEY),
    }


def read_table(table_name, params=None):
    return database_reader.read_supabase_table(table_name, params=params)


def insert_row(table_name, payload):
    return storage.post_to_supabase(table_name, payload)
