"""
Standalone Quack test server.

Starts a DuckDB instance with the quack extension and serves on
localhost:19494 (non-default port to avoid conflicts).

Usage:
    python -m tests.quack_test_server

The server runs until terminated (SIGTERM/SIGINT).
"""
import signal
import sys

import duckdb

QUACK_TEST_PORT = 19494
QUACK_TEST_TOKEN = "dbt_quack_test_token"
QUACK_TEST_URI = f"quack:localhost:{QUACK_TEST_PORT}"


def create():
    conn = duckdb.connect()  # in-memory server database
    conn.execute("INSTALL quack FROM core_nightly")
    conn.execute("LOAD quack")
    conn.execute(f"CALL quack_serve('{QUACK_TEST_URI}', token = '{QUACK_TEST_TOKEN}')")
    return conn


if __name__ == "__main__":
    conn = create()
    print(f"Quack test server running on {QUACK_TEST_URI}", flush=True)

    # Block until signal
    try:
        signal.sigwait({signal.SIGTERM, signal.SIGINT})
    except KeyboardInterrupt:
        pass

    try:
        conn.execute(f"CALL quack_stop('{QUACK_TEST_URI}')")
    except Exception:
        pass
    conn.close()
    sys.exit(0)
