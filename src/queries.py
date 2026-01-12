from psycopg2.extensions import cursor as Cursor


def test_query(query : str, cur : Cursor):
    BLOCKED_COMMANDS = ["INSERT", "DELETE", "DROP", "CREATE", "ALTER"]

    command = query.split()[0]

    if command in BLOCKED_COMMANDS:
        raise PermissionError("Command not allowed")
    
    cur.execute(query)