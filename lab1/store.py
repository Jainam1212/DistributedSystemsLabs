import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "Qwerty@123",
    "database": "distributed_sys",
}


def sqlConn():
    c = mysql.connector.connect(**DB_CONFIG)
    cursor = c.cursor()

    with open("./migration/001.sql", "r") as file:
        sql = file.read()
    for sql_cmd in sql.split(";"):
        sql_cmd = sql_cmd.strip()
        if sql_cmd:
            cursor.execute(sql_cmd)
    c.commit()
    cursor.close()

    return c


def set_result(id, status, result=None, error=None):
    c = sqlConn()
    cursor = c.cursor()
    cursor.execute(
        """
        INSERT INTO results (id, status, result, error)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            result = VALUES(result),
            error = VALUES(error)
        """,
        (id, status, result, error),
    )

    c.commit()
    cursor.close()
    c.close()


def get_result(id):
    c = sqlConn()

    cursor = c.cursor()

    cursor.execute(
        """
        SELECT id, status, result, error
        FROM results
        WHERE id = %s
        """,
        (id),
    )

    row = cursor.fetchone()

    cursor.close()
    c.close()

    if not row:
        return None

    return {
        "id": row[0],
        "status": row[1],
        "result": row[2],
        "error": row[3],
    }