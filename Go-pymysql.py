import datetime
import hashlib
import pymysql


def check_databases_exist(source_host, source_user, source_password, source_database, target_host, target_user, target_password, target_database):
    """
    檢查來源和目標資料庫是否存在

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_password (str): 來源資料庫密碼
        source_database (str): 來源資料庫名稱
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
    """

    # 連接來源資料庫
    source_connection = pymysql.connect(
        host=source_host,
        user=source_user,
        password=source_password,
        database=source_database,
    )

    # 檢查來源資料庫是否存在
    with source_connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()

    if source_database not in databases:
        raise Exception(f"Source database '{source_database}' does not exist")

    # 連接目標資料庫
    target_connection = pymysql.connect(
        host=target_host,
        user=target_user,
        password=target_password,
        database=target_database,
    )

    # 檢查目標資料庫是否存在
    with target_connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()

    if target_database not in databases:
        raise Exception(f"Target database '{target_database}' does not exist")


def create_archived_table(target_host, target_user, target_password, target_database, table_name):
    """
    在目標資料庫中建立歸檔資料表

    Args:
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        table_name (str): 要歸檔的資料表名稱
    """

    archived_table_name = f"{table_name}_archived"

    # 連接目標資料庫
    target_connection = pymysql.connect(
        host=target_host,
        user=target_user,
        password=target_password,
        database=target_database,
    )

    # 檢查目標資料庫中是否存在歸檔資料表
    with target_connection.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE {target_database}.{archived_table_name}")
        result = cursor.fetchone()

    if result is None:
        # 建立歸檔資料表
        with target_connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {target_database}.{archived_table_name} LIKE {target_database}.{table_name}")
        target_connection.commit()


def archive_table_with_batching_and_verification(
    source_host,
    source_user,
    source_password,
    source_database,
    target_host,
    target_user,
    target_password,
    target_database,
    table_name,
    batch_size=10000,
    archive_days=30,
):
    """
    從一個資料庫主機讀取資料表並歸檔到另一個資料庫主機，以批次方式處理資料，並進行資料驗證

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_password (str): 來源資料庫密碼
        source_database (str): 來源資料庫名稱
def archive_table_with_batching_and_verification(
    source_host,
    source_user,
    source_password,
    source_database,
    target_host,
    target_user,
    target_password,
    target_database,
    table_name,
    batch_size=10000,
    archive_days=30,
):
    """
    從一個資料庫主機讀取資料表並歸檔到另一個資料庫主機，以批次方式處理資料，並進行資料驗證

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_password (str): 來源資料庫密碼
        source_database (str): 來源資料庫名稱
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        table_name (str): 要歸檔的資料表名稱
        batch_size (int, optional): 每批資料的行數，預設為 10000
        archive_days (int, optional): 歸檔資料的日期條件，以天數為單位，預設為 30
    """

    # 獲取歸檔日期條件
    archive_date = datetime.datetime.now() - datetime.timedelta(days=archive_days)

    # 建立目標歸檔資料表
    create_archived_table(target_host, target_user, target_password, target_database, table_name)

    # 匯出來源資料
    source_connection = pymysql.connect(
        host=source_host,
        user=source_user,
        password=source_password,
        database=source_database,
    )
    with source_connection.cursor() as cursor:
        # 獲取資料表欄位名稱
        cursor.execute(f"DESCRIBE {source_database}.{table_name}")
        column_names = [column[0] for column in cursor.fetchall()]

        # 逐批匯出資料
        offset = 0
        while True:
            limit_clause = f"LIMIT {offset}, {batch_size}" if batch_size else ""
            query = f"""
            SELECT * FROM {source_database}.{table_name}
            WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'
            {limit_clause}
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                break

            # 匯入目標資料庫
            target_connection = pymysql.connect(
                host=target_host,
                user=target_user,
                password=target_password,
                database=target_database,
            )
            with target_connection.cursor() as cursor:
                cursor.executemany(
                    f"INSERT INTO {target_database}.{table_name}_archived ({','.join(column_names)}) VALUES (%s, %s, ...)",
                    results,
                )
            target_connection.commit()

            offset += batch_size

    # 刪除來源資料
    with source_connection.cursor() as cursor:
        query = f"""
        DELETE FROM {source_database}.{table_name}
        WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'
        """
        cursor.execute(query)
    source_connection.commit()


def archive_tables(
    source_host,
    source_user,
    source_password,
    source_database,
    source_tables,
    target_host,
    target_user,
    target_password,
    target_database,
    batch_size=10000,
    archive_days=30,
):
    """
    從一個資料庫主機讀取多個資料表並歸檔到另一個資料庫主機，以批次方式處理資料，並進行資料驗證

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_

def archive_table_with_batching_and_verification(
    source_host,
    source_user,
    source_password,
    source_database,
    target_host,
    target_user,
    target_password,
    target_database,
    table_name,
    batch_size=10000,
    archive_days=30,
):
    """
    從一個資料庫主機讀取資料表並歸檔到另一個資料庫主機，以批次方式處理資料，並進行資料驗證

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_password (str): 來源資料庫密碼
        source_database (str): 來源資料庫名稱
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        table_name (str): 要歸檔的資料表名稱
        batch_size (int, optional): 每批資料的行數，預設為 10000
        archive_days (int, optional): 歸檔資料的日期條件，以天數為單位，預設為 30
    """

    # 獲取歸檔日期條件
    archive_date = datetime.datetime.now() - datetime.timedelta(days=archive_days)

    # 建立目標歸檔資料表
    create_archived_table(target_host, target_user, target_password, target_database, table_name)

    # 匯出來源資料
    source_connection = pymysql.connect(
        host=source_host,
        user=source_user,
        password=source_password,
        database=source_database,
    )
    with source_connection.cursor() as cursor:
        # 獲取資料表欄位名稱
        cursor.execute(f"DESCRIBE {source_database}.{table_name}")
        column_names = [column[0] for column in cursor.fetchall()]

        # 逐批匯出資料
        offset = 0
        while True:
            limit_clause = f"LIMIT {offset}, {batch_size}" if batch_size else ""
            query = f"""
            SELECT * FROM {source_database}.{table_name}
            WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'
            {limit_clause}
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                break

            # 匯入目標資料庫
            target_connection = pymysql.connect(
                host=target_host,
                user=target_user,
                password=target_password,
                database=target_database,
            )
            with target_connection.cursor() as cursor:
                cursor.executemany(
                    f"INSERT INTO {target_database}.{table_name}_archived ({','.join(column_names)}) VALUES (%s, %s, ...)",
                    results,
                )
            target_connection.commit()

            offset += batch_size

    # 刪除來源資料
    with source_connection.cursor() as cursor:
        query = f"""
        DELETE FROM {source_database}.{table_name}
        WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'
        """
        cursor.execute(query)
    source_connection.commit()
def archive_tables(
    source_host,
    source_user,
    source_password,
    source_database,
    source_tables,
    target_host,
    target_user,
    target_password,
    target_database,
    batch_size=10000,
    archive_days=30,
):
    """
    從一個資料庫主機讀取多個資料表並歸檔到另一個資料庫主機，以批次方式處理資料，並進行資料驗證

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_password (str): 來源資料庫密碼
        source_database (str): 來源資料庫名稱
        source_tables (List[str]): 要歸檔的資料表名稱列表
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        batch_size (int, optional): 每批資料的行數，預設為 10000
        archive_days (int, optional): 歸檔資料的日期條件，以天數為單位，預設為 30
    """

    for table_name in source_tables:
        archive_table_with_batching_and_verification(
            source_host,
            source_user,
            source_password,
            source_database,
            target_host,
            target_user,
            target_password,
            target_database,
            table_name,
            batch_size,
            archive_days,
        )


import hashlib


def get_md5_checksum(data):
    """
    計算資料的 MD5 校驗碼

    Args:
        data (bytes or str): 要計算 MD5 校驗碼的資料

    Returns:
        str: MD5 校驗碼
    """

    if isinstance(data, str):
        data = data.encode("utf-8")

    md5_hash = hashlib.md5()
    md5_hash.update(data)
    return md5_hash.hexdigest()

