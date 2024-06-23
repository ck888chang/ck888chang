
import datetime
import hashlib
import os
import subprocess


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

    # 檢查來源資料庫是否存在
    subprocess.run(
        [
            "tidb-ctl",
            "--host",
            source_host,
            "--port",
            "4000",
            "query",
            "SHOW DATABASES",
        ],
        capture_output=True,
        check=False,
    )
    source_databases = subprocess.getoutput(command).split("\n")

    if source_database not in source_databases:
        raise Exception(f"Source database '{source_database}' does not exist")

    # 檢查目標資料庫是否存在
    subprocess.run(
        [
            "tidb-ctl",
            "--host",
            target_host,
            "--port",
            "4000",
            "query",
            "SHOW DATABASES",
        ],
        capture_output=True,
        check=False,
    )
    target_databases = subprocess.getoutput(command).split("\n")

    if target_database not in target_databases:
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

    # 檢查目標資料庫中是否存在歸檔資料表
    subprocess.run(
        [
            "tidb-ctl",
            "--host",
            target_host,
            "--port",
            "4000",
            "query",
            f"SHOW CREATE TABLE {target_database}.{archived_table_name}",
        ],
        capture_output=True,
        check=False,
    )
    output = subprocess.getoutput(command)

    if "Table `{}` doesn't exist".format(archived_table_name) in output:
        # 建立歸檔資料表
        subprocess.run(
            [
                "tidb-ctl",
                "--host",
                target_host,
                "--port",
                "4000",
                "query",
                f"CREATE TABLE {target_database}.{archived_table_name} LIKE {target_database}.{table_name}",
            ]
        )


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

    Returns:
        None
    """

    # 獲取歸檔日期條件
    archive_date = datetime.datetime.now() - datetime.timedelta(days=archive_days)

    # 逐批匯出和匯入資料
    offset = 0
    while True:
        # 獲取下一批資料
        source_data = subprocess.run(
            [
                "tidb-dump",
                "--host",
                source_host,
                "--port",
                "4000",
                "--user",
                source_user,
                "--password",
                source_password,
                "--database",
                source_database,
                "--table",
                table_name,
                "--where",
                f"create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'",
                "--limit",
                str(batch_size),
                "--result-file",
                "-",
            ],
            capture_output=True,
            check=False,
        )
        source_data_output = source_data.stdout.decode("utf-8")

        if not source_data_output:
            # 已無資料可供匯出
            break

        # 匯入資料到目標資料庫
        subprocess.run(
            [
                "tidb-loader",
                "--host",
                target_host,
                "--port",
                "4000",
                "--user",
                target_user,
                "--password",
                target_password,
                "--database",
                target_database,
                "--table",
                f"{target_database}.{table_name}_archived",
                "--data",
                source_data_output,
            ],
            check=False,
        )

        # 驗證資料
        verify_data(
            source_host,
            source_user,
            source_password,
            source_database,
            table_name,
            target_host,
            target_user,
            target_password,
            target_database,
            f"{target_database}.{table_name}_archived",
            archive_date,
        )

        # 更新偏移量
        offset += batch_size

    # 刪除原始資料
    subprocess.run(
        [
            "tidb-query",
            "--host",
            source_host,
            "--port",
            "4000",
            "--user",
            source_user,
            "--password",
            source_password,
            "--database",
            source_database,
            "--query",
            f"DELETE FROM {source_database}.{table_name} WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'",
        ],
        check=False,
    )

def verify_data(
    source_host,
    source_user,
    source_password,
    source_database,
    source_table_name,
    target_host,
    target_user,
    target_password,
    target_database,
    target_table_name,
    archive_date,
):
    """
    驗證來源資料和目標資料的資料一致性

    Args:
        source_host (str): 來源資料庫主機位址
        source_user (str): 來源資料庫使用者名稱
        source_password (str): 來源資料庫密碼
        source_database (str): 來源資料庫名稱
        source_table_name (str): 來源資料表名稱
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        target_table_name (str): 目標資料表名稱
        archive_date (datetime.datetime): 歸檔日期條件
    """

    # 計算來源資料的 MD5 校驗碼
    source_data_md5 = subprocess.run(
        [
            "tidb-query",
            "--host",
            source_host,
            "--port",
            "4000",
            "--user",
            source_user,
            "--password",
            source_password,
            "--database",
            source_database,
            "--query",
            f"SELECT MD5(HEX(CONCAT_WS(',', *))) FROM {source_database}.{source_table_name} WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'",
        ],
        capture_output=True,
        check=False,
    )
    source_data_md5 = source_data_md5.stdout.decode("utf-8").strip()

    # 計算目標資料的 MD5 校驗碼
    target_data_md5 = subprocess.run(
        [
            "tidb-query",
            "--host",
            target_host,
            "--port",
            "4000",
            "--user",
            target_user,
            "--password",
            target_password,
            "--database",
            target_database,
            "--query",
            f"SELECT MD5(HEX(CONCAT_WS(',', *))) FROM {target_database}.{target_table_name} WHERE create_time < '{archive_date.strftime("%Y-%m-%d %H:%M:%S")}'",
        ],
        capture_output=True,
        check=False,
    )
    target_data_md5 = target_data_md5.stdout.decode("utf-8").strip()

    if source_data_md5 != target_data_md5:
        raise Exception(f"Data verification failed: source data MD5 ({source_data_md5}) != target data MD5 ({target_data_md5})")


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
        source_tables (list): 要歸檔的資料表名稱列表
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        batch_size (int, optional): 每批資料的行

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
        source_tables (list): 要歸檔的資料表名稱列表
        target_host (str): 目標資料庫主機位址
        target_user (str): 目標資料庫使用者名稱
        target_password (str): 目標資料庫密碼
        target_database (str): 目標資料庫名稱
        batch_size (int, optional): 每批資料的行數，預設為 10000
        archive_days (int, optional): 歸檔資料的日期條件，以天數為單位，預設為 30
    """

    for table_name in source_tables:
        print(f"Archiving table '{table_name}'...")
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

