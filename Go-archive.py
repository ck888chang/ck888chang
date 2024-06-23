import datetime
import hashlib
import os
import subprocess

def archive_tables_with_batching_and_verification(
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

    Returns:
        None
    """

    # 檢查來源和目標資料庫是否存在
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

    if source_database not in source_databases:
        raise Exception(f"Source database '{source_database}' does not exist")

    if target_database not in target_databases:
        raise Exception(f"Target database '{target_database}' does not exist")

    # 逐個表進行歸檔
    for table_name in source_tables:
        # 建立歸檔資料表名稱
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
                    f"CREATE TABLE {target_database}.{archived_table_name} LIKE {source_database}.{table_name}",
                ]
            )

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
