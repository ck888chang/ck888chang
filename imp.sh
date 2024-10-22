uuuuh#!/bin/bash

# MySQL 連線設定
DB_USER="your_mysql_user"
DB_PASSWORD="your_mysql_password"
DB_NAME="your_database_name"

# 匯出檔案的資料夾
EXPORT_DIR="./exports"

# 檢查匯出的資料夾是否存在
if [ ! -d "$EXPORT_DIR" ]; then
    echo "匯出的資料夾 $EXPORT_DIR 不存在！"
    exit 1
fi

# 列出所有的 .sql.gz 檔案並按日期順序處理
for FILE in $(ls $EXPORT_DIR/*.sql.gz | sort); do
    echo "正在匯入 $FILE 到資料庫 $DB_NAME..."

    # 解壓並將內容匯入 MySQL
    gunzip -c $FILE | mysql -u $DB_USER -p$DB_PASSWORD $DB_NAME

    if [ $? -eq 0 ]; then
        echo "$FILE 匯入成功。"
    else
        echo "匯入 $FILE 時發生錯誤！"
        exit 1
    fi
done

echo "所有匯入完成！"