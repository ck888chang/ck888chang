
#!/bin/bash

# MySQL 連線設定
DB_USER="your_mysql_user"
DB_PASSWORD="your_mysql_password"
DB_NAME="your_database_name"
TABLE_NAME="your_table_name"

# 匯出資料夾
EXPORT_DIR="./exports"
mkdir -p $EXPORT_DIR

# 匯出日期範圍 (2024年)
START_DATE="2024-09-01"
END_DATE="2024-09-30"

# MySQL 查詢最小和最大ID的函數
get_min_max_id() {
    local start_time=$1
    local end_time=$2

    # 查詢當天 created_at 在指定時間範圍內的最小ID和最大ID
    query="SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM $TABLE_NAME WHERE created_at >= '$start_time' AND created_at < '$end_time';"
    result=$(mysql -u $DB_USER -p$DB_PASSWORD -e "$query" $DB_NAME)

    # 解析結果，提取 min_id 和 max_id
    min_id=$(echo "$result" | awk 'NR==2 {print $1}')
    max_id=$(echo "$result" | awk 'NR==2 {print $2}')

    echo "$min_id $max_id"
}

# 從 START_DATE 循環到 END_DATE
CURRENT_DATE=$START_DATE

while [[ "$CURRENT_DATE" != $(date -I -d "$END_DATE + 1 day") ]]; do
    # 定義當天的起始和結束時間
    START_TIME="$CURRENT_DATE 00:00:00"
    NEXT_DATE=$(date -I -d "$CURRENT_DATE + 1 day")
    END_TIME="$NEXT_DATE 00:00:00"

    # 查詢當天的最小和最大ID
    read MIN_ID MAX_ID <<< $(get_min_max_id "$START_TIME" "$END_TIME")

    # 如果查詢到的ID範圍是空的，跳過當天的匯出
    if [[ -z "$MIN_ID" || -z "$MAX_ID" ]]; then
        echo "在 $CURRENT_DATE 沒有數據，跳過匯出。"
    else
        # 格式化壓縮檔名 (直接輸出為 .sql.gz)
        OUTPUT_FILE="$EXPORT_DIR/${TABLE_NAME}_$CURRENT_DATE.sql.gz"

        # 使用 mysqldump 根據 ID 和 created_at 條件進行匯出
        echo "匯出並壓縮 $CURRENT_DATE 的數據 (ID: $MIN_ID-$MAX_ID) 到 $OUTPUT_FILE"
        mysqldump -u $DB_USER -p$DB_PASSWORD $DB_NAME $TABLE_NAME \
            --where="id >= $MIN_ID AND id <= $MAX_ID AND created_at >= '$START_TIME' AND created_at < '$END_TIME'" | gzip > $OUTPUT_FILE
    fi

    # 移動到下一天
    CURRENT_DATE=$NEXT_DATE
done

echo "匯出並壓縮完成！"