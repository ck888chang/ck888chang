import time
import mysql.connector
from colorama import Fore
from colorama import Style

def LogMSG(level, WriteLogMessage):
    # print(WriteLogMessage)
    logfilename = time.strftime("Log%Y%m%d.txt", time.localtime())
    logtime1 = time.strftime("%Y/%m/%d %H:%M:%S ", time.localtime())
    MsgLevel = ("INFO ", "WARN ", "ERROR ", "FATAL ", "DEBUG ")
    try:
        logf = open(logfilename, 'a')
        logf.write(logtime1 + '_' + MsgLevel[level] + '_' + WriteLogMessage + '\n')
        logf.close()
    except FileNotFoundError:
        print("FileNotFound")
        logf = open(logfilename, "w")
        logf.write(logtime1 + '_' + MsgLevel[level] + '_' + WriteLogMessage + '\n')
        logf.close()
    except IsADirectoryError:
        print("IsADirectory")

def get_conn(conn_type):
    # 設定連接到資料庫的相關參數
    if conn_type=="master":
        config = {
            'user': 'readonly_fatgame',
            'password': 'fK72Od/yFhYprwt@i8zQV:43',
            'host': 'fatgame.cpvuvttgymsf.ap-northeast-1.rds.amazonaws.com',
            'database': 'fatgame',
            'charset': 'utf8',
            'port': 3306,
            'compress': 'zlib'
        }
    elif conn_type == "slave":
        config = {
            'user': 'readonly_fatgame',
            'password': 'fK72Od/yFhYprwt@i8zQV:43',
            'host': 'fatgame-slave-003.cpvuvttgymsf.ap-northeast-1.rds.amazonaws.com',
            'database': 'fatgame',
            'charset': 'utf8',
            'port': 3306,
            'compress': 'zlib'
        }
    elif conn_type == "test":
        config = {
            'user': 'root',
            'password': '7taw996d',
            'host': 'localhost',
            'database': 'testdb',
            'charset': 'utf8',
            'port': 3306,
            'compress': 'zlib'
        }
    # 連接到資料庫
    conn = mysql.connector.connect(**config)
    return conn

def query_all(cur, sql, args):
    start3 = time.time()
    #print(sql)
    try:
        cur.execute(sql, args)
    except:
        print("Can't query data")
    results = cur.fetchall()
    end3 = time.time()
    print("Query execute time ", end3 - start3, "second")
    return results

def queryminid(conn,tbname):
    cursor = conn.cursor()
    sql = 'select min(id) from %s' % (tbname)
    cursor.execute(sql)
    results = cursor.fetchone()[0]
    cursor.close()
    return results

def querymaxid(conn,tbname):
    cursor = conn.cursor()
    sql = 'select max(id) from %s' % (tbname)
    cursor.execute(sql)
    results = cursor.fetchone()[0]
    cursor.close()
    if results == None:
        return 0
    return results

def CompareID(tbname):
    m_conn = get_conn("master")
    s_conn = get_conn("slave")
    MaterMinID = queryminid(m_conn, tbname)
    SlaveMinID = queryminid(s_conn, tbname)
    print(MaterMinID)
    print(SlaveMinID)
    m_conn.close()
    s_conn.close()
    tmpid=MaterMinID
    for i in range(0,10,1):
        m_conn = get_conn("master")
        s_conn = get_conn("slave")
        SlaveMaxID = querymaxid(s_conn, tbname)
        MaterMaxID = querymaxid(m_conn, tbname)
        print("MaterMaxID", MaterMaxID)
        print("SlaveMaxID", SlaveMaxID)
        print(SlaveMaxID - MaterMaxID)
        m_conn.close()
        s_conn.close()
        time.sleep(3)
        print("每三秒鐘產生多少筆資料：",MaterMaxID - tmpid)
        tmpid=MaterMaxID

def jumpnumber():
    #conn = get_conn("slave")
    conn = get_conn("test")
    cursor = conn.cursor()

    query = "select table_schema, table_name, partition_name,partition_method, partition_description, table_rows from information_schema.partitions where table_schema = 'fatgame' and table_name = 'bet_logs' and table_rows > 0 order by partition_name desc limit 1"
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    if len(results) > 0:
        print(results[0][2])
        query = "SELECT max(id),min(id) FROM bet_logs PARTITION ({}) ORDER BY id".format(results[0][2])
        Partition = 'yes'
        PartitionName = results[0][2]
    else:
        print(results)
        Partition = 'No'
        query = "SELECT max(id),min(id) FROM bet_logs ORDER BY id"
    #print(query)
    results = query_all(cursor,query,None)
    #print(results)
    maxid = results[0][0]
    minid = results[0][1]
    print("最大號碼{}，最小號碼{}，二號相差{}".format(maxid, minid , maxid - minid))

    timesA = 0
    sumA = 0
    start5 = time.time()
    for xround in range(minid, maxid, 1000000000):
        if xround == minid:
            continue
        else:
            print(minid, xround, maxid)
            if Partition == 'yes':
                query = "SELECT ID FROM bet_logs PARTITION ({}) where ID BETWEEN {} AND {} ORDER BY id".format(PartitionName, minid, xround)
            else:
                query = "SELECT ID FROM bet_logs where ID BETWEEN {} AND {} ORDER BY id".format(minid, xround)
            print(query)
            results = query_all(cursor, query, None)
            print("得到筆數", len(results))
            for i, (current_id,) in enumerate(results[:-1]):
                next_id = results[i + 1][0]
                if next_id - current_id > 1:
                    print("The gap between id {} and id {} is {}".format(current_id, next_id, next_id - current_id - 1))
                    timesA += 1
                    sumA = sumA + (next_id - current_id - 1)
        minid = xround
    print("timeer")
    #time.sleep(30)
    # 檢查結果中是否有跳號的情況

    print("共有 {} 次的 ID 欄位有跳號！".format(timesA))
    print("共有 {} 行的 ID 欄位有跳號！".format(sumA))
    # 關閉連接
    cursor.close()
    conn.close()
    end5 = time.time()
    print("Query execute time ", end5 - start5, "second")


def getPartitionName(conn):
    cursor = conn.cursor()
    #query = "select  partition_name from information_schema.partitions where table_schema = 'testdb' and table_name = 'bet_logs' and table_rows > 1; "
    query = "select  partition_name from information_schema.partitions where table_schema = 'fatgame' and table_name = 'bet_logs' and table_rows > 1; "
    cursor.execute(query)
    results = cursor.fetchall()
    #print(results)
    cursor.close()
    return results

def getLostIDcreateAt(conn, current_id, next_id):
    cursor = conn.cursor()
    query1 = "select id,bet_at,settled_at,created_at,updated_at from bet_logs where ID between {} and {} ".format(current_id, next_id)
    query2 = "select count(id) from bet_logs where ID between {} and {} ".format(current_id, next_id)
    print(query1)
    LogMSG(1,query1)
    cursor.execute(query2)
    results = cursor.fetchall()
    print(results)
    cursor.close()
    return results


def jumpnumber2():
    conn = get_conn("slave")
    #conn = get_conn("test")

    PartLoop = getPartitionName(conn)
    start = time.time()
    for Loopx in PartLoop:
        print(Loopx[0])
        query = "SELECT ID FROM bet_logs PARTITION ({}) ORDER BY id".format(Loopx[0])
        #print(query)
        timesA = 0
        sumA = 0

        cursor = conn.cursor()
        results = query_all(cursor, query, None)
        print("得到筆數", len(results))
        for i, (current_id,) in enumerate(results[:-1]):
            next_id = results[i + 1][0]
            if next_id - current_id > 1:
                lostID = next_id - current_id - 1
                Gapmsg = "The gap between id {} and id {} is {}".format(current_id, next_id, lostID)
                if lostID > 10000:
                    print(Fore.RED + Gapmsg + Style.RESET_ALL)
                    getLostIDcreateAt(conn, current_id, next_id)
                else:
                    print(Fore.GREEN + Gapmsg + Style.RESET_ALL)
                LogMSG(1,Gapmsg)
                timesA += 1
                sumA = sumA + (next_id - current_id - 1)
        print("共有 {} 次的 ID 欄位有跳號！".format(timesA))
        LogMSG(1,"共有 {} 次的 ID 欄位有跳號！".format(timesA))
        print("共有 {} 行的 ID 欄位有跳號！".format(sumA))
        LogMSG(1,"共有 {} 行的 ID 欄位有跳號！".format(sumA))
        # 關閉連接
        cursor.close()
    conn.close()
    end = time.time()
    print("Query execute time ", end - start, "second")


def queryMany(conn, sql, args):
    cursor = conn.cursor()
    start3 = time.time()
    #print(sql)
    try:
        cursor.execute(sql, args)
    except:
        print("Can't query data")
    results = cursor.fetchall()
    cursor.close()
    end3 = time.time()
    #print("Query execute time ", end3 - start3, "second")
    return results

def getJumpRate():
    ##這段程式碼定義了一個 getParti 函數，用於查詢指定資料庫中 bet_logs 表格的分區狀態和分區內資料的 ID 范圍，然後計算出總共缺失的資料行數。
    ##具體來說，函數會建立一個 MySQL 連線（使用 get_conn 函數），然後使用 information_schema.partitions 資訊表查詢指定資料庫中 bet_logs 表格的所有分區名稱和資料行數。
    ## 接著，使用 for 迴圈遍歷每個分區，並根據分區名稱使用 min 和 max 函數查詢該分區內資料的 ID 範圍。最後，將總資料行數減去所有分區內資料行數的和，就是缺失的資料行數。
    ##這段程式碼可能還需要一些改進，例如加入錯誤處理和日誌記錄，以確保程式的穩定性和可維護性。
    ## 這段程式碼只有當天的partition有效，因為partition使用bet_at條件切，回補資料時可能將比較大的id填在昨天或前天的partition裡面。
    conn = get_conn("slave")
    #conn = get_conn("test")

    #query = "select  partition_name,table_rows from information_schema.partitions where table_schema = 'testdb' and table_name = 'bet_logs' and table_rows > 1 order by 1 desc  limit 1 ; "
    query = "select  partition_name,table_rows from information_schema.partitions where table_schema = 'fatgame' and table_name = 'bet_logs' and table_rows > 1 order by 1 desc  limit 1 ; "
    results = queryMany(conn, query, None)
    print("results", results)
    for loopx in results:
        print("loopx[0]", loopx[0])
        query = "select min(ID),max(ID) from bet_logs PARTITION ({})".format(loopx[0])
        resultsx = queryMany(conn, query, None)
        print("最小id ，最大id", resultsx)
        print("資料庫裡總筆數", loopx[1])
        print("最大id-最小id 之差 (應有的筆數)", resultsx[0][1]-resultsx[0][0])
        query2 = "select count(ID) from bet_logs where ID between {} and {}".format(resultsx[0][0], resultsx[0][1])
        resultsx2 = queryMany(conn, query2, None)
        print("查資料庫實際筆數", resultsx2)
        print("資料庫總筆數 - 應有的筆數 ", (resultsx[0][1]-resultsx[0][0]) - loopx[1])
        print("實際筆數    - 應有的筆數 ", (resultsx[0][1] - resultsx[0][0]) - resultsx2[0][0])
        print("跳號的比率", ((resultsx[0][1] - resultsx[0][0]) - resultsx2[0][0]) / resultsx2[0][0])

if __name__ == '__main__':
    #print("先執行10次，比較master跟slave每3秒鐘有多少筆資料，差異多少？")
    #CompareID('bet_logs')
    #print("開始查詢資料庫相差異的筆數")
    #LogMSG(1,"開始查詢資料庫相差異的筆數")
    #jumpnumber2()
    getJumpRate()