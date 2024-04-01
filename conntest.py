import time
import sys
import pymysql
import csv
import codecs
import schedule
import requests
import os
from skpy import Skype, SkypeChats
import colorama
from colorama import Fore
from colorama import Style
import datetime
#import pdb
#   pdb.set_trace()   ##
csv.field_size_limit(sys.maxsize)

def LogFileMSG(level, WriteLogMessage):
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




def get_conn():
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='6d', db='testdb', charset='utf8',compress=None)
    #conn = pymysql.connect(host='database-1.ckht8fbw26hy.ap-northeast-1.rds.amazonaws.com', port=3306, user='admin', passwd='6d', db='innodb', charset='utf8',compress=None)
    return conn



def insert(cur, sql, args):
    # print(sql)
    # print(str(args)[1:][:-1])
    # execsql = sql + str(args)[1:][:-1]
    # print (args)
    # cur.execute(execsql)
    cur.execute(sql, args)


def multiinsert(conn, cur, sql, args):
    start = time.time()
    try:
        cur.executemany(sql, args=args)
        conn.commit()
    except:
        print("Happen Some error")
        conn.rollback()
    end = time.time()
    print("Insert execute time ", end - start, "second")



def query_all(cur, sql, args):
    start = time.time()
    #print(sql)
    try:
        cur.execute(sql, args)
    except:
        print("Can't query data")
    end = time.time()
    print("Query execute time ", end - start, "second")
    return cur.fetchall()


def mysqlvariables(cur):
    aa = (
        ('innodb_buffer_pool_size',4368709120,49392123904),('innodb_io_capacity_max',4000,16000),('innodb_io_capacity',500,8000),
        ('innodb_read_io_threads',8,64),('innodb_write_io_threads',8,32),('innodb_log_buffer_size',16777216,134217728),('innodb_flush_log_at_trx_commit',2,2),
        ('innodb_flush_log_at_timeout',2,5),('innodb_sort_buffer_size',67108864,67108864),('innodb_autoextend_increment',128,128),('innodb_log_file_size',268435456,536870912),
        ('net_buffer_length',524288,1048576),('tmp_table_size',134217728,268435456),('max_heap_table_size',134217728,268435456),
        ('read_buffer_size',1048576,16777216),('read_rnd_buffer_size',2097152,16777216),('sort_buffer_size',16777216,2147483648),('join_buffer_size',524288,16777216),
        ('bulk_insert_buffer_size',8388608,16777216),('max_seeks_for_key',1000,1000),('thread_cache_size',60,400),('wait_timeout',180,3600),('interactive_timeout',180,3600),('binlog_cache_size',524288,5242880),('sync_binlog',2,5))
        ### IRU
    for x in aa:
        #print("SHOW VARIABLES like '%s'"% (x[0]))
        queryvariable = ("SHOW VARIABLES like '%s'"% (x[0]))
        results = queryvariables(cur=cur, sql=queryvariable, args=None)
        #print(results[0][1])
        if x[1] <= int(results[0][1]) <= x[2]:
            print(Fore.GREEN + "SHOW VARIABLES like '%s'; ###  %s IS between %s and %s" % (x[0], results[0][1], x[1], x[2]) + Style.RESET_ALL)
            #print(Fore.BLUE + Style.BRIGHT + "This is the color of the sky" + Style.RESET_ALL)
            #print("OK variables")
        else:
            print(Fore.RED + "set GLOBAL %s = %s ;  ### NO Good variables %s" % (x[0], x[1], results[0][1]) + Style.RESET_ALL)


def Mvariables():
    hostip = (('CKDocker','localhost','root','d','testdb'),
              ('fatpitProduction', 'fatpit-st-001.cpvuvttgymsf.ap-northeast-1.rds.amazonaws.com', 'fatpit_pro','04', 'fatpit_pro'),   )

for x in hostip:
        #print(x[0], x[1], x[2], x[3], x[4])
        print(x[0])
        try:
            dbconn = pymysql.connect(host=x[1], port=3306,user=x[2], passwd=x[3], db=x[4], charset='utf8', compress=None)
        except pymysql.err.OperationalError as Access_denied:
            print(Access_denied.__str__())
        dbcur = dbconn.cursor()
        mysqlvariables(dbcur)
        dbcur.close()
        dbconn.close()
        print("#" * 90)
        #break
        #s_conn = pymysql.connect(host='fatgame-slave-003.cpvuvttgymsf.ap-northeast-1.rds.amazonaws.com', port=3306,
        #user='readonly_fatgame', passwd='', db='fatgame', charset='utf8', compress=None)




if __name__ == '__main__':
    #colorama.init()
    # read_csv_to_mysql('/Users/ck/Downloads/slowlog_2022-12-19.csv')
    # read_mysql_to_csv('2.csv')
    # readcsv()
    #mysql_to_mysql('bet_logs')
    #timercontroller()
    #skype_message("GGGGGGGGGGGGG")
    Mvariables()
    #updatecolumn()
    #runtestsql()
    #partition()
