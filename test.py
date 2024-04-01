import pymysql
import redshift_connector
###   pip install redshift_connector

def mysqlconnect():
    # Connect to MySQL
    cnx = pymysql.connect(host="jvdckrdsa.ckht8fbw26hy.ap-northeast-1.rds.amazonaws.com", port=3306, user="admin", passwd="7taw996d", db="fatpit_staging")
    cursor = cnx.cursor()

    # Query data from MySQL
    query = "SELECT * FROM rounds WHERE id <= 10"
    cursor.execute(query)
    rows = cursor.fetchall()
    for x in rows:
        print(x)
    cursor.close()
    cnx.close()

def Redshiftconnect():
    # Connect to Redshift
    conn = redshift_connector.connect(
         host='jvdckshift.210199513596.ap-northeast-1.redshift-serverless.amazonaws.com',
         database='dev',
         port=5439,
         user='admin',
         password='7taw996D'
      )
    cur = conn.cursor()

    cur.execute("select * from cktable")
    result: tuple = cursor.fetchall()
    print(result)
    # Write data to Redshift
    #for row in rows:
    #    cur.execute("INSERT INTO redshift_table (column1, column2, ...) VALUES (%s, %s, ...)", row)

    # Close connections
    cur.close()
    conn.close()

if __name__ == '__main__':
    print("connect to mysql")
    mysqlconnect()
    print("connect to mysql, finish!")
    print("connect to Redshift")
    Redshiftconnect()
    print("connect to Redshift, finish!")

##########
#### EC2 server
## JVDCKEC2A 172.30.0.22 (ec2-18-176-54-176.ap-northeast-1.compute.amazonaws.com)
## JVDCKEC2B 172.30.0.89 (ec2-18-183-176-33.ap-northeast-1.compute.amazonaws.com)
## JVDckrdsa 172.30.0.19 (54.168.65.10)
#  jvdckrdsa.ckht8fbw26hy.ap-northeast-1.rds.amazonaws.com
#  jvdckshift.210199513596.ap-northeast-1.redshift-serverless.amazonaws.com
#  172.30.2.84

##  jdbc:mysql://jvdckrdsa.ckht8fbw26hy.ap-northeast-1.rds.amazonaws.com:3306/fatpit_staging

## jdbc:redshift://jvdckshift.210199513596.ap-northeast-1.redshift-serverless.amazonaws.com:5439/dev?tcpKeepAlive=true