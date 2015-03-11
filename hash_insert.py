# coding = utf-8
count= 0
mydict = {}
N = 3
conn = []
host = ['223.3.41.18','223.3.39.17','223.3.40.197']
import sys
import string
import HashRing
import MySQLdb
#Node is 3.Virtual node is 3000/Node
ring = HashRing.HashRing(host,1000)
#Build the connection with the three hosts
for i in range(0,N):
    try:
        conn_temp=MySQLdb.connect(host=host[i],user='root',passwd='123456',db='weibo',port=3306,charset='utf8',use_unicode=True)
        conn.append(conn_temp)
    except Exception,e:
        print e
#open the data file 
file = open('user.txt','r')
alllines = file.readlines()
#insert line by line
for eachline in alllines:
    count = count+1
    mydict = eval(eachline)
    uid = mydict['uid']
    int_uid = string.atoi(str(uid))
    server = ring.get_node_pos(uid)
    host_temp = server[0]
    pos = server[1]
    index_temp = host.index(host_temp)
    if conn[index_temp]:
         cursor = conn[index_temp].cursor()
         sql_1 = "CREATE TABLE  IF NOT EXISTS USER%s (uid BIGINT NOT NULL,bi_followers_count INT NOT NULL,PRIMARY KEY(uid)) DEFAULT CHARSET=UTF8"
         value1=pos
         cursor.execute(sql_1,value1)
         sql_2="INSERT INTO USER%s VALUES(%s,%s)"
         int_bi_followers_count=string.atoi(str(mydict['bi_followers_count']))
         value2=(pos,int_uid,int_bi_followers_count)
         cursor.execute(sql_2,value2)
         print  "one insert is ok",'%d' %(count)
#close the connection
for i in range(0,N):
    conn[i].close()
