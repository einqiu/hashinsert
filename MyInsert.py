# Filename: MyInsert.py
# @author :Strugoogle
# Created on 2015-3-16
N = 3
host = ['223.3.41.18','223.3.39.17','223.3.40.197']
conn = []
import threading
import MySQLdb
import HashRing
import Queue
import string
from time import sleep
encoding = 'utf-8'
N = 3
Q = []
for j in range(N):
    Q.append(Queue.Queue())
conn=[]
class Assign_data(threading.Thread):
    def __init__(self,listdata):
        threading.Thread.__init__(self)
        self.listdata = listdata
        for i in range(0,N):
            try:
                conn_temp=MySQLdb.connect(host=host[i],user='root',passwd='123456',db='weibo',port=3306,charset='utf8',use_unicode=True)
                conn.append(conn_temp)
            except Exception,e:
                print e
        self.ring=HashRing.HashRing(host,1000)
    def decide_host(self,mydict={}):
        uid = mydict['uid']
        int_uid=string.atoi(str(uid))
        server = self.ring.get_node_pos(uid)
        host_temp = server[0]
        pos = server[1]
        index_temp = host.index(host_temp)
        return index_temp,pos
    def run(self):
        global Q
        length = len(self.listdata)
        for i in range(length):
            mydict = self.listdata[i]
            indexandpos = self.decide_host(mydict)
            Q[indexandpos[0]].put([indexandpos[1],mydict])
            #print Q[indexandpos[0]].qsize()
            


class Insert_data(threading.Thread):
    def __init__(self,i=0):
        threading.Thread.__init__(self)
        self.i=i
    def run(self):
        global Q
        if conn[self.i] :
            cursor = conn[self.i].cursor()
            while True:
                mytempdictlist = Q[self.i].get()
                position = mytempdictlist[0]
                mytempdict = mytempdictlist[1]
                sql_1="CREATE TABLE  IF NOT EXISTS USER%s (time DATETIME NOT NULL,uid BIGINT NOT NULL,bi_followers_count INT NOT NULL,\
                city SMALLINT,verified SMALLINT,followers_count INT,province SMALLINT,description VARCHAR(200),screen_name VARCHAR(100),\
                gender VARCHAR(1),created_at DATETIME, \
                PRIMARY KEY(uid)) DEFAULT CHARSET=UTF8"
                cursor.execute(sql_1,position)
                int_uid=string.atoi(str(mytempdict['uid']))
                int_bi_followers_count=string.atoi(str(mytempdict['bi_followers_count']))
                int_city=string.atoi(str(mytempdict['city']))
                int_verified=string.atoi(str(mytempdict['verified']))
                int_followers_count=string.atoi(str(mytempdict['followers_count']))
                int_province=string.atoi(str(mytempdict['province']))
                var_description=mytempdict['description']
                var_screen_name=mytempdict['screen_name']
                var_gender=mytempdict['gender']
                created_at=mytempdict['created_at']
                sql_2="INSERT INTO USER%s VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql_2,[position,int_uid,int_bi_followers_count,int_city,int_verified, int_followers_count,int_province,var_description,var_screen_name,var_gender,created_at])
                print 'OK'
                sleep(20)
          
                