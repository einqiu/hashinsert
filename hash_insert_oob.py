# coding = utf-8
N = 3
host = ['223.3.41.18','223.3.39.17','223.3.40.197']
conn = []
import MySQLdb
import HashRing
import sys
import string
import thread
class hash_insert_oob(object):

    def __init__(self, data_to_insert=[]):
        '''Estable the connect of the three hosts,and get the list to insert to database
        'data_to_insert' is the data to insert
        'length' is the length of the list
        'ring' is the object of the HashRing'''
        self.data_to_insert = data_to_insert
        self.length=len(data_to_insert)
        assert self.length>=1
        for i in range(0,N):
            try:
                conn_temp=MySQLdb.connect(host=host[i],user='root',passwd='123456',db='weibo',port=3306,charset='utf8',use_unicode=True)
                conn.append(conn_temp)
            except Exception,e:
                print e
        self.ring=HashRing.HashRing(host,1000)
    def decide_host(self,mydict={}):
        '''Accoring to the mydict, the host as the database server can be decided 
        'uid' is the uid from the dict and 'int_uid' is the cast to int,
        'server' is the hash postion of the dict,
        'host_temp' is the host,
        'pos' is the table number'''
        uid = mydict['uid']
        int_uid=string.atoi(str(uid))
        server = self.ring.get_node_pos(uid)
        host_temp = server[0]
        pos = server[1]
        index_temp = host.index(host_temp)
        return index_temp,pos
    def assign_data(self):
        '''Assgin the data_to_insert to the apporiate list,
        valuess[0] is the first host,and valuess[1] is the second host,and valuess[2] is the third host'''
        valuess=[[] for i in range(3)]
        for i in range(0,self.length):
            temp_dict=self.data_to_insert[i]
            index_pos=self.decide_host(temp_dict)
            k=index_pos[0]
            pos=index_pos[1]
            valuess[k].append([pos,temp_dict['uid'],temp_dict['bi_followers_count']])
        return valuess

    def insert_data_0(self,value=[]):
        if conn[0]:
            length_0=len(value)
            cursor = conn[0].cursor()
            for i in range(length_0):
                sql_1="CREATE TABLE  IF NOT EXISTS USER%s (uid BIGINT NOT NULL,bi_followers_count INT NOT NULL,PRIMARY KEY(uid)) DEFAULT CHARSET=UTF8"
                position=value[i][0]
                cursor.execute(sql_1,position)
                sql_2="INSERT INTO USER%s VALUES(%s,%s)"
                int_uid=string.atoi(str(value[i][1]))
                int_bi_followers_count=string.atoi(str(value[i][2]))
                cursor.execute(sql_2,[position,int_uid,int_bi_followers_count])
    def insert_data_1(self,value=[]):
        if conn[1]:
            length_1=len(value)
            cursor = conn[1].cursor()
            for i in range(length_1):
                sql_1="CREATE TABLE  IF NOT EXISTS USER%s (uid BIGINT NOT NULL,bi_followers_count INT NOT NULL,PRIMARY KEY(uid)) DEFAULT CHARSET=UTF8"
                position=value[i][0]
                cursor.execute(sql_1,position)
                sql_2="INSERT INTO USER%s VALUES(%s,%s)"
                int_uid=string.atoi(str(value[i][1]))
                int_bi_followers_count=string.atoi(str(value[i][2]))
                cursor.execute(sql_2,[position,int_uid,int_bi_followers_count])
    def insert_data_2(self,value=[]):
        if conn[2]:
            length_2=len(value)
            cursor = conn[2].cursor()
            for i in range(length_2):
                sql_1="CREATE TABLE  IF NOT EXISTS USER%s (uid BIGINT NOT NULL,bi_followers_count INT NOT NULL,PRIMARY KEY(uid)) DEFAULT CHARSET=UTF8"
                position=value[i][0]
                cursor.execute(sql_1,position)
                sql_2="INSERT INTO USER%s VALUES(%s,%s)"
                int_uid=string.atoi(str(value[i][1]))
                int_bi_followers_count=string.atoi(str(value[i][2]))
                cursor.execute(sql_2,[position,int_uid,int_bi_followers_count])
    def start_insert(self):
        assign_value = self.assign_data()
        thread.start_new_thread(self.insert_data_0,(assign_value[0],))
        thread.start_new_thread(self.insert_data_1,(assign_value[1],))
        thread.start_new_thread(self.insert_data_2,(assign_value[2],))

        

      