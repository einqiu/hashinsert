#import HashRing
#host = ['192.168.0.11','192.168.0.33','192.168.0.44']
#ring = HashRing.HashRing(host,1000)
#server = ring.get_node_pos('5')
#print server[1]

'''import MySQLdb
try:
    conn = MySQLdb.connect(host='strugoogle1',user='root',passwd='123456',db='weibo',port=3306)
except Exception ,e:
    print e
cursor = conn.cursor()
sql_1 = "CREATE TABLE  IF NOT EXISTS USER%s (uid BIGINT UNSIGNED NOT NULL,bi_followers_count INT NOT NULL,PRIMARY KEY(uid)) DEFAULT CHARSET=UTF8"
value1=[1039]
cursor.execute(sql_1,value1)
sql_2="INSERT INTO USER%s VALUES(%s,%s)"
value2=[1039,3941630908,1]
cursor.execute(sql_2,value2)
print 'ok' '''
'''import hash_insert_oob
oob = hash_insert_oob.hash_insert_oob([{'uid':'3941630908','bi_followers_count':'22'},{'uid':'3948906508','bi_followers_count':'22'},{'uid':'3912345608','bi_followers_count':'22'},{'uid':'39415690908','bi_followers_count':'23'},{'uid':'7659630458','bi_followers_count':'22'},{'uid':'3956743908','bi_followers_count':'108'},{'uid':'9874330908','bi_followers_count':'220'},{'uid':'3978963908','bi_followers_count':'45'}])
oob.start_insert()'''
'''import hash_insert_oob
oob=hash_insert_oob.hash_insert_oob()
oob.start_insert({'uid':'3941630908','bi_followers_count':'22'})"""'''

import  MyInsert
import time
#from MyInsert import Q
file = open('user.txt','r')
alllines = file.readlines()
listdata = []
for eachline in alllines:
    mydict = eval(eachline)
    listdata.append(mydict)

assign= MyInsert.Assign_data(listdata)
assign.start()
insert0=MyInsert.Insert_data(0)
insert0.start()
insert1=MyInsert.Insert_data(1)
insert1.start()
insert2=MyInsert.Insert_data(2)
insert2.start()

