#!/usr/bin/env python
# -*- coding: utf8 -*-

from contextlib import contextmanager
import random  
import threading  
import time  
import MySQLdb
import getopt
import sys
import os
import datetime
from checkApp import ApplicationInstance
import ConfigParser
import endecrypt
from Queue import Queue
import re
import chardet
import subprocess

ITHREADS = 0
TASKQ = {}
U_ADMIN = ''
U_ADMIN_PWD = ''
U_SQLUPLOAD  = ''
U_SQLUPLOAD_PWD  = ''
QHOST = Queue()  
RUNNING_HOST = []
LOCK = None
db_host = '127.0.0.1'
db_port = 3306
db_name = 'sqlperfdb'

def Usage():
    print 'move.py usage:'
    print '-h,--help: print help message.'
    print '-v, --version:   print script version'
    print '-t, --thread:   input thread count'

def Version():
    print 'slowLog.py 1.0.0.0.1'

def OutPut(args):  
    print 'Hello, %s'%args

def getOpt():
    global ITHREADS
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hv:t:', ['help', 'version', 'thread='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('-t', '--thread'):
            ITHREADS = int(a)
 

 # 创建数据库连接           
@contextmanager
def get_conn(db_host=db_host,db_name=db_name,db_port=db_port):
    conn = MySQLdb.connect(host=db_host,user=U_ADMIN, passwd=U_ADMIN_PWD, db=db_name,port=db_port,charset='utf8mb4')
    try:
        yield conn
    finally:
        conn.close()

# 从sql语句中解析出表名
def get_table_name(sql_text):
    sql_lower = sql_text.lower()
    sql_list = sql_lower.split()
    if sql_list[0] == "insert":
        into_index = sql_list.index('into')
        table_name = sql_list[into_index + 1]
        table_name = table_name.split('(')[0].strip()
        table_name = check_and_remove_dbname(table_name)
    elif sql_list[0] == "update":
        table_name = sql_list[1]
        table_name = check_and_remove_dbname(table_name)
    elif sql_list[0] == 'select' or sql_list[0] == 'delete':
        sql_list.reverse()
        from_index = sql_list.index('from')
        table_name = sql_list[from_index - 1]
        table_name = check_and_remove_dbname(table_name)
    else:
        table_name = 'not_match'
    return table_name


# 根据table_name获取db_name
def get_db_name(table_name):
    sql = "select database_name from db_table where table_name = '{0}'".format(table_name)
    try:
        with get_conn(db_name='mapping') as conn:
            with conn as cur:
                cur.execute(sql)
                res = cur.fetchall()
        if res:
            return res[0][0]
        else:
            return "not_found_db"
    except MySQLdb.Warning, w:
        return "not_found_db"
    except MySQLdb.Error, e:
        return "not_found_db"
       
# 将有db_name.table_name的只返回table_name
def check_and_remove_dbname(strings):
    s = strings.replace('`','').split(".")
    if len(s) == 1:
        return s[0]
    elif len(s) == 2:
        return s[1]
    else:
        return "no_found_db"    

# 检查已发送并超过2周的sql记录并删除
def delete_expire_record(hash_code):
    sql = "delete from slowlog_sql WHERE hashcode='{0}' AND send_status=1 and datachange_lasttime < DATE_ADD(NOW(),INTERVAL -14 DAY)".format(hash_code)
    try:
        with get_conn() as conn:
            with conn as cur:
                cur.execute(sql)
    except MySQLdb.Warning, w:
        pass
    except MySQLdb.Error, e:
        print e
    
# 将符合条件的写入slowlog_sql
def writeToDB(hostip, hostname, loginUser, loginIp, begin_time, end_time, avgTime, totalTime, counts, rows, avgLocktime, hashcode ):
    sql = "insert slowlog_history(hostname, loginname, loginip, begin_time, end_time, query_time_avg, query_time_total, locktime_avg, counts, avgRows, hashcode) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    param = (hostname.replace(" ", "").replace("\n", ""), loginUser.replace(" ", "").replace("\n", ""), loginIp.replace(" ", "").replace("\n", ""), begin_time, end_time, avgTime, totalTime, avgLocktime, counts, rows, hashcode)
    try:
        with get_conn() as conn:
            with conn as cur:
                cur.execute(sql, param)
    except MySQLdb.Warning, w:
        pass
    except MySQLdb.Error, e:
        print e


# 将slowlog写入slowlog_history
def writeDirectorTB(hostip, hostname, loginUser, loginIp, sqltext, sourcesql, hashcode ):
    table_name = get_table_name(sqltext)
    db_name = get_db_name(table_name)
    sql_insert = "replace into slowlog_sql(hostname, loginname, loginip, database_name, sqltext, sourcesql, sqlchange_time, hashcode) values (%s,%s,%s,%s,%s,%s,now(),%s)"
    param = (hostname.replace(" ", "").replace("\n", ""), loginUser.replace(" ", "").replace("\n", ""), loginIp.replace(" ", "").replace("\n", ""), db_name, sqltext, sourcesql, hashcode)
    try:
        with get_conn() as conn:
            with conn as cur:
                cur.execute(sql_insert, param)
    except MySQLdb.Warning, w:
        pass 
    except MySQLdb.Error, e:
        print e

# check sql type, if in [select, update, delete, insert, replace]  return 1 else 0
def checkSqltype(sqltext):
    tmpsql = sqltext.strip().lower()
    if (tmpsql.find("insert") > -1 or tmpsql.find("replace") > -1) and tmpsql.find("from") > -1 and tmpsql.find("select") > -1 and tmpsql.find("values") == -1:  #  insert / replace  select ... from
        return 1    
    elif (tmpsql.find("update ") > -1 or tmpsql.find("delete ") > -1 or tmpsql.find("update.") > -1) and (tmpsql.find("insert ") == -1 and tmpsql.find("replace ") == -1):  # update / delete
        return 2
    elif tmpsql.find("select") > -1 and tmpsql.find("from") > -1:   # select
        return 3
    else:
        return 0

# 检查字符集，将非 utf8 的转换成 utf8
def convertCharactSet(s_str):
    charact_type=chardet.detect(s_str)["encoding"]
    return s_str.decode(charact_type, 'ignore').encode('utf-8')
    #return unicode(s_str,charact_type).encode('utf8')

# 分析慢查询
def slowlog(slowlogFile, hostip):
    sqlDic={}
    sqlDic_s={}
    sqlDic_s_f={}
    #修改或者hostname的方式，此种方式不兼容没有主机名的方式，根据线上hostname规则，直接将IP的点换成中杠
    #hostname = slowlogFile.split("/")[len(slowlogFile.split("/"))-1].split("-slow")[0]
    hostname = "-".join(hostip.split("."))
    # 打印执行到的服务器以及slowlog
    os.system("echo execute host: {0} `date +'%Y-%m-%d %H:%M:%S'` > /tmp/tmp_nohup.out 2>/dev/null".format(hostname))
    os.system("echo execute file: {0}  >> /tmp/tmp_nohup.out 2>/dev/null".format(slowlogFile))
    # 使用mysqldumslow解析slow文件
    os.system("mysqldumpslow -s c %s > /tmp/tmp_slowlog.sql.%s 2>/dev/null" %(slowlogFile, hostip))
    os.system("mysqldumpslow -s c -a %s > /tmp/tmp_slowlog.sql.%s.source 2>/dev/null" %(slowlogFile, hostip))

    # 抽象后的 sql
    logFile = open("/tmp/tmp_slowlog.sql.%s" %(hostip))
    
    # 取日志开始结束时间
    begin_time = os.popen("grep \"# Time:\" %s |awk 'NR==1 {print $3\" \"$4}'" %(slowlogFile)).read().replace("\n", "")
    end_time = os.popen("grep \"# Time:\" %s |awk 'END {print $3\" \"$4}'" %(slowlogFile)).read().replace("\n", "")
    yy = begin_time[:2]
    mm = begin_time[0:4][-2:]
    dd = begin_time[0:6][-2:]
    tt = begin_time[-8:]
    begin_time="20%s-%s-%s %s" %(yy, mm, dd, tt)

    yy = end_time[:2]
    mm = end_time[0:4][-2:]
    dd = end_time[0:6][-2:]
    tt = end_time[-8:]
    end_time="20%s-%s-%s %s" %(yy, mm, dd, tt)

    # 先判断是不是有 hashcode 需要更新( sourcesql 不新鲜 )或写入
    # 如果有就进行 sqlDic_s 的处理，如果没有就直接不处理
    isExists = 1
    infoItem={}
    isSecond=0
    lineNo = 0
    sqltext = ""
    loginUser = ""
    for line in logFile.xreadlines():
        line = convertCharactSet(line)
        if line[0:6] == "Count:":
            # 即解析:"Count: 1  Time=1.18s (1s)  Lock=0.00s (0s)  Rows=0.0 (0), product_useer[product_useer]@8.8.8.8"
            lineNo = lineNo + 1
            arrLine = line.split(" ")
            mCount = arrLine[1]
            avgTime = arrLine[3].replace("Time=", "").replace("s", "")
            totalTime = arrLine[4].replace("(", "").replace(")", "").replace("s", "")
            loginInfo = arrLine[11]
            loginUser = loginInfo.split("[")[0]
            loginIp = loginInfo.split("@")[1].replace("[", "").replace("]", "")
            avgLocktime = arrLine[6].replace("Lock=", "").replace("s", "")
            totalLocktime = arrLine[7].replace("(", "").replace(")", "").replace("s", "")
            avgRow = arrLine[9].replace("Rows=", "").replace("s", "")
            totalRow = arrLine[10].replace("(", "").replace(")", "").replace("s", "").replace(",", "")

            sqltext = ""
            isSecond=1
        # 如果第一行开头不为Count，则可能为sql语句
        else:
            if isSecond == 1: 
                # 判断是否为comment行
                if line.replace("\n", "").strip()[:2] == "/*" and line.replace("\n", "").strip()[-2:] == "*/" and line.count("*/") == 1 and line.count("/*") == 1:                    
                    continue
                else:
                    sqltext = sqltext + line          
            else:
                sqltext = sqltext + line    
            isSecond = 0

            if sqltext != "":
                hashcode=hash(sqltext)
                sqlDic[lineNo] = {}
                sqlDic[lineNo]["loginUser"]=loginUser
                sqlDic[lineNo]["loginIp"] = loginIp
                sqlDic[lineNo]["sql"]=sqltext
                sqlDic[lineNo]["hashcode"]=hashcode
            
                # 写入slowlog_history                
                writeToDB(hostip, hostname, loginUser, loginIp, begin_time, end_time, avgTime, totalTime, mCount, avgRow, avgLocktime, hashcode)
                # 从slowlog_sql删除过期的sql
                delete_expire_record(hashcode)   

                if isExists == 1:
                    sql_check_hashcode = "select hashcode from slowlog_sql where hashcode = '{0}'".format(hashcode)
                    with get_conn() as conn:
                        with conn as cur:
                            count = cur.execute(sql_check_hashcode)
                    if count == 0:    
                        isExists = 0
                    else:
                        isExists = 1

    # 有不存在于 slowlog_sql 中需要更新写入的    
    if isExists == 0:
        # source sql deal
        lineNo = 0
        sqltext = ""
        isSecond=0
        logFile = open("/tmp/tmp_slowlog.sql.%s.source" %(hostip))
        for line in logFile.xreadlines():
            line = convertCharactSet(line)
            if line[0:6] == "Count:":            
                lineNo = lineNo + 1
                arrLine = line.split(" ")
                loginInfo = arrLine[11]
                loginUser = loginInfo.split("[")[0]
                loginIp = loginInfo.split("@")[1].replace("[", "").replace("]", "")           

                sqltext = ""
                isSecond=1
            else:                
                if isSecond == 1: # is comment line ??
                    if line.replace("\n", "").strip()[:2] == "/*" and line.replace("\n", "").strip()[-2:] == "*/" and line.count("*/") == 1 and line.count("/*") == 1:                        
                        continue
                    else:
                        sqltext = sqltext + line              
                else:
                    sqltext = sqltext + line
                isSecond=0

                if sqltext != "":
                    sqlDic_s[lineNo] = sqltext
                    filename="/tmp/.%s.%s" %(hash(sqltext), hostip)
                    file_object = open(filename, 'w')
                    file_object.writelines(sqltext)
                    file_object.close()
                    
                    f_sql=os.popen("/home/dba/toolscripts/mysqlslowlog/getFormatSql %s" %(filename))

                    s_sql = f_sql.read().replace(" ", "").replace("\n", "") 
                    sqlDic_s_f[lineNo] = s_sql

                    if os.path.exists(filename):
                        os.system("rm %s" %(filename))
        logFile.close()

        # 比较 源 sql 和 抽象化后的 sql, 更新抽象化后 sql 对应的 源 sql
        # 通过去掉所有的参数 / 空格 / 回车 后进行字符串比较
        for mline in sqlDic:
            infoItem = sqlDic[mline]
            tmpsql = infoItem["sql"]
            loginUser = infoItem["loginUser"]
            loginIp = infoItem["loginIp"]
            hashcode=infoItem["hashcode"]

            onesql = tmpsql
            tmpsql = tmpsql.replace(" ", "").replace("\n", "")
            s_line = 0
            isFound = 0
            sourceSql = ""

            if checkSqltype(onesql) == 3:  # 只有select才会获取sourcesql，其他全部返回""（空字符串）
                for s_line in sqlDic_s_f:
                    f_sql = sqlDic_s_f[s_line]
                    oldSql = sqlDic_s[s_line]
                    
                    f_sql = f_sql.replace(" ", "").replace("\n", "")
                    
                    if f_sql == tmpsql:                                               
                        isFound = 1
                        sourceSql =  oldSql
                        break
            else:
                sourcesql = ""
                isFound = 1
            # 根据字典对比，符合条件的写入slowlog_sql        
            writeDirectorTB(hostip, hostname, loginUser, loginIp, onesql, sourceSql, hashcode)

            if isFound == 0:
                print "Not Found : \nsql1:  %s \nsql2:  %s"  %(onesql, tmpsql)

    # clear slow log
    if os.path.exists(slowlogFile):
        os.system("rm %s" %(slowlogFile))
    if os.path.exists("/tmp/tmp_slowlog.sql.%s.source" %(hostip)):            
        os.system("rm /tmp/tmp_slowlog.sql.%s.source" %(hostip))
    if os.path.exists("/tmp/tmp_slowlog.sql.%s" %(hostip)):
        os.system("rm /tmp/tmp_slowlog.sql.%s" %(hostip))

#get host info for clean thread 
def getHostInfo():  
    global QHOST 
    global RUNNING_HOST
    dbmonitor_ip='127.0.0.1'
    dbmonitor_port=3306
    dbmonitor_db='sqlmonitordb'
    
    try:
        if LOCK.acquire():
            # if QHOST is not empty, exit
            if QHOST.qsize() > 0:
               LOCK.release()
               time.sleep(30)
               return
            with get_conn(dbmonitor_ip, dbmonitor_db, dbmonitor_port) as conn:
                with conn as cur:
                    strsql = "select b.ip_business from serverlist a join cf_machine b on a.machine_name=b.machine_name where db_type='MySQL' and Perf_status=1 and monitor_status=1 and env_type not like '非生产%' and a.service_name not like 'DWInfobright%' and a.service_name not like 'DBTraceSummary%';"
                    cur.execute(strsql)
                    results=cur.fetchall()
            for r in results:
                if RUNNING_HOST.count(r) == 0:
                    QHOST.put(r)                    

            first_run = 0
            LOCK.release()
    except MySQLdb.Error,e:                
        LOCK.release()
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])


# deal slow log thread
class dealSlowlog(threading.Thread):  
    def __init__(self, t_name):  
        threading.Thread.__init__(self, name=t_name)  
        self.name=t_name
    def run(self):
        global QHOST
        global LOCk
        global RUNNING_HOST
        #slowlog("/backup/tmp_slowlog_collect/SVR4467HW2288-slow.log.1406516102-1406516401", "192.168.60.211")
        #return

        while True:
            if LOCK.acquire():
                if QHOST.qsize() == 0:
                   LOCK.release()
                   
                   # get hostlist from db
                   getHostInfo()
                   continue
                   
                hostItem=QHOST.get()
                RUNNING_HOST.append(hostItem)

                self.hostInfo=hostItem
                LOCK.release()
             
            try:
                remote_ip=self.hostInfo[0].strip()

                #get slow log
                mfile = os.popen("ssh -p 22 %s \"find /data/slowlog_history/* -mtime -7 -exec 'ls' -lh -c {} \\;|grep -iv '_done\$'\"" %(remote_ip))
                for listFile in mfile:
                    tmpstr = listFile.replace("\n", "").split(" ")[-1:][0]
                    filename = os.path.basename(tmpstr)
                    retno = os.system("scp -P 22 %s:%s /backup/tmp_slowlog_collect/%s >/dev/null 2>&1" %(remote_ip, tmpstr, filename.replace("done_", "")))
                    if retno == 0:
                       os.system("ssh -p 22 %s 'mv %s %s_done'" %(remote_ip, tmpstr, tmpstr))
                       #slowlog dump
                       slowlog("/backup/tmp_slowlog_collect/%s" %(filename.replace("done_", "")), remote_ip)
            else:
               print retno, " = ", listFile
            
                    time.sleep(10)       
                m_endtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                
                if LOCK.acquire():
                    RUNNING_HOST.remove(hostItem)
                    LOCK.release()
             
                ########break ###########
            except MySQLdb.Error,e:
                if LOCK.acquire():
                    RUNNING_HOST.remove(hostItem)
                    LOCK.release()                
                print "[ %s ] Mysql Error %d: %s [%s]" % (remote_ip, e.args[0], e.args[1], time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
            except Exception,e:
                if LOCK.acquire():
                    RUNNING_HOST.remove(hostItem)
                    LOCK.release()

                # clear slow log
                if os.path.exists("/backup/tmp_slowlog_collect/%s" %(filename.replace("done_", ""))):
                    os.system("rm /backup/tmp_slowlog_collect/%s" %(filename.replace("done_", "")))
                if os.path.exists("/tmp/tmp_slowlog.sql.%s.source" %(remote_ip)):            
                    os.system("rm /tmp/tmp_slowlog.sql.%s.source" %(remote_ip))
                if os.path.exists("/tmp/tmp_slowlog.sql.%s" %(remote_ip)):
                    os.system("rm /tmp/tmp_slowlog.sql.%s" %(remote_ip))

                print "[ %s ] error happend %s [ %s ]" %(remote_ip, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), e)


def main():     
    global ITHREADS
    global U_ADMIN 
    global U_ADMIN_PWD
    global U_SQLUPLOAD 
    global U_SQLUPLOAD_PWD
    global TASKQ
    global LOCK

    #get db user
    config=ConfigParser.ConfigParser()
    cfgfile=open('/home/dba/toolscripts/mysqlslowlog/login.cnf','r')
    config.readfp(cfgfile)
    
    U_ADMIN=config.get('mysqllogin','admin_user')
    U_ADMIN_PWD=endecrypt.endeCrypt.decrypt(config.get('mysqllogin','admin_passwd'))

    U_SQLUPLOAD=config.get('mysqllogin','product_user')
    U_ADMIN=config.get('mysqllogin','product_user')
    U_SQLUPLOAD_PWD=endecrypt.endeCrypt.decrypt(config.get('mysqllogin','product_passwd'))
    U_ADMIN_PWD=endecrypt.endeCrypt.decrypt(config.get('mysqllogin','product_passwd'))

    #create application instance
    appInstance = ApplicationInstance( '/tmp/slowLog.pid' )

    # get option
    getOpt()

    LOCK = threading.Lock()
    
    #deal thread
    for idx in range(0, ITHREADS):
        TASKQ[idx]=dealSlowlog('Con.' + str(idx))
        TASKQ[idx].start()

    for idx in range(0, ITHREADS):
        TASKQ[idx].join()   

    #remove pid file    
    appInstance.exitApplication()

    print 'All DONE'
if __name__ == '__main__':  
    main()

