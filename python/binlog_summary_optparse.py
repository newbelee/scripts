import re,sqlite3,optparse,os,time
#import re,sqlite3,argparse,os,time

db_loc = r'/tmp/Victor&sqlite3.db'

def get_schema_table_name(table, use_database):
    if '.' in table:
        schema_table_name = table
    else:
        schema_table_name = use_database + '.' + table
    return schema_table_name

def insert_dml_info(file_object):
    try:
        conn = sqlite3.connect(db_loc)
        cursor = conn.cursor()
        cursor.execute('''create table dml_info
            (schema_table varchar(50), dml_type varchar(10), dml_time timestamp)''')
        cursor.execute('''create table commit_info (dml_time timestamp)''')
        if isinstance(file_object, str):
 ##           file_object = open(file_object)
 ##       with file_object as f:
 		  with open(file_object) as f:
            dml_info_records = []
            commit_info_records=[]
            for line in f:
                dml_flag = 0
                if re.match('use',line) and line.strip().endswith('/*!*/;'):
                    use_database=re.split('`|`',line)[1]
                elif re.match('SET TIMESTAMP=',line):
                    dml_time=re.split('=|/', line)[1]
                elif re.match('insert|delete|update',line,re.I):
                    if re.match('insert',line,re.I):
                        m= re.search(r'(into)(.*?)(values|\(|\n|partition)',line,re.I)
                        table=m.group(2).strip()
                        dml_type='insert'
                    elif re.match('delete',line,re.I):
                        m=re.search(r'(from)(.*?)(partition|where|\n)',line,re.I)
                        table=m.group(2).strip()
                        dml_type='delete'
                    else:
                        m=re.search(r'(update|LOW_PRIORITY|IGNORE)(.*?)(set|\n)',line,re.I)
                        table=m.group(2).strip()
                        dml_type='update'
                    schema_table = get_schema_table_name(table, use_database)
                    dml_flag=1
                elif re.match('### (DELETE|INSERT|UPDATE)',line):
                     schema_table=re.split(' ',line)[-1].strip('').replace('`','').strip('\n')
                     dml_type=re.split(' ',line)[1]
                     dml_flag=1
                elif 'COMMIT/*!*/;' in line:
                    commit_info_records.append([dml_time])
                if dml_flag ==1:
                    dml_info_record=[]
                    dml_info_record.append(schema_table)
                    dml_info_record.append(dml_type)
                    dml_info_record.append(dml_time)
                    dml_info_records.append(dml_info_record)
                if len(dml_info_records) % 10000 ==0:
                    cursor.executemany("insert into dml_info values (?,?,?)",dml_info_records)
                    cursor.executemany("insert into commit_info values (?)", commit_info_records)
                    conn.commit()
                    dml_info_records=[]
                    commit_info_records=[]
            cursor.executemany("insert into dml_info values (?,?,?)", dml_info_records)
            cursor.executemany("insert into commit_info values (?)", commit_info_records)
            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
       print e

def query(sql):
    conn = sqlite3.connect(db_loc)
    cursor = conn.cursor()
    cursor.execute(sql)
    results=cursor.fetchall()
    column_size=len(results[0])
    for each_result in results:
        for i in range(column_size):
            print str(each_result[i]).ljust(20),
        print
    cursor.close()
    conn.close()

def unix_timestamp(localtime):
    timearray = time.strptime(localtime, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timearray))
    return  timestamp

if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser(description='Aggregate DML in Binlog')
    parser.add_argument("-f", dest='file_object', action='store',type=argparse.FileType('r'), help="The binlog file with txt format",
                        required=True, metavar='')
    parser.add_argument("-tps", action='store_true', help="TPS")
    parser.add_argument("-opr", action='store_true', help="DML per table")
    parser.add_argument("-extend", action='store_true', help="DML per table per seconds")
    parser.add_argument("-start", action='store', help="start datetime, for example: 2004-12-25 11:25:56")
    parser.add_argument("-stop", action='store', help="stop datetime, for example: 2004-12-25 11:25:56")
    args = parser.parse_args(r'-f C:\Users\Administrator\Desktop\1.txt -tps -opr -extend'.split())
    # args = parser.parse_args()
    file_object=args.file_object
    tps=args.tps
    opr=args.opr
    extend=args.extend
    start_datetime=args.start
    stop_datetime=args.stop
    '''
    parser = optparse.OptionParser()
    parser.add_option("-f", "--filename",dest="file_object",metavar="FILE", help="")
    parser.add_option("--tps", action='store_true', help="TPS")
    parser.add_option("--opr", action='store_true', help="DML per table")
    parser.add_option("--extend", action='store_true', help="DML per table per seconds")
    parser.add_option("--start", action='store', help="start datetime, for example: 2004-12-25 11:25:56")
    parser.add_option("--stop", action='store', help="stop datetime, for example: 2004-12-25 11:25:56")
    parser.add_option("--force", action='store_true', help="remove the existed sqlite3 file")
    (options, args) = parser.parse_args()
    file_object=options.file_object
    tps=options.tps
    opr=options.opr
    extend=options.extend
    start_datetime=options.start
    stop_datetime=options.stop
    force=options.force
    if (start_datetime and not stop_datetime) or (not stop_datetime and start_datetime):
        print "you have to specify the start_datetime and stop_datetime both"
        exit() 
    if force and os.path.exists(db_loc):
        os.remove(db_loc)
    if not os.path.exists(db_loc):
        insert_dml_info(file_object)
    if tps :
        if start_datetime :
            start_timestamp = unix_timestamp(start_datetime)
            stop_timestamp = unix_timestamp(stop_datetime)
            sql = "select datetime(dml_time, 'unixepoch','localtime'),count(*) from commit_info \
                  where dml_time BETWEEN "+str(start_timestamp)+" and "+str(stop_timestamp)+ "  group by dml_time order by 1"
        else:
            sql = "select datetime(dml_time, 'unixepoch','localtime'),count(*) from commit_info  group by dml_time order by 1"
        query(sql)
        print
    if opr:
        if start_datetime:
            start_timestamp = unix_timestamp(start_datetime)
            stop_timestamp = unix_timestamp(stop_datetime)
            sql = "select schema_table,upper(dml_type),count(*) times from dml_info \
                         where dml_time BETWEEN " + str(start_timestamp) + " and " + str(stop_timestamp) + "  group by schema_table,dml_type order by 3 desc"
        else:
            sql = "select schema_table,upper(dml_type),count(*) times from dml_info group by schema_table,dml_type order by 3 desc"
        query(sql)
        print
    if extend:
        if start_datetime:
            start_timestamp = unix_timestamp(start_datetime)
            stop_timestamp = unix_timestamp(stop_datetime)
            print start_timestamp,stop_timestamp
            sql = "select datetime(dml_time, 'unixepoch','localtime'),schema_table,upper(dml_type),count(*)  from dml_info  \
                      where dml_time BETWEEN " + str(start_timestamp) + " and " + str(stop_timestamp) + " group by dml_time,schema_table,dml_type order by 1"
        else:
            sql = "select datetime(dml_time, 'unixepoch','localtime'),schema_table,upper(dml_type),count(*)  from dml_info  \
                     group by dml_time,schema_table,dml_type order by 1"
        query(sql)
