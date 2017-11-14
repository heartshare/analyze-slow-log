import os
import datetime
import MySQLdb
import MySQLdb.cursors

dbmon_db_host="127.0.0.1"
dbmon_db_port=3358
dbmon_db_user="root"
dbmon_db_password="xxx"
dbmon_db_database="dbmon"
lepus_db_database="lepus"

mess_good=''
mess_bad=''
service_list={}

onedayago= (datetime.datetime.now() - datetime.timedelta(days = 1))
date_format=onedayago.strftime("%Y%m%d")
current_dir=os.getcwd()

conn = MySQLdb.connect(host=dbmon_db_host,port=dbmon_db_port,user=dbmon_db_user, passwd=dbmon_db_password,db=dbmon_db_database,connect_timeout=3,cursorclass=MySQLdb.cursors.DictCursor)
cur=conn.cursor()
cur.execute('delete from lepus.mysql_slow_query_review;')
cur.execute('delete from lepus.mysql_slow_query_review_history;')
conn.commit()

#cur.execute('select ID,DBNAME,IP,PORT,"it_ops_dba@yhd.com" from mysqllist where sendmail=1 and slowcheck=1;')
cur.execute("select ID,DBNAME,IP,PORT from mysqllist where ip='172.20.225.146';  ")
result=cur.fetchall()
cur.close()
conn.close()
if result:
    for row in result:
        dbname=row.get('DBNAME')
        ip=row.get('IP')
        port=row.get('PORT')
   	service_list[dbname]=row.get('EMAILS')     
        print '%s:%s' %(dbname,ip)

        if os.path.exists('%s' %(dbname)):
            print 'directory:%s exist.\n' %dbname
            #mess_good+='directory:%s exist.\n' %dbname
        else:
            os.mkdir('%s' %(dbname))
            print 'create directory:%s\n' %dbname
        
        #cp_slowlog=os.system('rsync -ap --progress --bwlimit=20000 %s:/data/mysql/log/slow_%d_%s.log ./%s/%s_%s.log' %(ip,port,date_format,dbname,ip,date_format))
        cp_slowlog=os.system('rsync -ap --progress --bwlimit=20000 %s:/export/data/mysql/log/slow.log ./%s/%s_%s.log' %(ip,dbname,ip,date_format))
        if cp_slowlog==0:
	    os.system('ssh -t %s cp /export/data/mysql/log/slow.log /export/data/mysql/log/%s_slow.log' %(ip,date_format))
	    os.system('ssh -t %s rm -f /export/data/mysql/log/slow.log' %(ip))

	    conn = MySQLdb.connect(host=ip,port=dbmon_db_port,user=dbmon_db_user, passwd=dbmon_db_password,connect_timeout=3,cursorclass=MySQLdb.cursors.DictCursor)
	    cur=conn.cursor()
	    sql='set global slow_query_log_file="/export/data/mysql/log/slow.log";'
	    cur.execute(sql)
	    cur.close()
	    conn.close()
	   
            print "copy slow log from %s:%s done\n" %(dbname,ip)
        else:
            print "copy slow log from %s:%s failed\n" %(dbname,ip)
            
        merge_file=os.system('cat ./%s/%s_%s.log >>./%s/%s_%s_slow.log' %(dbname,ip,date_format,dbname,ip,date_format))
        if merge_file==0:
            print "merge file ./%s/%s_%s.log ok\n" %(dbname,ip,date_format)
            #mess_good+="merge file ./%s/%s_%s.log ok\n" %(dbname,ip,date_format)
        else:
            print "merge file ./%s/%s_%s.log failed\n" %(dbname,ip,date_format)
            #mess_bad+="merge file ./%s/%s_%s.log failed\n" %(dbname,ip,date_format)
            
#use pt-query-digest to analyze the slow log for mail
if service_list:
    for k,v in service_list.iteritems():
        dbname=k
        #emails=v
        #emails='IT_OPS_DBA@yhd.com'
        emails='gongxingliang@yhd.com'
        print 'start pt-query-digest for %s\n' %dbname
        pt_check=os.system('/usr/bin/pt-query-digest --limit=95%% --outliers=1:200 ./%s/%s_%s_slow.log > ./%s/%s_%s_%s_pt.log' %(dbname,ip,date_format,dbname,dbname,ip,date_format))
        if pt_check==0:
            print "pt-query-degist analyze ./%s/%s_slow.log done\n" %(dbname,date_format)
            #mess_good+="pt-query-degist analyze ./%s/%s_slow.log done\n" %(dbname,date_format)
        else:
            print "pt-query-degist analyze ./%s/%s_slow.log failed\n" %(dbname,date_format)
            #mess_bad+="pt-query-degist analyze ./%s/%s_slow.log failed\n" %(dbname,date_format)
        
        #send_mail=os.system("/usr/local/bin/sendEmail  -s mx.jd.local -f yhddb@jd.com -t %s -u %s:%s slow query log -xu 'yhddb' -xp 'Baojing456*' -o message-file=%s/%s/%s_%s_pt.log  -a '%s/%s/%s_%s_pt.log'" %(emails,date_format,dbname,current_dir,dbname,dbname,date_format,current_dir,dbname,dbname,date_format))
        #if send_mail==0:
         #   print "%s send mail ok" %dbname
         #   mess_good+="%s send mail ok" %dbname
        #else:
        #    print "%s send mail failed" %dbname
        #    mess_bad+="%s send mail failed" %dbname
            
#use pt-query-digest to analyze the slow log for lepus
if result:
    for row in result:
        lepus_server_id=row.get('ID')
        dbname=row.get('DBNAME')
        ip=row.get('IP')
        
        if os.path.isfile('./%s/%s_%s.log' %(dbname,ip,date_format)):
            pt_check=os.system('/usr/bin/pt-query-digest --user=%s --password=%s  --port=%d  --review h=%s,D=%s,t=mysql_slow_query_review  --history h=%s,D=%s,t=mysql_slow_query_review_history  --no-report --limit=100%% --filter=" \$event->{add_column} = length(\$event->{arg}) and \$event->{serverid}=%s " ./%s/%s_%s.log > /tmp/lepus_slowquery.log' %(dbmon_db_user,dbmon_db_password,dbmon_db_port,dbmon_db_host,lepus_db_database,dbmon_db_host,lepus_db_database,lepus_server_id,dbname,ip,date_format))
            if pt_check==0:
                print "pt-query-degist analyze ./%s/%s_%s.log done\n" %(dbname,ip,date_format)
                #mess_good+="pt-query-degist analyze ./%s/%s_%s.log done\n" %(dbname,ip,date_format)
            else:
                print "pt-query-degist analyze ./%s/%s_%s.log failed\n" %(dbname,ip,date_format)
                #mess_bad+="pt-query-degist analyze ./%s/%s_%s.log failed\n" %(dbname,ip,date_format)

delete old log files        
os.system("cd %s && /usr/bin/find ./ -name '*.log' -mtime +5 | xargs rm -f " %current_dir)                         
print 'script done'
