import MySQLdb
import sendmail

import sys
import getopt
reload(sys)
sys.setdefaultencoding('utf8') 

import smtplib
from email.mime.text import MIMEText
from email.message import Message
from email.header import Header

mail_host = 'mx.local'
mail_port = 25
mail_user = 'xx'
mail_pass = 'xxx'
mail_send_from = 'xxxxx'
mail_send_from_fail = 'xxxxx'
to_list = 'xxxx'

dbmon_db_host="127.0.0.1"
dbmon_db_port=3358
dbmon_db_user="root"
dbmon_db_password="xxx"
dbmon_db_database="xxx"
lepus_db_database="xxxxx"



def parseoptions(argv):	
	global filter_ip,filter_db
	try:
		opts, args = getopt.getopt(argv[1:],"Hh:d:",["help","host=","database="])
		for opt,arg in opts:
			if opt in ("-H","--help"):
				print "Please input host or database"
				sys.exit(0)
			elif opt in ("-h","--host"):
				filter_ip=arg
			elif opt in ("-d","--database"):
				filter_db=arg

	except Exception,e:
		print e
		sys.exit(1)
	
			

def send_mail(to_list_del,sub,content):
    '''
    to_list
    sub
    content
    send_mail("aaa@126.com","sub","content")
    '''
    #me=mail_user+"<</span>"+mail_user+"@"+mail_postfix+">"
    me=mail_send_from
    msg = MIMEText(content, _subtype='html', _charset='utf8')
    msg['Subject'] = Header(sub,'utf8')
    msg['From'] = Header(me,'utf8')
    msg['To'] = to_list
    try:
        smtp = smtplib.SMTP()
        smtp.connect(mail_host,mail_port)
        smtp.login(mail_user,mail_pass)
        smtp.sendmail(me,to_list, msg.as_string())
        smtp.close()
        return True
    except Exception, e:
        print str(e)
        return False





def get_connect(host):
	connect = MySQLdb.connect(host=host,port=dbmon_db_port,user=dbmon_db_user, passwd=dbmon_db_password,connect_timeout=3)
	cursor = connect.cursor(cursorclass=MySQLdb.cursors.DictCursor)
	return connect,cursor


def slow_check(host,db,sql,execute_times,query_time_avg):
    try:
	conn,curs =get_connect(host)
        conn.select_db(db)
        curs.execute("explain %s"  %(sql))

        result = curs.fetchall()
	num=len(result)
	num=num+1
	text = ''
	flag=1
	head_text="""<table style='width:2000px;height:100%;'><tr>
            <th style='border:1px;border-style:none solid none none;'>id</th>
            <th style='border:1px;border-style:none solid none none;'>table</th>
            <th style='border:1px;border-style:none solid none none;'>partitions</th>
            <th style='border:1px;border-style:none solid none none;'>type</th>
            <th style='border:1px;border-style:none solid none none;'>possible_keys</th>
            <th style='border:1px;border-style:none solid none none;'>ley</th>
            <th style='border:1px;border-style:none solid none none;'>key_len</th>
            <th style='border:1px;border-style:none solid none none;'>ref</th>
            <th style='border:1px;border-style:none solid none none;'>rows</th>
            <th style='border:1px;border-style:none solid none none;'>Extra</th></tr>"""

        for re in result:
		id=re.get('id')
                select_type=re.get('select_type')
                table=re.get('table')
                partitions=re.get('partitions')
		if partitions:
			partitions=partitions[:22]
                type=re.get('type')
                possible_keys=re.get('possible_keys')
		if possible_keys:
			possible_keys=possible_keys[:25]
                key=re.get('key')
                key_len=re.get('key_len')
                ref=re.get('ref')
                rows=re.get('rows')
                Extra=re.get('Extra')
		if key:
			explain_text='''
			<tr>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
			</tr>
                                ''' %(id,table,partitions,type,possible_keys,key,key_len,ref,rows,Extra)
		else:
			explain_text='''
                        <tr>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td bgcolor="#FF0000" style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td style='border:1px;border-style:solid solid none none;'>%s</td>
                           <td nowrap style='border:1px;border-style:solid solid none none;'>%s</td>
                        </tr>
                                ''' %(id,table,partitions,type,possible_keys,key,key_len,ref,rows,Extra)	

		text = text+str(explain_text)
		middle=head_text+text+"</table>"
	## sql get first 4000
	sql = sql[:2000]
	first_text='''
		<tr>
		     <td>%s</td>
		     <td>%s</td>
		     <td>%s</td>
		     <td>%s</td>
		     <td>%s</td>
		</tr>
		<tr><td colspan=5>%s</td></tr>
		<tr><td colspan=5 style="height:50px;"></td></tr>
		''' %(host,db,execute_times,query_time_avg,sql,middle)
	return_text=first_text
	return return_text,num	
    except ValueError:
	print 'Error'
        return '',0
    except Exception,e:
        print e
        return '',0

def main():
	conn = MySQLdb.connect(host=dbmon_db_host,port=dbmon_db_port,user=dbmon_db_user, passwd=dbmon_db_password,db=dbmon_db_database,connect_timeout=3)
	cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
	_sql = "select IP from dbmon.mysqllist where sendmail >=1 and slowcheck=1;"
	cur.execute(_sql)
	all_ip=cur.fetchall()
	cur.close()
	conn.close()

	for _ip in all_ip:
	    _ip=_ip.get('IP')	
	    sql = "SELECT c.ip,b.db_max,a.sample,b.ts_cnt,b.Query_time_pct_95 FROM lepus.mysql_slow_query_review a INNER JOIN lepus.mysql_slow_query_review_history b ON a.`checksum` = b.`checksum` INNER JOIN dbmon.mysqllist c ON b.serverid_max = c.id where c.ip= '%s' order by b.Query_time_pct_95 desc ;" %(_ip)

	    conn = MySQLdb.connect(host=dbmon_db_host,port=dbmon_db_port,user=dbmon_db_user, passwd=dbmon_db_password,db=dbmon_db_database,connect_timeout=3)
	    cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
	    cur.execute(sql)
	    servers = cur.fetchall()
	    cur.close()
	    conn.close()
 	    all_text=""
	    for  row in servers:
		host=row.get('ip')
		db=row.get('db_max')
		sql=row.get('sample')
		execute_times=row.get('ts_cnt')
		query_time_avg=row.get('Query_time_pct_95')

		#result,num=str(slow_check(host,db,sql))
		result,num=slow_check(host,db,sql,execute_times,query_time_avg)
		all_text=all_text+str(result)+'\n'
		

	    if all_text <>"":
        	text_head="<html><body><table style='width:2000px;' border='2' cellspacing='0' cellpadding='0' ><tr><td>host</td><td>db</td><td>execute_times</td><td>query_time_avg</td><td style='width:300px;'>SQL</td></tr>\n"
        	all_text=text_head+all_text+"</table></body></html>"
	        #send_mail('','MySQL Slowlog Check',all_text)
	        send_mail('',host+'-'+db+'-MySQL Slowlog Check',all_text)
	
if __name__ == '__main__':
	#parseoptions(sys.argv)
	main()
