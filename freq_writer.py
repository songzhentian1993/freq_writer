# -*- coding: utf-8 -*-
import ConfigParser
import logging
import time
import sys
import os
import shutil
import signal
import string

from kafka import KafkaConsumer
from kafka import TopicPartition

import log_utils
import sys
import time
from datetime import date
import threading
import re
import datetime
import base64
import happybase
import logging.handlers
import MySQLdb

reload(sys)
sys.setdefaultencoding("utf-8")

logger = logging.getLogger('mysqldb_logger')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('./log/mysqldb.log')
handler.setLevel(logging.WARNING)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(log_formatter)
logger.addHandler(handler)

def dump_mysql(keyv, str_val):
    try:
        db = MySQLdb.connect(g_config.g_mysqldb_host, g_config.g_mysqldb_user, g_config.g_mysqldb_passwd, g_config.g_mysqldb_dbname, use_unicode=True, charset='utf8')
    except MySQLdb.Error, e:
        logger.error('dump_mysql: Fail to connect MySQLdb, [%s].' % e)
        
    cursor = db.cursor()

    sql_insertlog = '''INSERT INTO frequency_data 
                       (keyv, str_val) 
                       VALUES 
                       ('{}', '{}');''' .format(keyv, str_val)
                      
    try:
        cursor.execute(sql_insertlog)
        db.commit()
    except MySQLdb.Error, e:
        logger.error('dump_mysql: sql error, [%s].' % e)
        db.rollback()
    finally:
        cursor.close()
        db.close()

def drop_create_table():  
    try:
        db = MySQLdb.connect(g_config.g_mysqldb_host, g_config.g_mysqldb_user, g_config.g_mysqldb_passwd, g_config.g_mysqldb_dbname, use_unicode=True, charset='utf8')
    except MySQLdb.Error, e:
        logger.error('drop_create_table: Fail to connect MySQLdb, [%s].' % e)
         
    cursor = db.cursor()

    sql_createtable = '''CREATE TABLE IF NOT EXISTS %s (
                         log_id INT NOT NULL AUTO_INCREMENT,
                         keyv CHAR(60), 
                         str_val CHAR(60),
                         PRIMARY KEY (log_id)
                         )ENGINE=InnoDB DEFAULT CHARSET=utf8;''' % g_config.g_mysqldb_dbtable
    
    sql_droptable = 'DROP TABLE IF EXISTS %s;' % g_config.g_mysqldb_dbtable
    
    try:
        cursor.execute(sql_droptable)
        cursor.execute(sql_createtable)
        db.commit()
    except MySQLdb.Error, e:
        logger.error('drop_create_table: sql error, [%s].' % e)
        db.rollback()
    finally:
        cursor.close()
        db.close()        
        
class MemCache():
    def __init__(self, col='cf:imp', delim='\x01'):
        self.delim = delim
        self.col = col
        self.dump_dict = {}
        self.initflag = False

    def init(self, table, day):
        total = 0
        self.camp_dict = {day: {}}
        for region in range(0, 10):
            start_row = str(region) + self.delim + day
            end_row = str(region) + self.delim + day[:-1] + chr(ord(day[-1])+1)
            for key, value in table.scan(row_start=start_row, row_stop=end_row, columns=[self.col]):
                total += 1
                imp = value[self.col]
                self.camp_dict[day][key] = int(imp)
            log_utils.i('load from region: %d, start row=%s, end row=%s, total data=%d' %(region, start_row, end_row, total))
        self.initflag = True

    def get(self, key, day):
        if not self.camp_dict.has_key(day):
            self.camp_dict[day] = {}
        val = self.camp_dict[day].get(key, 0) + 1
        self.camp_dict[day][key] = val
        return val

    def dump(self, day, file_name, clear_thread_dump, flag=True):
        start_pos=13
        if not self.initflag:
            log_utils.i('Cache Mem is being initialized!')
            return 0 
        if not self.camp_dict.has_key(day):
            if flag:
                log_utils.w('Data %s do not have data!' %(str(day)))
            return -1
        start = time.time()
        ofp = open(file_name, 'w')
        if clear_thread_dump:
            drop_create_table()
        count = 0
        for key in self.camp_dict[day].keys():
            keyv = key.split(self.delim)[2]
            val = self.camp_dict[day][key]
            #print '%s\t%s' %(key, str(val))
            #ofp.write("%s\t%s\n" % (key[start_pos:], str(val)))
            ofp.write("%s\t%s\n" % (keyv, str(val)))
            if clear_thread_dump:
                dump_mysql(keyv, str(val))
            count += 1
        end = time.time()
        ofp.close()
        last_num = self.dump_dict.get(day, 0)
        if last_num > count:
            log_utils.w("fail dump data with prefix %s into file %s: total %d < %d (last total), cost %d ms" % (day, file_name, count, last_num, (end-start)*1000))
            return -1
        self.dump_dict[day] = count
        log_utils.i("dump data with prefix %s into file %s: total %d >= %d (last total), cost %d ms" % (day, file_name, count, last_num, (end-start)*1000))
        return 1

    def clear(self, day):
        if self.camp_dict.has_key(day):
            del(self.camp_dict[day])
            log_utils.i('Delete cache mem key:%s' %(day))	
        if self.dump_dict.has_key(day):	
            del(self.dump_dict[day])
            log_utils.i('Delete cache dump key:%s' %(day))	

    def getkey(self):
        return str(self.camp_dict.keys())

    def size(self):
        total = 0
        for key in self.camp_dict.keys():
            total += len(self.camp_dict[key])
        return total


class GlobalConfig():
    def __init__(self):
        self.g_kafka_server = []
        self.g_kafka_topic_name = ''
        self.g_kafka_consumer_group = ''
        self.g_one_batch_num = 1
        self.g_one_batch_time_threshold = 10
        self.g_dump_interval = 60
        self.g_dump_file_name = "frequency_data.txt"
        #self.g_dest_file_name="/pdata1/log/nginx/dsp/frequency_data.txt"
        self.g_hbase_host = ''
        self.g_hbase_port = 9090
        self.g_hbase_rtable = ''
        self.g_hbase_ftable = ''
        self.g_hbase_region = 10
        self.g_hbase_batch_num = 5000
        self.g_mysqldb_host = ''
        self.g_mysqldb_user = ''
        self.g_mysqldb_passwd = ''
        self.g_mysqldb_dbname = ''
        self.g_mysqldb_dbtable = ''


g_config = GlobalConfig()
g_memcache = MemCache()


g_interrupt = False


def init_global_config():
    global g_config
    try:
        cf = ConfigParser.ConfigParser()
        cf.read("./conf/freq_writer.conf")
        kvs = cf.items("kafka_server")
        for kv in kvs:
            if len(kv) != 2:
                log_utils.e("kafka_server error, kv size=%d" % (len(kv)))
                return -1
            g_config.g_kafka_server.append(kv[1])
        g_config.g_kafka_topic_name = cf.get("kafka", "topic_name")
        g_config.g_kafka_consumer_group = cf.get("kafka", "consumer_group")
        g_config.g_one_batch_num = cf.getint("freq_writer", "batch_num")
        g_config.g_one_batch_time_threshold = cf.getint("freq_writer", "batch_time_threshold")
        g_config.g_hbase_host = cf.get("hbase", "host")
        g_config.g_hbase_port = cf.getint("hbase", "port")
        g_config.g_hbase_rtable = cf.get("hbase", "rtable")
        g_config.g_hbase_ftable = cf.get("hbase", "ftable")
        g_config.g_hbase_region = cf.getint("hbase", "region")
        g_config.g_hbase_batch_num = cf.getint("hbase", "batch_num")
        g_config.g_mysqldb_host = cf.get('mysqldb', 'host')
        g_config.g_mysqldb_user = cf.get('mysqldb', 'user')
        g_config.g_mysqldb_passwd = cf.get('mysqldb', 'passwd')
        g_config.g_mysqldb_dbname = cf.get('mysqldb', 'dbname')
        g_config.g_mysqldb_dbtable = cf.get('mysqldb', 'dbtable')
    except Exception, e:
        log_utils.f("fail to init_global_config, error[%s]" % e)
        return -1
    return 0

"""
程序开始时运行，读取上次的进度
以及初始化progress_list
"""
def get_progress_and_seek(consumer):
    try:
        # partitions可能改变，必须重新获取
        partitions = consumer.partitions_for_topic(g_config.g_kafka_topic_name)
        log_utils.d("get from consume, partitions len=%d" % len(partitions))
        assign_list = []
        for p_id in partitions:
            assign_list.append(TopicPartition(g_config.g_kafka_topic_name, p_id))

        consumer.assign(assign_list)
        for p_id in partitions:
            offset = consumer.committed(TopicPartition(g_config.g_kafka_topic_name, p_id))
            if offset is None:
                continue
            log_utils.d("p_id=%d, offset=%d" % (p_id, offset))
            consumer.seek(TopicPartition(g_config.g_kafka_topic_name, p_id), offset)
            log_utils.d("p_id=%d seek succeed" % p_id)
    except Exception, e:
        log_utils.f("fail to get_progress_and_seek, error[%s]" % e)
        log_utils.f(e, exc_info=True)
        return -1
    return 0

def get_hbase_connection():
    try:
        #connection = happybase.Connection(host=g_config.g_hbase_host, port=g_config.g_hbase_port, autoconnect=False)
        connection = happybase.Connection(host=g_config.g_hbase_host, port=g_config.g_hbase_port)
        connection.open()
        rtable = connection.table(g_config.g_hbase_rtable)
        ftable = connection.table(g_config.g_hbase_ftable)
        return rtable, ftable
    except Exception, e1:
        log_utils.f("conn hbase db error[%s]" % e1)
        log_utils.f(e1, exc_info=True)
        return None


"""
1. 初始化mysql, kafkaconsume
2. seek到上次进度
3. 读取数据并处理
"""
def main():
    log_utils.i("start compute thread")
    if 0 != init_global_config():
        log_utils.f("fail to init_global_config")
        return -1

    consumer = KafkaConsumer(
        bootstrap_servers=g_config.g_kafka_server,
        group_id=g_config.g_kafka_consumer_group,
        auto_offset_reset='latest',
        #auto_offset_reset='smallest', enable_auto_commit=False,
        consumer_timeout_ms=500)

    # 2. seek到上次进度
    if 0 != get_progress_and_seek(consumer):
        log_utils.f("fail to get_progress_and_seek")
        return -1

    # connect hbase table
    rtable, ftable = get_hbase_connection()
    if rtable is None or ftable is None:
        return -1

    #init mem cache
    today = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    time1 = time.time()
    g_memcache.init(rtable, today)
    time2 = time.time()
    log_utils.w("load data from hbase to init memcache, size=%d, cost=[%.3lf]s" %(g_memcache.size(), time2-time1))
    
    log_utils.w("freq writer is running")
    # 3. deal
    while not g_interrupt:
        deal_start = time.time()
        if 0 != read_and_deal_one_batch(consumer, rtable, ftable):
            # 处理一批数据失败，重新seek, 重新处理
            if 0 != get_progress_and_seek(consumer):
                log_utils.f("fail to get_progress_and_seek")
                return -1
            rtable, ftable = get_hbase_connection()
            if rtable is None or ftable is None:
                return -1
        deal_end = time.time()
        log_utils.i("deal one batch cost %.3lfms" %((deal_end - deal_start) * 1000.00))
    log_utils.i("end compute thread")


"""
读取一批数据，并处理
"""
def read_and_deal_one_batch(consumer, rtable, ftable):
    data_list = []
    read_num = g_config.g_one_batch_num
    read_time_start = time.time()
    reason = 'outsize'
    last_log_time = "NULL"
    maxtime=0
    brse_imp_logatch_list = []
    mintime=sys.maxint
    while read_num > 0:
        try:
            message = next(consumer)
            # print "receive message=", (message)
            log_utils.d("receive message=%s" % message.value)

            tmp_time = parse_imp_log(message.value, data_list)
            if tmp_time is not None:
                maxtime, mintime = getMaxMinTime(maxtime, mintime, tmp_time)

            read_num -= 1
            time_now = time.time()
            if time_now - read_time_start >= g_config.g_one_batch_time_threshold:
                reason = 'outtime'
                break
        except StopIteration:
            # print "there is no data now"
            reason = 'nonext'
            log_utils.d("there is no data now")
            time.sleep(0.1)
            break
        except Exception, e1:
            log_utils.e(e1, exc_info=True)
            return -1
    read_time_end = time.time()
    log_utils.i("read_one_batch count[%d] cost [%.3lf] ms, batch reason:%s, first log time:%s, last log time:%s" % (len(data_list), (read_time_end - read_time_start) * 1000.00, reason, timestampToStr(mintime), timestampToStr(maxtime)))
    try:
        time_start = time.time()
        insert_batch_data(rtable, ftable, data_list)
        time_insert = time.time()
        log_utils.i("insert_into_hbase_batch cost [%.3lf] ms" % ((time_insert - time_start) * 1000.00))
        # commit 所有partition
        consumer.commit()
        time_commit = time.time()
        #log_utils.i("kafka commit cost [%.3lf] ms" % ((time_commit - time_insert) * 1000.00))
        out_time = isTimeOut(maxtime)
        log_utils.i("detail_one_batch count[%d] cost [%.3lf] ms, kafka commit cost [%.3lf] ms, first log time:%s, last log time:%s, deplay time:[%.3lf] ms" % (len(data_list), (time.time() - read_time_start) * 1000.00, (time_commit - time_insert) * 1000.00, timestampToStr(mintime), timestampToStr(maxtime), out_time*1000.00))
        if int(out_time) > 60:
            log_utils.e("Frequency data processing delay: [%.3lf] ms" %(out_time*1000.00))
    except Exception, e2:
        # 执行一次失败, 需要重新seek
        log_utils.e(e2, exc_info=True)
        return -1
    return 0

def isTimeOut(lasttime):
    if lasttime == 0 or lasttime == sys.maxint:
        return 0
    out_time = time.time() - lasttime
    return out_time

def split_to_dict(arg):
    args = arg.split('&')
    kv = {}
    for vv in args:
        i = vv.find('=')
        if -1 == i:
            continue
        kv[vv[0:i]] = vv[i+1:]
    return kv

def get_value(kv, k):
    if k in kv:
        return kv[k]
    return ''

def timestampToStr(ss):
    if ss == 0 or ss == sys.maxint:
        return 'NULL'
    x = time.localtime(ss)
    return time.strftime('%d/%b/%Y:%H:%M:%S',x)

def getMaxMinTime(maxt, mint, ti):
    try:
        tt = time.mktime(time.strptime(ti,'%d/%b/%Y:%H:%M:%S'))
        if tt > maxt:
            maxt = tt
        if tt < mint:
            mint = tt
    except:
        pass
    return maxt, mint

g_pattern = re.compile(r"^[^[]*\[([^ ]*)[^?]*\?([^ \/]*)")
def parse_imp_log(log, data_list):
    result = g_pattern.match(log)
    if result is None:
        log_utils.w("[invalid log:format][log:%s]" % (log))
        return
    log_time = result.group(1)
    arg = result.group(2)
    day = datetime.datetime.strptime(log_time, '%d/%b/%Y:%H:%M:%S').strftime('%Y-%m-%d')
    #day = datetime.datetime.strptime(day, '%d/%b/%Y').strftime('%Y-%m-%d')
    #args = arg.split('&')
    args = split_to_dict(arg)
    try:
        campaign_id = get_value(args, 'campaign_id')
        if not campaign_id.isdigit():
            log_utils.w("[invalid log:campaign_id][log:%s]" % (log))
            return None
        try:
            ip = get_value(args, 'ip_encode')
            ip = '' if ip is None or ip == '' else base64.b64decode(ip)
        except Exception,e:
            ip = None
        try:
            uid = get_value(args, 'udid_encode')
            uid = '' if uid is None or uid == '' else base64.b64decode(uid)
        except Exception,e:
            uid = None

        adx = get_value(args, 'adx')
        ad = get_value(args, 'ad')
        bid = get_value(args, 'bid')
        imp = get_value(args, 'imp')
        filter_key = '%s\x01%s\x01%s\x01%s' % (adx, ad, bid, imp)
        #filter_key_tmp = '%s\x01%s\x01%s\x01%s' % (adx, ad, bid, imp)
        #filter_key = '%s%s' %(str(hex(hash(filter_key_tmp)))[2:8], filter_key_tmp)

        if uid is not None and re.match(r'[0-9a-zA-Z\-\._=]+$', uid):
            val_uid = '%s\x01%d\t%s\t%s' % (day, 1, uid, campaign_id)
            #val_uid_tmp = '%d\t%s\t%s' % (1, uid, campaign_id)
            #uid_hash_tmp = str(hex(hash(val_uid_tmp)))[2:8]
            #val_uid = '%s%s\x01%s' %(day, uid_hash_tmp, val_uid_tmp)
        else:
            val_uid = None
            log_utils.w("[invalid log:uid][log:%s]" % (log))

        if ip is not None and re.match(r'[0-9\.a-zA-Z:]+$', ip):
            val_ip = '%s\x01%d\t%s\t%s' % (day, 2, ip, campaign_id)
            #val_ip_tmp = '%d\t%s\t%s' % (2, ip, campaign_id)
            #ip_hash_tmp = str(hex(hash(val_ip_tmp)))[2:8]
            #val_ip = '%s%s\x01%s' %(day, ip_hash_tmp, val_ip_tmp)
        else:
            val_ip = None
            log_utils.w("[invalid log:ip][log:%s]" % (log))
        data_list.append([filter_key, val_uid, val_ip, day])
        return log_time
    except Exception,e:
        log_utils.w("log => %s" % log)
        log_utils.w(e, exc_info=True)
        return None

def putIfNotPresents(table, rowlist):
    time1 = time.time()
    rows = table.rows(rowlist)
    dup_list = []
    for key, data in rows:
        dup_list.append(key.split('\x01', 1)[1])
    time2 = time.time()
    b = table.batch()
    for row in rowlist:
        #table.put(row, {'cf:e': '1'})     #最慢
        #table.counter_inc(row, 'cf:d')    #duplicated 次之
        b.put(row, {'cf:e': '1'})          #最快
    b.send()
    time3 = time.time()
    log_utils.i("put if not presents batch: %s, get cost: [%.3lf] ms, put cost: [%.3lf] ms"  %(str(len(rowlist)), (time2-time1)*1000.0, (time3-time2)*1000.0))
    return dup_list

def getRowKey(rowkey):
    region = hash(rowkey) % g_config.g_hbase_region
    row = str(region) + '\x01' + rowkey
    return row

def updateHbaseTable(table, rowlist):
    time1 = time.time()
    #rows = table.rows(rowlist)
    #val_dict = {}
    #for rowk, data in rows:
        #val_dict[rowk] = int(data['cf:imp'])
    #time2 = time.time()
    b = table.batch()
    for row_day in rowlist:
        imp = g_memcache.get(row_day[0], row_day[1])
        b.put(row_day[0], {'cf:imp': str(imp)})
    b.send()
    time2 = time.time()
    #log_utils.i("batch data: %s" %(','.join(rowlist)))
    log_utils.i("insert uid and ip batch: %s, put cost: [%.3lf] ms"  %(str(len(rowlist)), (time2-time1)*1000.0))

def insert_batch_data(rtable, ftable, data_list):
    step = g_config.g_hbase_batch_num
    length = len(data_list)
    sp = 0
    while sp + step < length:
        insert_hbase_batch_data(rtable, ftable, data_list[sp:sp+step])
        sp += step
    if sp < length:
        insert_hbase_batch_data(rtable, ftable, data_list[sp:length])

def insert_hbase_batch_data(rtable, ftable, data_list):
    time1 = time.time()
    filter_key_list = []
    for elem in data_list:
        filter_key_list.append(getRowKey(elem[0]))
    dup_key_list = putIfNotPresents(ftable, filter_key_list) 
    time2 = time.time()
            
    uid_row_list = []
    ip_row_list = []
    for elem in data_list:
        filter_key = elem[0]
        val_uid = elem[1]
        val_ip = elem[2]
        day = elem[3]
        if filter_key not in dup_key_list:
            if val_uid is not None:
                uid_row_list.append([getRowKey(val_uid), day])
            if val_ip is not None:
                ip_row_list.append([getRowKey(val_ip), day])
        else:
            log_utils.w('duplicate impression found:elem=%s' % elem)

    uid_row_list_len = len(uid_row_list)
    ip_row_list_len = len(ip_row_list)

    row_list = uid_row_list
    row_list.extend(ip_row_list)
    updateHbaseTable(rtable, row_list)
    #updateHbaseTable(rtable, uid_row_list)
    #updateHbaseTable(rtable, ip_row_list)
    time3 = time.time()
    log_utils.i("insert uid batch: %s, ip batch: %s, cost: [%.3lf] ms; duplicated: %s, cost: [%.3lf] ms" %(str(uid_row_list_len), str(ip_row_list_len), (time3-time2)*1000.0, str(len(dup_key_list)), (time2-time1)*1000.0))

def current_day_str():
    return str(date.today())

def last_day_str():
    d = datetime.datetime.now() - datetime.timedelta(days=1)
    return d.strftime('%Y-%m-%d')

def dump_thread(interval, file_name):
    log_utils.i("start dump thread")
    last_dump_time = time.time()
    while not g_interrupt:
        now = time.time()
        if now - last_dump_time >= interval:
            try:
                sign_file = open(file_name + ".done", "w")
                sign_file.write("1")
                sign_file.close()

                prefix = current_day_str()
                flag = g_memcache.dump(prefix, file_name, False)
                if flag == 0:
                    last_dump_time = time.time()
                    continue
                if flag == -1:
                    log_utils.e("fail dump new data with prefix %s cost %d ms, cache mem keys: %s, size=%d" % (prefix, (time.time() - now)*1000, g_memcache.getkey(), g_memcache.size()))
                    last_dump_time = time.time()
                    continue
                #dest_file = g_config.g_dest_file_name
                #if os.path.exists(dest_file):
                #    os.remove(dest_file)
                #shutil.move(file_name, dest_file)
                os.system('sh tar_data.sh')

                sign_file = open(file_name + ".done", "w")
                sign_file.write("0")
                sign_file.close()
                log_utils.i("dump new data with prefix %s cost %d ms, cache mem keys: %s, size=%d" % (prefix, (time.time() - now)*1000, g_memcache.getkey(), g_memcache.size()))
                last_dump_time = time.time()
            except Exception, e:
                sign_file = open(file_name + ".done", "w")
                sign_file.write("-1")
                sign_file.close()
                log_utils.e(e, exc_info=True)
            #os.chdir(cur_dir)
        time.sleep(5)
    log_utils.i("end dump thread")

def clear_thread():
    log_utils.i("start clear thread")
    day_str = current_day_str()
    #day_str = "2017-01-10"
    while not g_interrupt:
        current =  current_day_str()
        if current == day_str:
            time.sleep(5)
            continue
        hour = int(time.strftime("%H"))
        if hour < 1:
            time.sleep(5)
            continue
        try:
            start_time = time.time()
            prefix = last_day_str()
            file_name = './data/frequency_data.txt.' + prefix
            g_memcache.dump(prefix, file_name, True)
            g_memcache.clear(prefix)
            day_str = current
            log_utils.i("clear old data with prefix %s cost %d s" % (prefix, time.time() - start_time))
        except Exception, e:
            log_utils.f("fail to clear old data")
            log_utils.f(e, exc_info=True)
    log_utils.i("end clear thread")

def signal_term_handler(sig, frame):
    log_utils.i("exit signal received")
    global g_interrupt
    g_interrupt = True


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_term_handler)
    signal.signal(signal.SIGTERM, signal_term_handler)
    log_utils.init_log("./log/freq_writer", level=logging.INFO)
    try:
        thread_list = [
            threading.Thread(target=dump_thread, name="dump_thread",
                             args=(g_config.g_dump_interval, g_config.g_dump_file_name)),
            threading.Thread(target=clear_thread, name="clear_thread"),
            threading.Thread(target=main, name="compute_thread")
        ]
        for thread in thread_list:
            thread.start()
        while not g_interrupt:
            time.sleep(1)
        log_utils.i("end main thread")
        for thread in thread_list:
            thread.join()
        log_utils.i("all thread exited")
    except Exception, e:
        log_utils.f("fail to run")
        log_utils.f(e, exc_info=True)

