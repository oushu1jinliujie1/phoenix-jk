#!/usr/bin/python
# coding=utf-8
"""
@Time           :   2022-08-08 16:39
@Author         :   LSC
@File           :   test.py
@version        :   V1.0
@Python Version :   python 3.10.2
"""
try:
    import sys, json, logging,os,io
    import datetime
    from time import strftime
    sys.path.append(sys.path[0]+'/../python/site-packages/JPype1-1.4.0-py3.10-linux-x86_64.egg')
    sys.path.append(sys.path[0]+'/../python/site-packages/JayDeBeApi-1.2.3-py3.10.egg')
    import configparser
    import jaydebeapi
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you '
             'have sourced greenplum_path.sh.  Detail: ' + str(e))

now=datetime.datetime.now()
logger = logging.getLogger("1.log")
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
file_handler = logging.FileHandler(sys.path[0]+"/../logs/phoenix_"+now.strftime("%Y%m%d")+".log")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

class Conf:
    def __init__(self):
        self.conf = configparser.ConfigParser()
        self.root_path = os.path.dirname(os.path.abspath(__file__))
        self.f = os.path.join(self.root_path + "/../conf/config.conf")
        self.conf.read(self.f)
    def read_sections(self):
        print(self.conf.sections())
    def read_options(self, s1, s2):
        print(self.conf.options(s1))
        print(self.conf.options(s2))
    def read_conf(self, m, n):
        name = self.conf.get(m, n)
        return name

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

import jaydebeapi

def CheckColExists(curs,sql):
  try:
    curs.execute(sql)
  except Exception as e:
    print(e)
    return False
  return True

def InsertPhoenix(curs,sql):
    try:
        curs.execute(sql)
    except Exception as e:
        print(e)
        return  False
    return  True

if __name__ == '__main__':
    options=sys.argv[1]
    try:
        case_switch=sys.argv[2]
        if case_switch !='0' and case_switch != '1':
            exit(1)
    except:
        case_switch=0
    base_dir=sys.path[0]
    filename=options
    tableNames=''
    if os.path.exists(filename):
        json_body=io.open(filename,'r',encoding='utf-8').read();
    else:
        logger.error("传入文件不存在，文件名称为："+filename)
        sys.exit("传入文件不存在")
    if is_json(json_body):
        logger.info("Start Running")
        dict_options=json.loads(json_body)
        queryName=str(dict_options['queryName'])
        for i in dict_options['userTables']:
            if tableNames != '':
                tableNames=tableNames+','
            tableNames=tableNames+i['schemaName']+'.'+i['tableName']
        connection=str(dict_options['connection'])
        if case_switch=='1':
            tableNames=tableNames.upper()
            connection=connection.upper()
        phoenix_conf=Conf()
        hbase_url=phoenix_conf.read_conf('phoenix','hbase_url')
        jdbc_client=phoenix_conf.read_conf('phoenix','jdbc_client')
        conn = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver','jdbc:phoenix:'+hbase_url,[ '', ''],jdbc_client)
        curs=conn.cursor()
        for tableName in tableNames.split(','):
            check_sql='select "'+connection+'" from "'+tableName.split('.')[0]+'"."'+tableName.split('.')[1]+'" limit 1'
            logger.info(check_sql)
            logger.info("检查字段是否存在")
            if CheckColExists(curs,check_sql):
                logger.info("字段 "+connection+"在表 "+tableName+" 中存在")
            else:
                logger.info("字段 "+connection+"在表 "+tableName+" 中不存在")
                exit(1)
        logger.info("将数据插入元数据表")
        insert_sql="upsert into ct.os_meta values('"+queryName+"','"+tableNames+"','"+connection+"')"
        if InsertPhoenix(curs,insert_sql):
            conn.commit()
            logger.info("数据插入元数据表成功")
        else:
            exit(1)
        curs.close()
        conn.close()
    else:
        logger.error("Please enter the correct JSON format!")
        exit(1)

