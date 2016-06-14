# coding: utf-8

from odps import ODPS
from pymongo import MongoClient
import sys
import ConfigParser
import json


config_file = sys.argv[1]
print "config file is %s" % config_file
config = ConfigParser.RawConfigParser()
config.read(config_file)
SECTION_NAME = "DEFAULT"

client = MongoClient(config.get(SECTION_NAME, "mongo_server_ip"), config.getint(SECTION_NAME, "mongo_port"))
db_name = config.get(SECTION_NAME, "mongo_db")
db = client[db_name]

import datetime
import time
today = datetime.date.fromtimestamp(time.time())

# 生成timerange函数
def get_time_range(bizdate):
    if not bizdate:
        bizdate= time.time()
    end_date = datetime.datetime.fromtimestamp(bizdate).replace(hour=0,minute=0, second=0, microsecond=0)
    start_date = end_date + datetime.timedelta(days=-1)
    print start_date
    return [start_date, end_date]


# ODPS需要做的事情
# 1：获取表名，判断是否存在，不存在创建
# 2：根据时间创建分区
# 3：遍历result写入数据
#
# 先定义几个初始变量:
# 1: table name
# 2: 是否全表同步


#table_name:odps的表名
#is_child: 是否属于非父的表，即是child表
#mongo_table_name: mongo 对应的表名
#src_hierarchy:来源表数据层级: abc.xyz.efg=>table['abc']['xyz']['efg'], parent_id自动使用table['abc']['xyz']['_id']
#column_names: 同步的列名列表(map[mongo_column_name, odps_column_name])
class odps_table:
    def __init__(self, table_name, is_child, mongo_table_name, src_hierarchy, column_names, parent_id_name):
        self.table_name = table_name
        self.is_child = is_child
        self.mongo_table_name = mongo_table_name
        self.src_hierarchy = src_hierarchy
        self.column_names = column_names
        self.parent_id_name = parent_id_name




class parent_odps_table:
    def __init__(self, parent_odps_table):
        self.parent_odps_table = parent_odps_table
        self.child_tables = []

    def add_child_tables(self, *tables):
        self.child_tables = tables



columns_map = json.loads(config.get(SECTION_NAME, "columns_map"))
child_tables = json.loads(config.get(SECTION_NAME, "child_tables"))
child_table_names = json.loads(config.get(SECTION_NAME, "child_table_names"))
parent_table_name = json.loads(config.get(SECTION_NAME, "parent_table_name"))

idColumn = config.get(SECTION_NAME, "id_column")
accessId = config.get(SECTION_NAME, "odps_access_id")
accessKey = config.get(SECTION_NAME, "odps_access_key")
odps_project = config.get(SECTION_NAME, "odps_project")
end_point = config.get(SECTION_NAME, "odps_end_point")
bizdate = config.get(SECTION_NAME, "bizdate")


parent_table = odps_table(parent_table_name[0], False, parent_table_name[0].replace("s_", ""), None, columns_map, idColumn)
root_table = parent_odps_table(parent_table)
root_table.child_tables = []
mongo_parent_table_name = parent_table_name[0].replace("s_", "")

#init child tables informations
for (index,name) in enumerate(child_table_names, start=0):
    o = odps_table(name, True, parent_table_name[0].replace("s_", ""), \
        name.replace(parent_table_name[0] + "_", ""), child_tables[index], idColumn)
    root_table.child_tables.append(o)

#print child table info
for c in root_table.child_tables:
    print "mongo_table_name is %s,odps table_name is %s" % (c.mongo_table_name, c.table_name)
    print "columns is : "
    for name in c.column_names:
        print "name is :%s" % name

#get mongo db table name
dynamic = db[mongo_parent_table_name]

def getValue(mongo_name, temp):
    temp_r = None
    for s in mongo_name.split("."):
        if not temp_r:
            temp_r = temp.get(s, "")
            continue
    if isinstance(temp_r, dict):
        return str(temp_r.get(s, ""))
    elif isinstance(temp_r, list):
        return ','.join(temp_r)
    elif isinstance(temp_r,  unicode):
        return temp_r.encode('utf-8')
    else:
        return str(temp_r)

def getChildHierarchy(src_hierarchy, tr):
    if not src_hierarchy:
        return None
    names = src_hierarchy.split(".")
    t = None
    for (index, name) in enumerate(names):
        if not t:
            t = tr.get(name)
            if not t or isinstance(t, list):
                break
            continue
        t = t.get(name)
        if not t or isinstance(t, list):
            break
    return t

def getChildRecords(child_table, tr):
    if not child_table.is_child:
        return None
    if not child_table.src_hierarchy:
        return None
    odps_table = child_table.odps_table
    parent_id_value = tr.get(child_table.parent_id_name)
    parent_id = isinstance(parent_id_value, unicode) and parent_id_value.encode('utf-8') or str(parent_id_value)
    #split src_hierarchy
    rootValues = getChildHierarchy(child_table.src_hierarchy, tr)
    if not rootValues:
        return None
    records = []
    for r in rootValues:
        odps_record = odps_table.new_record()
        for (index, columnName) in enumerate(child_table.column_names):
            odps_record[index] = getValue(columnName, r)
        odps_record[len(child_table.column_names)] = parent_id
        records.append(odps_record)
    return records


odps = ODPS(accessId, accessKey, odps_project, end_point or 'http://service.odps.aliyun.com/api')

from odps.models import Schema, Column, Partition
def get_odps_columns(column_names, is_child=False):
    p_odps_columns = []
    for (index, key) in enumerate(column_names):
        p_odps_columns.append(Column(name=column_names[key],type="string"))
    if is_child:
        p_odps_columns.append(Column(name="parentId",type="string"))
    return p_odps_columns

def get_odps_partition():
    return [Partition(name='pt', type='string', comment='the partition')]

def get_schema(odps_columns, odps_partitions):
    return Schema(columns=odps_columns, partitions=odps_partitions)

def get_partition_value(bizdate=""):
    if bizdate:
        return bizdate
    today = datetime.date.fromtimestamp(time.time())
    today = today + datetime.timedelta(days=-1)
    return today.strftime("%Y%m%d")

def create_partition_value(partition, odps_table):
    if not partition:
        print "partition 不能为空"
    else:
        print "尝试创建新的分区pt=%s" % partition
        print "started to drop partition %s" % partition
        odps_table.delete_partition('pt=' + partition,if_exists=True)
        print "started to create partition %s" % partition
        print odps_table.create_partition("pt=" + partition, if_not_exists=True)

def create_table_if_not_exists(root_table):
    if not odps.exist_table(root_table.parent_odps_table.table_name):
        print "parent table does not exist,so we create one %s" % root_table.parent_odps_table.table_name
        columns = get_odps_columns(root_table.parent_odps_table.column_names)
        root_table.parent_odps_table.columns = columns
        odps.create_table(root_table.parent_odps_table.table_name,
                          get_schema(columns,
                                     get_odps_partition()), if_not_exists=True)
    for child in root_table.child_tables:
        if not odps.exist_table(child.table_name):
            columns = get_odps_columns(child.column_names, is_child=True)
            print "child table does not exist,so we create one %s" % root_table.parent_odps_table.table_name
            odps.create_table(child.table_name,
                          get_schema(columns,
                                     get_odps_partition()), if_not_exists=True)

def init_partition(root_table, bizdate=""):
    #init parent table partition
    print "start to init parent_table:%s partition" % root_table.parent_odps_table.table_name
    create_partition_value(get_partition_value(bizdate), root_table.parent_odps_table.odps_table)
    #init childs table partitions
    for child_table in root_table.child_tables:
        print "start to init child table:%s" % child_table.table_name
        create_partition_value(get_partition_value(bizdate), child_table.odps_table)

#初始化设置打开odps table的连接信息
def init_root_table_odps(root_table):
    root_table.parent_odps_table.odps_table = odps.get_table(root_table.parent_odps_table.table_name)
    for c in root_table.child_tables:
        c.odps_table = odps.get_table(c.table_name)

import time
from odps.tunnel import TableTunnel

num = 0
#初始化相关
print "init table and set odps table attribute"

create_table_if_not_exists(root_table)
init_root_table_odps(root_table)
init_partition(root_table)

#parent table data written
print "start to sync parent table data....."
result = dynamic.find()
tunnel = TableTunnel(odps)
print "partition values is %s" % get_partition_value(bizdate)
upload_session = tunnel.create_upload_session(root_table.parent_odps_table.table_name, \
 partition_spec='pt=' + get_partition_value(bizdate))
with upload_session.open_record_writer(block_id=1) as writer:
    for tr in result:
        num = num + 1
        if num % 1000 == 0:
            print "%d num records has been sent to odps..." % num
        record = root_table.parent_odps_table.odps_table.new_record()
        for (index, columnName) in enumerate(root_table.parent_odps_table.column_names):
            record[index] = getValue(columnName, tr)
        writer.write(record)
upload_session.commit([1])

print "start to sync childs table data...."
#child tables written
for ct in root_table.child_tables:
    result = dynamic.find()
    upload_session = tunnel.create_upload_session(ct.table_name, partition_spec='pt=' + get_partition_value(bizdate))
    num = 0
    with upload_session.open_record_writer(block_id=1) as writer:
        for tr in result:
            num = num + 1
            if num % 1000 == 0:
                print "%d num records has been sent to odps..." % num
            records = getChildRecords(ct, tr)
            if records:
                for r in records:
                    writer.write(r)
    upload_session.commit([1])
print "============sync finished!============="
