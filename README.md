# mongo_odps_tunnel
从mongodb同步数据到odps的工具类

sync.cfg 
配置odps相关配置，要同步的表名
child_tables表示当Mongo db有子表的情况时候使用
比如
{
  "a":"1",
  "b":[{},{},{}]
}
b单独就是一个表，但是parentid是上层表的id,同步的时候会自动生成parentid
