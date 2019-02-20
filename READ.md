# RIVER

## 1. 什么是River

River是一个用于将Hive数据同步至HBase或Redis的工具。

## 2. 快速起步

### 2.1 环境准备

+ Java: JDK7及以上
+ Scala: Scala2.11及以上
+ Spark: Spark2.3.*
+ Hadoop: 2.6.0-cdh5.15.0
+ Hive: 1.1.0-cdh5.15.0
+ HBase: 1.2.0-cdh5.15.0

### 2.2 运行模式

+ 单机模式：对应Spark集群的单机模式
+ standalone模式：对应Spark集群的分布式模式
+ yarn模式：对应Spark集群的yarn模式

### 2.3 打包

进入项目根目录，使用maven打包：

```
# <profile_id> 可选，不指定-P<profile_id>，默认dev：
#   dev     开发模式，会将依赖的Spark、Hadoop、Hive、HBase相关jar包打包
#   prod    生产模式，不会将依赖的Spark、Hadoop、Hive、HBase相关jar包打包
mvn clean package -DskipTests -P<profile_id>
```

打包结束后，项目根目录下会生成target目录，其中的river-1.0-SNAPSHOT.jar即为应用程序包。

### 2.4 启动

#### 2.4.1 任务提交

在安装有Spark客户端的服务器上提交任务。

命令如下：

```
spark-submit --master yarn river-1.0-SNAPSHOT.jar <arg1> <arg2> <arg3>
```

#### 2.4.2 应用程序参数

```
Usage: <arg1> <arg2> <arg3>
       arg1 - 指定数据库类型。可选：redis|hbase。必传
       arg2 - 表字段配置文件路径。必传
       arg3 - redis配置文件路径。<arg1>为redis时，必传
```

#### 2.4.3 配置文件

+ 表字段配置文件

```json
{
  "sql":"",
  "srcTable":"ime.rpt_uc_month",
  "destTable":"test:rpt_uc_month",
  "key":"id",
  "fields": [
    {
      "name":"id",
      "newName":"id",
      "newType":"string"
    },
    {
      "name":"month",
      "datePattern":"yyyy-MM",
      "newName":"month_new",
      "newDatePattern":"yyyy^MM",
      "newType":"string"
    },
    {
      "name":"clk_user_cnt",
      "newType":"long"
    }
  ]
}
```

> sql: 指定源数据查询SQL。程序会执行该SQL语句，将执行结果集作为源数据<br/>
> srcTable: 指定源数据表。程序会将该表中的全部数据作为源数据。若sql项已配置，则忽略本项<br/>
> destTable: 指定结果数据表。如果要将数据同步至HBase，则需现在HBase中手动创建该表。如果要将数据同步至Redis，则该项则作为key的前缀，以:作为结尾进行拼接<br/>
> key: 指定使用Hive中的哪一列作为HBase的row或Redis的key<br/>
> fields: 描述具体的字段信息<br/>
> name: 该字段在Hive表中的名称<br/>
> datePattern: 该字段在Hive中若是以字符串格式存储的日期，在目的表中需要以新的格式存储，则指定该字段在Hive中存储的格式。默认:yyyy-MM-dd HH:mm:ss 
> newName: 指定该字段在目的表中的新名称。若不指定，则取name<br/>
> newDatePattern: 指定在Hive中以日期(或字符串格式的日期)类型存储的字段，在目的表中需要转换的新格式。
> newType: 指定该字段在目的表中的新类型。若不指定，则与Hive中的类型相对应<br/>

+ redis配置文件

```
# Redis Server IP地址。默认localhost
redis.host=172.16.117.61
# Redis Server 端口。默认6379
redis.port=
# 默认2000
redis.connectionTimeout=
# 默认2000
redis.soTimeout=
```