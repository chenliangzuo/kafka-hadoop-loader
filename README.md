# KAFKA-HADOOP-LOADER
``komlei``  
``QQ:122726894``

### 简介
1.通过MR的方式并发消费kafka中的数据,按照日期落到HDFS指定目录
2.生产环境中常遇到根据时间戳去落HDFS的目录,一天一个文件夹,不同日期在不同路径
3.通过kafka的simple-consumer提高数据发送落盘的准确性


### 代码导读
##### utils
1.kafka-zk-utils
```
使用curator操作kafka在zk上的相关信息
-获取分区的leader
-znode的数据解析
-消费的偏移量读写
```

2.CheckpointManager
```
通过zk的znode和hdfs:tmp路径下两种方式存储消费的偏移量管理
```

##### input

1.kafkaInputFormat

```
自定义切片方式,决定mapper的数量
每个分区对应一个mapper,获取到分区的leader
```

2.KafkaInputSplit

```
切片javabean信息封装
private String brokerId;    //102
private String broker;      //slave1:9092
private int partition;      //分区号
private String topic;       //topic
private long startOffset;   //消费偏移量起始
```

3.KafkaInputRecordReader

```
nextKeyValue():
ByteBufferMessageSet messages为空则开始fetch
否则：
key = new MsgMetaKeyWritable(split, messageOffset.offset());
if (message.isNull()) {
    value.set("");
} else {
    String data = new String(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
    value.set(data);
}
```

#####output

1.MultiOutputFormat
```
1.是否压缩
2.return MultiOutRecordWriter()
```

2.MultiOutRecordWriter
```
buildSuffixPath(MsgMetaKeyWritable key)创建具体路径的具体文件的writer
---自定义时间戳获取类or  key使用系统时间
if (hasTS || key.getTimestamp() != null) {
    suffixPath = timeFormat.format(key.getTimestamp());
} else {
    suffixPath = pathFormat.replaceAll("'", "");
}
```

##### mapper

HadoopJobMapper
```
运行这个项目的基本M,简单判断是否使用自定义实现时间戳类否则使用系统时间
```

##### start

StartJob
```
MultiOutputFormat.configurePathFormat(conf, "'{T}/'yyyy-MM-dd'/'"); //路径配置
conf.setClass(CONFIG_TIMESTAMP_EXTRACTOR_CLASS, TimeStampExample.class, TimestampExtractor.class);  //自定义时间戳获取类
```


### TO USE
1.项目路径下 maven package
2.cd ${pj}/bin/start-load.sh
```
sh start-load.sh -remote localhost:2281 test test-group /tmp latest off
```

-remote 开启远程调试,二次开发可使用
后续参数依次为：zk地址    kafka的topic group-name  hdfs路径  从当前or最初消费   是否压缩
