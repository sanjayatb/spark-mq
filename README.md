# spark-mq
Mq source for spark

This is overriding spark Source API and given custom read capability to IBM MQ data source.
u

### Example
```
SparkSession _sparkSession = SparkSession
                .builder()
                .appName("MQ Spark application")
                .master("local[*]")
                .getOrCreate();


        Dataset<?> input = _sparkSession.readStream()
                .format("mq")
                .option("interfaceFileType","TEST")
                .option("queue","queue.name")
                .option("host","hostname")
                .option("port","1980")
                .option("user","SanjayaMqUser")
                .option("queueManager","QMangerName")
                .option("channel","channel.name")
                .option("error.queue","error.queue.name")
                .option("error.queueManager","errorQMangerName")
                .option("error.host","error.hostname")
                .option("error.port","1980")
                .option("error.user","SanjayaMqUser")
                .option("fileFormat","XML")  //JSON, FIXED_WIDTH, can add custom iterators 
                .option("split","splitTagName")  //XML array split tag which split blocks to Spark Row
                .load();
 ```

