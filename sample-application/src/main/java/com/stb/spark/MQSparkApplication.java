package com.stb.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;

import java.util.concurrent.TimeoutException;

public class MQSparkApplication {

    public static void main(String[] args) {
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
                .option("queueManager","QMangerName")
                .option("channel","channel.name")
                .option("user","SanjayaMqUser")
                .load();

//        Dataset<?> output = input.map(data -> data, Encoders.BYTE()); //transform spark functions
        try {
            input.writeStream()
                    .format("kafka")
                    .outputMode(OutputMode.Append())
                    .option("kafka.bootstrap.servers","serves")
                    .start();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

}
