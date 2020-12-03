package com.stb.spark.mq;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

public class MQSourceProvider implements DataSourceRegister, StreamSourceProvider,scala.Serializable {
    @Override
    public String shortName() {
        return "mq";
    }

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return Tuple2.apply("mq",MQSourceRDD.SCHEMA);
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return new MQSource(sqlContext,parameters);
    }
}
