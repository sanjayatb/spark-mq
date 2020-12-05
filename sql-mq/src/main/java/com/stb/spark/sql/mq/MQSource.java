package com.stb.spark.sql.mq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.Serializable;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MQSource implements the spark source interface
 */
public class MQSource implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQSource.class);

    private static final ClassTag<InternalRow> EVIDENCE = ClassTag$.MODULE$.apply(InternalRow.class);

    private final SQLContext sqlContext;
    private final MQResource resource;
    private final MQQueue queue;
    private final Map<String, String> options;

    private final AtomicLong currentOffset = new AtomicLong(0);

    public MQSource(SQLContext sqlContext, Map<String, String> options) {
        this.sqlContext = sqlContext;
        this.options = options;
        this.resource = new MQResource(options);
        String queueName = resource.getQueueName();
        MQException.logExclude(2033);

        try {
            this.queue = resource.getQManager().accessQueue(queueName, MQResource.QUEUE_OPEN_OPTION_BROWSE);
        } catch (MQException e) {
            LOGGER.error("Fail to get table.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public StructType schema() {
        return MQSourceRDD.SCHEMA;
    }

    @Override
    public Option<Offset> getOffset() {
        try {
            long current = currentOffset.get();
            MQMessage mqMessage = new MQMessage();
            MQGetMessageOptions getMessageOptions = new MQGetMessageOptions();
            getMessageOptions.options = MQResource.QUEUE_OPEN_OPTION_BROWSE;
            queue.get(mqMessage, getMessageOptions);
            currentOffset.compareAndSet(current, System.currentTimeMillis());
            return Option.apply(new MQSourceOffset(System.currentTimeMillis()));
        } catch (MQException e) {
            if (e.getReason() == MQException.MQRC_NO_MSG_AVAILABLE) {
                return Option.apply(null);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        if (end.json().equals(String.valueOf(currentOffset.get()))) {
            MQSourceRDD result = new MQSourceRDD(sqlContext.sparkContext(), resource, options);
            Function1<byte[], InternalRow> function = new FunctionWrapper();
            RDD<InternalRow> rowRDD = result.map(function, EVIDENCE);
            return sqlContext.internalCreateDataFrame(rowRDD, schema(), true);
        } else {
            return sqlContext.internalCreateDataFrame(sqlContext.sparkContext().emptyRDD(EVIDENCE), schema(), true);
        }
    }

    public void commit(Offset end) {
    }

    public void stop() {
        try {
            queue.close();
        } catch (MQException e) {
            LOGGER.error("Fail to close");
        }
    }

    private class FunctionWrapper extends AbstractFunction1<byte[], InternalRow> implements Serializable {

        @Override
        public InternalRow apply(byte[] bytes) {
            Seq<Object> result = JavaConversions.asScalaBuffer(Arrays.asList((Object) bytes)).toSeq();
            return InternalRow.apply(result);
        }
    }

}
