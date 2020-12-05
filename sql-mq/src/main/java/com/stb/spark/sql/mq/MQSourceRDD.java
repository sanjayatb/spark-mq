package com.stb.spark.sql.mq;

import com.ibm.mq.*;
import com.stb.spark.sql.mq.iterators.IteratorFactory;
import com.stb.spark.sql.mq.iterators.MultiIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ibm.mq.constants.CMQC.*;
import static com.stb.spark.sql.mq.Contants.*;

/***
 * RDD creation class
 */
public class MQSourceRDD extends RDD<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQSourceRDD.class);
    public static final StructType SCHEMA = new StructType().add("value", DataTypes.BinaryType);
    private static final ClassTag<byte[]> EVIDENCE = ClassTag$.MODULE$.apply(byte[].class);
    private static final List<Dependency<?>> EMPTY_DEPENDENCY = new ArrayList<>();
    private final MQResource resource;
    private final Map<String, String> options;
    private String movePath;
    private List<String> movePaths;

    public MQSourceRDD(SparkContext context, MQResource resource, Map<String, String> options) {
        super(context, JavaConversions.asScalaIterator(EMPTY_DEPENDENCY.iterator()).toSeq(), EVIDENCE);
        this.resource = resource;
        this.options = options;
        movePath = options.get("movePath").get();
        movePaths = Arrays.asList(movePath.split(","));
    }

    private MQQueueManager getQueueManager() {
        if (StringUtils.isNotBlank(resource.getErrorQueueManagerName())) {
            return resource.getErrorQManager();
        } else {
            return resource.getQManager();
        }
    }

    public Iterator<byte[]> compute(Partition partition, TaskContext context) {
        MultiIterator<byte[]> multiIterator = new MultiIterator<>();

        context.addTaskFailureListener((tc, throwable) -> {
            MQQueueManager manager = getQueueManager();
            try {
                if (StringUtils.isNotBlank(resource.getBackoutQueue())) {
                    MQQueue queue = manager.accessQueue(resource.getBackoutQueue(), MQResource.QUEUE_OPEN_OPTION_WRITE);
                    multiIterator.forEachRemaining(m -> writeMessage(queue, m));
                    queue.close();
                    LOGGER.info("Write fail message to {} queue", resource.getBackoutQueue());
                } else if (StringUtils.isNotBlank(resource.getErrorQueueName())) {
                    MQQueue queue = manager.accessQueue(resource.getErrorQueueName(), MQResource.QUEUE_OPEN_OPTION_WRITE);
                    multiIterator.forEachRemaining(m -> writeMessage(queue, m));
                    queue.close();
                    LOGGER.info("Write fail message to {} queue", resource.getErrorQueueName());
                } else {
                    LOGGER.error("No backout queue");
                }
                manager.commit();
            } catch (MQException e) {
                LOGGER.error("Fail to send backout queue", e);
            }
        });

        try {
            MQQueueManager queueManager = resource.getQManager();
            MQQueue queue = queueManager.accessQueue(resource.getQueueName(), MQResource.QUEUE_OPEN_OPTION_READ);

            boolean doneReading = false;
            String sourceFormat = !options.get(FILE_FORMAT).isDefined() ? XML_FORMAT : options.get(FILE_FORMAT).get();
            java.util.Map<String, String> mapOptions = JavaConverters.mapAsJavaMapConverter(options).asJava();
            HashMap<String, String> map = new HashMap<>();
            mapOptions.entrySet().stream().forEach(entry -> map.put(entry.getKey(), entry.getValue()));

            while (!doneReading) {
                byte[] data = getMessage(queue);
                doneReading = data == null;

                if (!doneReading) {
                    InputStream inputStream = new ByteArrayInputStream(data);
                    java.util.Iterator<byte[]> msgIterator = IteratorFactory.create(sourceFormat,
                            inputStream, mapOptions, null);
                    multiIterator.add(msgIterator);
                    writeToFile(data, "MQ_" + options.get(DATA_SOURCE).get());
                }
            }

            queue.close();
            queueManager.commit();
        } catch (MQException e) {
            LOGGER.error("Fail to read message from queue");
        }
        return JavaConversions.asScalaIterator(multiIterator);
    }

    private byte[] getMessage(MQQueue queue) {
        MQMessage mqMessage = new MQMessage();
        MQGetMessageOptions getOptions = new MQGetMessageOptions();

        getOptions.options = MQResource.GET_OPTIONS_CONSTANT;
        getOptions.waitInterval = 1;
        byte[] output = null;

        try {
            queue.get(mqMessage, getOptions);
            if (mqMessage.messageType == MQMT_REPORT) {
                output = mqMessage.correlationId;
            } else {
                output = new byte[mqMessage.getDataLength()];
                mqMessage.readFully(output);
            }
        } catch (MQException | IOException e) {
            LOGGER.error("Fail to get message", e);
        }
        return output;
    }

    private void writeToFile(byte[] data, String interfaceType) {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        movePaths.stream().forEach(movePath -> {
            Path pathSrc = new Path(movePath);
            String dateTime = new StringBuilder(new SimpleDateFormat(SIMPLE_DATE_FORMAT)
                    .format(new Date())).append("_").append(UUID.randomUUID()).append("_").toString();
            Path destfilePath = new Path(movePath + File.separator + dateTime + interfaceType);
            OutputStream out = null;
            try (ByteArrayInputStream in = new ByteArrayInputStream(data);
                 FileSystem fs = pathSrc.getFileSystem(conf)) {
                out = fs.create(destfilePath);
                IOUtils.copyBytes(in, out, conf);
                out.close();
                LOGGER.info("MQ Data archive successfully", destfilePath);
            } catch (Exception e) {
                LOGGER.error("Error happen while handling file", destfilePath, e);
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException e) {
                    LOGGER.error("Fail to Archive");
                }
            }
        });
    }

    public Partition[] getPartitions() {
        AtomicInteger partitionCounter = new AtomicInteger(0);
        List<String> executors = JavaConversions.seqAsJavaList(super.context().getExecutorIds());
        if (executors.isEmpty()) {
            executors = Arrays.asList("driver");
        }
        return executors.stream().map(
                ex -> new MQSourceRDDPartition(partitionCounter.getAndIncrement()))
                .collect(Collectors.toList()).toArray(new MQSourceRDDPartition[]{});
    }

    private void writeMessage(MQQueue queue, byte[] data) {
        MQMessage message = new MQMessage();
        message.messageFlags = MQMF_SEGMENTATION_ALLOWED;

        if (resource.getAckQueue() != null) {
            message.replyToQueueManagerName = resource.getQueueManagerName();
            message.replyToQueueName = resource.getAckQueue();
            message.report = MQRO_COA | MQRO_COD;
        }
        try {
            message.write(data);
            queue.put(message);
        } catch (IOException | MQException e) {
            throw new RuntimeException("Fail to write data to MQ");
        }

    }

    private static final class MQSourceRDDPartition implements Partition {
        private final int index;

        public MQSourceRDDPartition(int index) {
            this.index = index;
        }

        @Override
        public int index() {
            return index;
        }
    }

}
