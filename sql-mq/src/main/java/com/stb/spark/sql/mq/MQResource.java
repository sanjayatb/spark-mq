package com.stb.spark.sql.mq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction0;

import java.io.Serializable;
import java.util.Hashtable;

import static com.ibm.mq.constants.CMQC.*;

/***
 * MQResource class
 */
public class MQResource implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQResource.class);

    public static final int QUEUE_OPEN_OPTION_INQUIRE = MQOO_INQUIRE;
    private static final int[] BACKOUT_CONSTANTS = {MQIA_BACKOUT_THRESHOLD, MQCA_BACKOUT_REQ_Q_NAME};

    private final Hashtable<String, Object> options;
    private final String queueManagerName;
    private final String queueName;
    private final String backoutQueue;
    private final String ackQueue;
    private final String errorQueueManagerName;
    private final String errorQueueName;
    private final Hashtable<String, Object> errorOptions;

    public static final int GET_OPTIONS_CONSTANT = MQGMO_WAIT | MQGMO_PROPERTIES_COMPATIBILITY
            | MQGMO_ALL_SEGMENTS_AVAILABLE | MQGMO_COMPLETE_MSG | MQGMO_ALL_MSGS_AVAILABLE |
            MQGMO_SYNCPOINT;
    public static final int QUEUE_OPEN_OPTION_BROWSE = MQOO_INPUT_AS_Q_DEF | MQOO_FAIL_IF_QUIESCING;
    public static final int QUEUE_OPEN_OPTION_READ = MQOO_INPUT_AS_Q_DEF | MQOO_FAIL_IF_QUIESCING;
    public static final int QUEUE_OPEN_OPTION_WRITE = MQOO_OUTPUT | MQOO_PASS_ALL_CONTEXT;

    public MQResource(Map<String, String> options) {
        this.options = new Hashtable<>();
        this.options.put(HOST_NAME_PROPERTY, options.get("host").getOrElse(new ScalaUtils.DefaultEmptyFunction()));
        this.options.put(CHANNEL_PROPERTY, options.get("channel").getOrElse(new ScalaUtils.DefaultEmptyFunction()));
        this.options.put(USER_ID_PROPERTY, options.get("user").getOrElse(new ScalaUtils.DefaultEmptyFunction()));
        this.options.put(PORT_PROPERTY, Integer.parseInt(options.get("port").getOrElse(new ScalaUtils.DefaultZeroFunction())));
        this.options.put(TRANSPORT_PROPERTY, TRANSPORT_MQSERIES);

        this.errorOptions = new Hashtable<>();
        this.errorOptions.put(HOST_NAME_PROPERTY, options.get("error: host").getOrElse(new ScalaUtils.DefaultEmptyFunction()));
        this.errorOptions.put(CHANNEL_PROPERTY, options.get("error.channel").getOrElse(new ScalaUtils.DefaultEmptyFunction()));
        this.errorOptions.put(USER_ID_PROPERTY, options.get("error.user").getOrElse(new ScalaUtils.DefaultEmptyFunction()));
        this.errorOptions.put(PORT_PROPERTY, Integer.parseInt(options.get("error.port").getOrElse(new ScalaUtils.DefaultZeroFunction())));
        this.errorOptions.put(TRANSPORT_PROPERTY, TRANSPORT_MQSERIES);

        this.queueName = options.get("queue").get();
        this.queueManagerName = options.get("queueManager").get();
        this.ackQueue = options.get("ackQueue").getOrElse(new ScalaUtils.DefaultNullFunction());
        this.errorQueueName = options.get("error queue").getOrElse(new ScalaUtils.DefaultNullFunction());
        this.errorQueueManagerName = options.get("error.queueManager").getOrElse(new ScalaUtils.DefaultNullFunction());
        if (StringUtils.isBlank(this.errorQueueName)) {
            this.backoutQueue = (String) options.get("backoutQueue").getOrElse(new BackoutFunction());
        } else {
            LOGGER.info("[{}] queue -> using error queue : {}", queueName, errorQueueName);
            this.backoutQueue = "";
        }
    }

    public Hashtable<String, Object> getOptions() {
        return options;
    }

    public String getQueueManagerName() {
        return queueManagerName;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getBackoutQueue() {
        return backoutQueue;
    }

    public String getAckQueue() {
        return ackQueue;
    }

    public String getErrorQueueManagerName() {
        return errorQueueManagerName;
    }

    public String getErrorQueueName() {
        return errorQueueName;
    }

    public Hashtable<String, Object> getErrorOptions() {
        return errorOptions;
    }

    public MQQueueManager getQManager() {
        return MQManagerFactory.getOrCreate(getQueueManagerName(), options);
    }

    public MQQueueManager getErrorQManager() {
        return MQManagerFactory.getOrCreate(getErrorQueueManagerName(), errorOptions);
    }

    private class BackoutFunction extends AbstractFunction0 implements scala.Serializable {
        @Override
        public String apply() {
            int[] threshold = new int[1];
            char[] queueNameChar = new char[48];

            try {
                MQQueue queue = getQManager().accessQueue(queueName, QUEUE_OPEN_OPTION_INQUIRE);

                queue.inquire(BACKOUT_CONSTANTS, threshold, queueNameChar);
                String backout = String.valueOf(queueNameChar);
                LOGGER.info("{{}} queue ->  using backout queue {{}}", queueName, backout);
                return backout;
            } catch (MQException e) {
                LOGGER.error("Error retrieving the backout queue [{}]", queueName, e);
                return null;
            }
        }
    }

}
