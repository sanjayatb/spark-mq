package com.stb.spark.mq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import static com.ibm.mq.constants.CMQC.HOST_NAME_PROPERTY;

public class MQManagerFactory {

    private MQManagerFactory(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(MQManagerFactory.class);
    private static final Map<String,MQQueueManager> MQ_MANAGERS = new HashMap<>();

    public static MQQueueManager getOrCreate(String queueManager, Hashtable<String,Object> options) {
        try {
            String key = queueManager+"-"+ options.get(HOST_NAME_PROPERTY);
            MQQueueManager mqQueueManagerObject = MQ_MANAGERS.get(key);
            if(null == mqQueueManagerObject){
                mqQueueManagerObject = new MQQueueManager(queueManager,options);
                MQ_MANAGERS.put(key,mqQueueManagerObject);
            }
            return mqQueueManagerObject;
        } catch (MQException e) {
            LOGGER.error("Fail to get MQ manager",e);
        }
        return null;
    }

}
