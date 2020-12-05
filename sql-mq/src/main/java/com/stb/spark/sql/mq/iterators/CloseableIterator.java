package com.stb.spark.sql.mq.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public abstract class CloseableIterator<T> implements Iterator<T>, Closeable {


    private static final Logger LOGGER = LoggerFactory.getLogger(CloseableIterator.class);
    private boolean closed;

    protected abstract Closeable getCloseableResource();

    public void close() {
        if (getCloseableResource() != null && !closed) {
            try {
                getCloseableResource().close();
            } catch (IOException e) {
                LOGGER.error("Fail to close", e);
            }
        }
    }

    protected boolean isClosed() {
        return closed;
    }

}
