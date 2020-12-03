package com.stb.spark.mq;

import org.apache.spark.sql.execution.streaming.Offset;

public class MQSourceOffset extends Offset {

    private long offset;

    public MQSourceOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String json() {
        return String.valueOf(offset);
    }

    @Override
    public boolean equals(Object o) {
       return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
