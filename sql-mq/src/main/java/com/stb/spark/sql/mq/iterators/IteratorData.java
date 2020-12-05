package com.stb.spark.sql.mq.iterators;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

public class IteratorData implements Serializable {

    private final InputStream inputStream;
    private final Map<String,String> mapOptions;
    private final String filePath;

    public IteratorData(InputStream inputStream, Map<String, String> mapOptions, String filePath) {
        this.inputStream = inputStream;
        this.mapOptions = mapOptions;
        this.filePath = filePath;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public Map<String, String> getMapOptions() {
        return mapOptions;
    }

    public String getFilePath() {
        return filePath;
    }

}
