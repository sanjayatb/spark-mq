package com.stb.spark.sql.mq.iterators;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;

public class JsonIterator extends CloseableIterator<byte[]>{

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonIterator.class);
    private JsonReader reader;
    private Gson gson;

    public JsonIterator(IteratorData iteratorData) {
        gson = new GsonBuilder().serializeNulls().create();
        this.reader = new JsonReader(new InputStreamReader(iteratorData.getInputStream()));
        String split = iteratorData.getMapOptions().get("split");

        if(StringUtils.isNotEmpty(split)){
            try {
                reader.beginObject();
                if(!reader.nextName().equals(split)){
                    String errorMsg = "Could not find keyword" + split;
                    LOGGER.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
                reader.beginArray();
            } catch (IOException e) {
               LOGGER.error("Fail to read json data");
            }
        }
    }

    @Override
    protected Closeable getCloseableResource() {
        return reader;
    }

    @Override
    public boolean hasNext() {
        try {
            return reader.peek() != JsonToken.END_ARRAY &&
                    reader.peek() != JsonToken.END_DOCUMENT;
        } catch (IOException e) {
            LOGGER.error("Peek data failed",e);
        }
        return false;
    }

    @Override
    public byte[] next() {
        if(hasNext()){
            JsonElement jsonElement = gson.fromJson(reader,JsonElement.class);
            String text = gson.toJson(jsonElement);
            return text.getBytes();
        }else {
            throw new NoSuchElementException("End of Json File");
        }
    }
}
