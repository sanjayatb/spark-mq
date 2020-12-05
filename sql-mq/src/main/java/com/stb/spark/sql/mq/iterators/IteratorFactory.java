package com.stb.spark.sql.mq.iterators;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import static com.stb.spark.sql.mq.Contants.*;

public class IteratorFactory {

    public static final Map<String, Function<IteratorData,? extends Iterator>> ITERATOR_MAP;

    private static final Function<IteratorData,Iterator<byte[]>> createTextIterator = LineIterator::new;
    private static final Function<IteratorData,Iterator<byte[]>> createJsonIterator = JsonIterator::new;
    private static final Function<IteratorData,Iterator<byte[]>> createXmlIterator = XmlIterator::new;

    static {
        ITERATOR_MAP = new HashMap<>();
        ITERATOR_MAP.put(FIXED_WIDTH_FORMAT,createTextIterator);
        ITERATOR_MAP.put(JSON_FORMAT,createJsonIterator);
        ITERATOR_MAP.put(XML_FORMAT,createXmlIterator);
    }

    private IteratorFactory(){}

    public static Iterator<byte[]> create(String sourceFormat, InputStream inputStream,
                                          Map<String, String> options, String filePath){

        IteratorData iteratorData = new IteratorData(inputStream,options,filePath);

        return ITERATOR_MAP.get(sourceFormat).apply(iteratorData);
    }

}
