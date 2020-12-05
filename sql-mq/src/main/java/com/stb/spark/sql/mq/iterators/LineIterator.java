package com.stb.spark.sql.mq.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class LineIterator extends CloseableIterator<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LineIterator.class);

    private Scanner scanner;
    private static final String DEFAULT_ENCODING = "UTF8";
    private String encoding;

    public LineIterator(IteratorData iteratorData) {
        encoding = iteratorData.getMapOptions().getOrDefault("encoding",DEFAULT_ENCODING);
        scanner = new Scanner(iteratorData.getInputStream(),encoding);
    }

    @Override
    protected Closeable getCloseableResource() {
        return scanner;
    }

    @Override
    public boolean hasNext() {
        return scanner.hasNextLine();
    }

    @Override
    public byte[] next() {
        byte[] output = null;
        if(hasNext()){
            try {
                output = scanner.nextLine().getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Not able to parse input data for encoding {}",encoding);
            }
        }else {
            throw new NoSuchElementException();
        }
        return output;
    }
}
