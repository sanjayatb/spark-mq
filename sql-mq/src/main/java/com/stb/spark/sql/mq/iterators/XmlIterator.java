package com.stb.spark.sql.mq.iterators;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

/***
 * Iterate xml files
 */
public class XmlIterator extends CloseableIterator<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XmlIterator.class);

    private Optional<Map<String, String>> headers = Optional.empty();
    private String split;
    private BufferedReader reader;
    private byte[] nextItem;

    public XmlIterator(IteratorData iteratorData) {
        split = iteratorData.getMapOptions().get("split");
        reader = new BufferedReader(new InputStreamReader(iteratorData.getInputStream()));
        parseHeaders(iteratorData.getMapOptions().get("headers"));
    }

    private void parseHeaders(String staticBlockNames) {
        if (StringUtils.isNotEmpty(staticBlockNames)) {
            headers = Optional.of(Arrays.stream(staticBlockNames.split(","))
                    .collect(Collectors.toMap(key -> key, value -> {
                        try {
                            return new String(extractChunk(value));
                        } catch (IOException e) {
                            LOGGER.error("Fail to extract data", e);
                        }
                        return null;
                    })));
        }
    }

    private byte[] extractChunk(String nodeName) throws IOException {
        String line = null;
        boolean firstLine = true;
        StringBuilder xmlChunk = new StringBuilder();
        boolean appenderLines = StringUtils.isEmpty(nodeName);
        while (reader != null && (line = reader.readLine()) != null) {
            if (line.contains("</" + nodeName + ">") && firstLine) {
                firstLine = false;
                line = line.replace("><", ">\n<");
                Reader inputString = new StringReader(line);
                reader = new BufferedReader(inputString);
                continue;
            }
            firstLine = false;
            if (line.contains("<" + nodeName + ">") || line.contains("<" + nodeName + " ")) {
                appenderLines = true;
            } else if (line.contains("</" + nodeName + ">")) {
                xmlChunk.append(line + "\n");
                return xmlChunk.toString().getBytes();
            }
            if (appenderLines) {
                xmlChunk.append(line + "\n");
            }
        }
        close();
        return xmlChunk.length() > 0 ? xmlChunk.toString().getBytes() : null;
    }

    @Override
    protected Closeable getCloseableResource() {
        return reader;
    }

    @Override
    public boolean hasNext() {
        if (nextItem == null) {
            try {
                nextItem = moveToNode();
            } catch (IOException e) {
                LOGGER.error("Node not found", e);
            }
        }
        return nextItem != null;
    }

    private byte[] moveToNode() throws IOException {
        byte[] splitVal = extractChunk(split);
        if (splitVal == null) {
            return null;
        }
        StringBuilder xmlChunk = new StringBuilder();
        headers.ifPresent(map -> {
                    xmlChunk.append("<root>");
                    map.entrySet().forEach(
                            entry -> xmlChunk.append(entry.getValue() + "\n"));
                }
        );
        xmlChunk.append(new String(splitVal));
        headers.ifPresent(map -> xmlChunk.append("</root>"));
        return xmlChunk.length() > 0 ? xmlChunk.toString().getBytes() : null;
    }

    @Override
    public byte[] next() {
        byte[] currentItem;
        if(hasNext()){
         currentItem = nextItem;
         nextItem = null;
        }else {
            throw new NoSuchElementException();
        }
        return currentItem;
    }

    @Override
    public void close() {
        if (reader != null) {
            super.close();
            reader = null;
        }
    }

}
