/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.Arrays.stream;

/**
 * The default implementation of {@link MessageReader} for the {@link ConsoleProducer}. This reader comes with
 * the ability to parse a record's headers, key and value based on configurable separators. The reader configuration
 * is defined as follows:
 * <p></p>
 * <pre>
 *    parse.key             : indicates if a record's key is included in a line input and needs to be parsed. (default: false).
 *    key.separator         : the string separating a record's key from its value. (default: \t).
 *    parse.headers         : indicates if record headers are included in a line input and need to be parsed. (default: false).
 *    headers.delimiter     : the string separating the list of headers from the record key. (default: \t).
 *    headers.key.separator : the string separating the key and value within a header. (default: :).
 *    ignore.error          : whether best attempts should be made to ignore parsing errors. (default: false).
 *    null.marker           : record key, record value, header key, header value which match this marker are replaced by null. (default: null).
 * </pre>
 */
public final class LineMessageReader implements MessageReader {
    private String topic;
    private BufferedReader reader;
    private boolean parseKey;
    private String keySeparator = "\t";
    private boolean parseHeaders;
    private String headersDelimiter = "\t";
    private String headersSeparator = ",";
    private String headersKeySeparator = ":";
    private boolean ignoreError;
    private int lineNumber;
    private boolean printPrompt = System.console() != null;
    private Pattern headersSeparatorPattern;
    private String nullMarker;

    @Override
    public void init(InputStream inputStream, Properties props) {
        topic = props.getProperty("topic");
        if (props.containsKey("parse.key"))
            parseKey = props.getProperty("parse.key").trim().equalsIgnoreCase("true");
        if (props.containsKey("key.separator"))
            keySeparator = props.getProperty("key.separator");
        if (props.containsKey("parse.headers"))
            parseHeaders = props.getProperty("parse.headers").trim().equalsIgnoreCase("true");
        if (props.containsKey("headers.delimiter"))
            headersDelimiter = props.getProperty("headers.delimiter");
        if (props.containsKey("headers.separator"))
            headersSeparator = props.getProperty("headers.separator");
        headersSeparatorPattern = Pattern.compile(headersSeparator);
        if (props.containsKey("headers.key.separator"))
            headersKeySeparator = props.getProperty("headers.key.separator");
        if (props.containsKey("ignore.error"))
            ignoreError = props.getProperty("ignore.error").trim().equalsIgnoreCase("true");
        if (headersDelimiter.equals(headersSeparator))
            throw new KafkaException("headers.delimiter and headers.separator may not be equal");
        if (headersDelimiter.equals(headersKeySeparator))
            throw new KafkaException("headers.delimiter and headers.key.separator may not be equal");
        if (headersSeparator.equals(headersKeySeparator))
            throw new KafkaException("headers.separator and headers.key.separator may not be equal");
        if (props.containsKey("null.marker"))
            nullMarker = props.getProperty("null.marker");
        if (keySeparator.equals(nullMarker))
            throw new KafkaException("null.marker and key.separator may not be equal");
        if (headersSeparator.equals(nullMarker))
            throw new KafkaException("null.marker and headers.separator may not be equal");
        if (headersDelimiter.equals(nullMarker))
            throw new KafkaException("null.marker and headers.delimiter may not be equal");
        if (headersKeySeparator.equals(nullMarker))
            throw new KafkaException("null.marker and headers.key.separator may not be equal");

        reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    @Override
    public ProducerRecord<byte[], byte[]> readMessage() {
        ++lineNumber;
        if (printPrompt) {
            System.out.print(">");
        }

        String line;
        try {
            line = reader.readLine();

        } catch (IOException e) {
            throw new KafkaException(e);
        }

        if (line == null) {
            return null;
        }

        String headers = parse(parseHeaders, line, 0, headersDelimiter, "headers delimiter");
        int headerOffset = headers == null ? 0 : headers.length() + headersDelimiter.length();

        String key = parse(parseKey, line, headerOffset, keySeparator, "key separator");
        int keyOffset = key == null ? 0 : key.length() + keySeparator.length();

        String value = line.substring(headerOffset + keyOffset);

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topic,
                key != null && !key.equals(nullMarker) ? key.getBytes(StandardCharsets.UTF_8) : null,
                value != null && !value.equals(nullMarker) ? value.getBytes(StandardCharsets.UTF_8) : null
        );

        if (headers != null && !headers.equals(nullMarker)) {
            stream(splitHeaders(headers)).forEach(header -> record.headers().add(header.key(), header.value()));
        }

        return record;
    }

    private String parse(boolean enabled, String line, int startIndex, String demarcation, String demarcationName) {
        if (!enabled) {
            return null;
        }
        int index = line.indexOf(demarcation, startIndex);
        if (index == -1) {
            if (ignoreError) {
                return null;
            }
            throw new KafkaException("No " + demarcationName + " found on line number " + lineNumber + ": '" + line + "'");
        }
        return line.substring(startIndex, index);
    }

    private Header[] splitHeaders(String headers) {
        return stream(headersSeparatorPattern.split(headers))
                .map(pair -> {
                    int i = pair.indexOf(headersKeySeparator);
                    if (i == -1) {
                        if (ignoreError) {
                            return new RecordHeader(pair, null);
                        }
                        throw new KafkaException("No header key separator found in pair '" + pair + "' on line number " + lineNumber);
                    }

                    String headerKey = pair.substring(0, i);
                    if (headerKey.equals(nullMarker)) {
                        throw new KafkaException("Header keys should not be equal to the null marker '" + nullMarker + "' as they can't be null");
                    }

                    String value = pair.substring(i + headersKeySeparator.length());
                    byte[] headerValue = value.equals(nullMarker) ? null : value.getBytes(StandardCharsets.UTF_8);
                    return new RecordHeader(headerKey, headerValue);

                }).toArray(Header[]::new);
    }

    // VisibleForTesting
    String keySeparator() {
        return keySeparator;
    }

    // VisibleForTesting
    boolean parseKey() {
        return parseKey;
    }

    // VisibleForTesting
    boolean parseHeaders() {
        return parseHeaders;
    }
}
