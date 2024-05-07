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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

class DefaultMessageFormatter implements MessageFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMessageFormatter.class);

    private boolean printTimestamp = false;
    private boolean printKey = false;
    private boolean printValue = true;
    private boolean printPartition = false;
    private boolean printOffset = false;
    private boolean printHeaders = false;
    private byte[] keySeparator = utfBytes("\t");
    private byte[] lineSeparator = utfBytes("\n");
    private byte[] headersSeparator = utfBytes(",");
    private byte[] nullLiteral = utfBytes("null");

    private Optional<Deserializer<?>> keyDeserializer = Optional.empty();
    private Optional<Deserializer<?>> valueDeserializer = Optional.empty();
    private Optional<Deserializer<?>> headersDeserializer = Optional.empty();

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey("print.timestamp")) {
            printTimestamp = getBoolProperty(configs, "print.timestamp");
        }
        if (configs.containsKey("print.key")) {
            printKey = getBoolProperty(configs, "print.key");
        }
        if (configs.containsKey("print.offset")) {
            printOffset = getBoolProperty(configs, "print.offset");
        }
        if (configs.containsKey("print.partition")) {
            printPartition = getBoolProperty(configs, "print.partition");
        }
        if (configs.containsKey("print.headers")) {
            printHeaders = getBoolProperty(configs, "print.headers");
        }
        if (configs.containsKey("print.value")) {
            printValue = getBoolProperty(configs, "print.value");
        }
        if (configs.containsKey("key.separator")) {
            keySeparator = getByteProperty(configs, "key.separator");
        }
        if (configs.containsKey("line.separator")) {
            lineSeparator = getByteProperty(configs, "line.separator");
        }
        if (configs.containsKey("headers.separator")) {
            headersSeparator = getByteProperty(configs, "headers.separator");
        }
        if (configs.containsKey("null.literal")) {
            nullLiteral = getByteProperty(configs, "null.literal");
        }

        keyDeserializer = getDeserializerProperty(configs, true, "key.deserializer");
        valueDeserializer = getDeserializerProperty(configs, false, "value.deserializer");
        headersDeserializer = getDeserializerProperty(configs, false, "headers.deserializer");
    }

    // for testing
    public boolean printValue() {
        return printValue;
    }

    // for testing
    public Optional<Deserializer<?>> keyDeserializer() {
        return keyDeserializer;
    }

    private void writeSeparator(PrintStream output, boolean columnSeparator) {
        try {
            if (columnSeparator) {
                output.write(keySeparator);
            } else {
                output.write(lineSeparator);
            }
        } catch (IOException ioe) {
            LOG.error("Unable to write the separator to the output", ioe);
        }
    }

    private byte[] deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Optional<Deserializer<?>> deserializer, byte[] sourceBytes, String topic) {
        byte[] nonNullBytes = sourceBytes != null ? sourceBytes : nullLiteral;
        return deserializer.map(value -> utfBytes(value.deserialize(topic, consumerRecord.headers(), nonNullBytes).toString())).orElse(nonNullBytes);
    }

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        try {
            if (printTimestamp) {
                if (consumerRecord.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
                    output.print(consumerRecord.timestampType() + ":" + consumerRecord.timestamp());
                } else {
                    output.print("NO_TIMESTAMP");
                }
                writeSeparator(output, printOffset || printPartition || printHeaders || printKey || printValue);
            }

            if (printPartition) {
                output.print("Partition:");
                output.print(consumerRecord.partition());
                writeSeparator(output, printOffset || printHeaders || printKey || printValue);
            }

            if (printOffset) {
                output.print("Offset:");
                output.print(consumerRecord.offset());
                writeSeparator(output, printHeaders || printKey || printValue);
            }

            if (printHeaders) {
                Iterator<Header> headersIt = consumerRecord.headers().iterator();
                if (!headersIt.hasNext()) {
                    output.print("NO_HEADERS");
                } else {
                    while (headersIt.hasNext()) {
                        Header header = headersIt.next();
                        output.print(header.key() + ":");
                        output.write(deserialize(consumerRecord, headersDeserializer, header.value(), consumerRecord.topic()));
                        if (headersIt.hasNext()) {
                            output.write(headersSeparator);
                        }
                    }
                }
                writeSeparator(output, printKey || printValue);
            }

            if (printKey) {
                output.write(deserialize(consumerRecord, keyDeserializer, consumerRecord.key(), consumerRecord.topic()));
                writeSeparator(output, printValue);
            }

            if (printValue) {
                output.write(deserialize(consumerRecord, valueDeserializer, consumerRecord.value(), consumerRecord.topic()));
                output.write(lineSeparator);
            }
        } catch (IOException ioe) {
            LOG.error("Unable to write the consumer record to the output", ioe);
        }
    }

    private Map<String, ?> propertiesWithKeyPrefixStripped(String prefix, Map<String, ?> configs) {
        final Map<String, Object> newConfigs = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                newConfigs.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        return newConfigs;
    }

    private byte[] utfBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] getByteProperty(Map<String, ?> configs, String key) {
        return utfBytes((String) configs.get(key));
    }

    private boolean getBoolProperty(Map<String, ?> configs, String key) {
        return ((String) configs.get(key)).trim().equalsIgnoreCase("true");
    }

    private Optional<Deserializer<?>> getDeserializerProperty(Map<String, ?> configs, boolean isKey, String key) {
        if (configs.containsKey(key)) {
            String name = (String) configs.get(key);
            try {
                Deserializer<?> deserializer = (Deserializer<?>) Class.forName(name).getDeclaredConstructor().newInstance();
                Map<String, ?> deserializerConfig = propertiesWithKeyPrefixStripped(key + ".", configs);
                deserializer.configure(deserializerConfig, isKey);
                return Optional.of(deserializer);
            } catch (Exception e) {
                LOG.error("Unable to instantiate a deserializer from " + name, e);
            }
        }
        return Optional.empty();
    }
}
