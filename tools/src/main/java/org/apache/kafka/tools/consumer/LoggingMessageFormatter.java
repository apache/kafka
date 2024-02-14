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
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class LoggingMessageFormatter implements MessageFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingMessageFormatter.class);
    private final DefaultMessageFormatter defaultWriter = new DefaultMessageFormatter();

    @Override
    public void configure(Map<String, ?> configs) {
        defaultWriter.configure(configs);
    }

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        defaultWriter.writeTo(consumerRecord, output);
        String timestamp = consumerRecord.timestampType() != TimestampType.NO_TIMESTAMP_TYPE
                ? consumerRecord.timestampType() + ":" + consumerRecord.timestamp() + ", "
                : "";
        String key = "key:" + (consumerRecord.key() == null ? "null " : new String(consumerRecord.key(), StandardCharsets.UTF_8) + ", ");
        String value = "value:" + (consumerRecord.value() == null ? "null" : new String(consumerRecord.value(), StandardCharsets.UTF_8));
        LOG.info(timestamp + key + value);
    }
}
