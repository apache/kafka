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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ErrorLoggingCallback implements Callback {
    private static final Logger log = LoggerFactory.getLogger(ErrorLoggingCallback.class);
    private final String topic;
    private final byte[] key;
    private final int valueLength;
    private final boolean logAsString;
    private byte[] value;
    public ErrorLoggingCallback(String topic, byte[] key, byte[] value, boolean logAsString) {
        this.topic = topic;
        this.key = key;

        if (logAsString) {
            this.value = value;
        }

        this.valueLength = value == null ? -1 : value.length;
        this.logAsString = logAsString;
    }

    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            String keyString = (key == null) ? "null" :
                    logAsString ? new String(key, StandardCharsets.UTF_8) : key.length + " bytes";
            String valueString = (valueLength == -1) ? "null" :
                    logAsString ? new String(value, StandardCharsets.UTF_8) : valueLength + " bytes";
            log.error("Error when sending message to topic {} with key: {}, value: {} with error:",
                    topic, keyString, valueString, e);
        }
    }
}
