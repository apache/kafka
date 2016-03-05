/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.test;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class MockProducerInterceptor implements ProducerInterceptor<String, String> {
    public static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    public static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);
    public static final AtomicInteger ONSEND_COUNT = new AtomicInteger(0);
    public static final AtomicInteger ON_SUCCESS_COUNT = new AtomicInteger(0);
    public static final AtomicInteger ON_ERROR_COUNT = new AtomicInteger(0);
    public static final AtomicInteger ON_ERROR_WITH_METADATA_COUNT = new AtomicInteger(0);
    public static final String APPEND_STRING_PROP = "mock.interceptor.append";
    private String appendStr;

    public MockProducerInterceptor() {
        INIT_COUNT.incrementAndGet();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // ensure this method is called and expected configs are passed in
        Object o = configs.get(APPEND_STRING_PROP);
        if (o == null)
            throw new ConfigException("Mock producer interceptor expects configuration " + APPEND_STRING_PROP);
        if (o != null && o instanceof String)
            appendStr = (String) o;

        // clientId also must be in configs
        Object clientIdValue = configs.get(ProducerConfig.CLIENT_ID_CONFIG);
        if (clientIdValue == null)
            throw new ConfigException("Mock producer interceptor expects configuration " + ProducerConfig.CLIENT_ID_CONFIG);
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        ONSEND_COUNT.incrementAndGet();
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(
                record.topic(), record.partition(), record.key(), record.value().concat(appendStr));
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            ON_ERROR_COUNT.incrementAndGet();
            if (metadata != null) {
                ON_ERROR_WITH_METADATA_COUNT.incrementAndGet();
            }
        } else if (metadata != null)
            ON_SUCCESS_COUNT.incrementAndGet();
    }

    @Override
    public void close() {
        CLOSE_COUNT.incrementAndGet();
    }

    public static void resetCounters() {
        INIT_COUNT.set(0);
        CLOSE_COUNT.set(0);
        ONSEND_COUNT.set(0);
        ON_SUCCESS_COUNT.set(0);
        ON_ERROR_COUNT.set(0);
        ON_ERROR_WITH_METADATA_COUNT.set(0);
    }
}
