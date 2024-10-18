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
package org.apache.kafka.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Configurable;

import java.util.HashMap;
import java.util.Map;

public class KafkaClientInterceptor implements Configurable {
    protected Map<String, Object> config;

    @Override
    public void configure(final Map<String, ?> config) {
        this.config = new HashMap<>(config);
    }

    public Admin wrapAdminClient(final KafkaAdminClient adminClient) {
        return adminClient;
    }

    public Consumer<byte[], byte[]> wrapMainConsumer(final KafkaConsumer<byte[], byte[]> mainConsumer) {
        return mainConsumer;
    }

    public Consumer<byte[], byte[]> wrapRestoreConsumer(final KafkaConsumer<byte[], byte[]> restoreConsumer) {
        return restoreConsumer;
    }

    public Consumer<byte[], byte[]> wrapGlobalConsumer(final KafkaConsumer<byte[], byte[]> globalConsumer) {
        return globalConsumer;
    }

    public Producer<byte[], byte[]> wrapProducer(final KafkaProducer<byte[], byte[]> producer) {
        return producer;
    }
}
