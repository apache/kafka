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
package org.apache.kafka.tools.consumergroup;

import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.tools.consumergroup.ConsumerGroupCommand.ConsumerGroupService;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SaslClientsWithInvalidCredentialsTest extends kafka.api.SaslClientsWithInvalidCredentialsTest {
    @Test
    public void testConsumerGroupServiceWithAuthenticationFailure() {
        try (ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
             Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic()));

            verifyAuthenticationException(() -> {
                try {
                    consumerGroupService.listGroups();
                    return null;
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Test
    public void testConsumerGroupServiceWithAuthenticationSuccess() {
        createClientCredential();

        try (ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
             Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic()));

            verifyWithRetry(() -> {
                consumer.poll(Duration.ofMillis(1000));
                return null;
            });
            assertEquals(1, consumerGroupService.listConsumerGroups().size());
        }
    }

    private ConsumerGroupService prepareConsumerGroupService() {
        Properties properties = new Properties();
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", kafkaClientSaslMechanism());

        File propsFile = TestUtils.tempPropertiesFile(properties);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()),
            "--describe",
            "--group", "test.group",
            "--command-config", propsFile.getAbsolutePath()};
        ConsumerGroupCommandOptions opts = new ConsumerGroupCommandOptions(cgcArgs);
        return new ConsumerGroupService(opts, Collections.emptyMap());
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    private Consumer<byte[], byte[]> createConsumer() {
        return createConsumer(
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer(),
            new Properties(),
            JavaConverters.asScalaSet(Collections.<String>emptySet()).toList()
        );
    }
}
