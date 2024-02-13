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
package org.apache.kafka.tools.consumer.group;

import kafka.admin.ConsumerGroupCommand;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SaslClientsWithInvalidCredentialsTest extends kafka.api.AbstractSaslClientsWithInvalidCredentialsTest {
    @Test
    public void testConsumerGroupServiceWithAuthenticationFailure() {
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
        try (Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic()));

            verifyAuthenticationException(() -> {
                consumerGroupService.listGroups();
                return null;
            });
        } finally {
            consumerGroupService.close();
        }
    }

    @Test
    public void testConsumerGroupServiceWithAuthenticationSuccess() {
        createClientCredential();
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
        try (Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic()));

            verifyWithRetry(() -> {
                consumer.poll(Duration.ofMillis(1000));
                return null;
            });
            assertEquals(1, consumerGroupService.listConsumerGroups().size());
        } finally {
            consumerGroupService.close();
        }
    }

    private ConsumerGroupCommand.ConsumerGroupService prepareConsumerGroupService() {
        Properties properties = new Properties();
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", kafkaClientSaslMechanism());

        File propsFile = TestUtils.tempPropertiesFile(properties);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()),
            "--describe",
            "--group", "test.group",
            "--command-config", propsFile.getAbsolutePath()};
        ConsumerGroupCommand.ConsumerGroupCommandOptions opts = new ConsumerGroupCommand.ConsumerGroupCommandOptions(cgcArgs);
        return new ConsumerGroupCommand.ConsumerGroupService(opts, Map$.MODULE$.empty());
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
