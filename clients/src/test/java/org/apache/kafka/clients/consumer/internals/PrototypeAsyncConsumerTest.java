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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Properties;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrototypeAsyncConsumerTest {
    private final Properties properties = new Properties();

    @BeforeEach
    public void setup() {
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(CLIENT_ID_CONFIG, "test-client");
    }
    @Test
    public void testSubscription() {
        SubscriptionState subscriptionState = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        PrototypeAsyncConsumer consumer =
                Mockito.mock(PrototypeAsyncConsumer.class);
        subscriptionState.subscribe(singleton("t1"), new NoOpConsumerRebalanceListener());
        Set<String> subscriptionRes = subscriptionState.subscription();
        Mockito.doCallRealMethod()
                .when(consumer)
                .subscription();
        Mockito.doReturn(subscriptionRes).when(consumer).subscription();
        assertEquals(1, consumer.subscription().size());
    }
}
