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
package org.apache.kafka.clients.consumer;


import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.getAssignorInstances;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerPartitionAssignorTest {

    private List<String> classNames;
    private List<Object> classTypes;

    @Test
    public void shouldInstantiateAssignor() {
        classNames = Collections.singletonList(StickyAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classNames, Collections.emptyMap());
        assertTrue(assignors.get(0) instanceof StickyAssignor);
    }

    @Test
    public void shouldInstantiateListOfAssignors() {
        classNames = Arrays.asList(StickyAssignor.class.getName(), CooperativeStickyAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classNames, Collections.emptyMap());
        assertTrue(assignors.get(0) instanceof StickyAssignor);
    }

    @Test
    public void shouldThrowKafkaExceptionOnNonAssignor() {
        classNames = Collections.singletonList(String.class.getName());
        assertThrows(KafkaException.class, () -> getAssignorInstances(classNames, Collections.emptyMap()));
    }

    @Test
    public void shouldThrowKafkaExceptionOnAssignorNotFound() {
        classNames = Collections.singletonList("Non-existent assignor");
        assertThrows(KafkaException.class, () -> getAssignorInstances(classNames, Collections.emptyMap()));
    }

    @Test
    public void shouldInstantiateFromClassType() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        classTypes = Collections.singletonList(StickyAssignor.class);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer());

        consumer.close();
    }

    @Test
    public void shouldInstantiateFromListOfClassTypes() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        classTypes = Arrays.asList(StickyAssignor.class, CooperativeStickyAssignor.class);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer());

        consumer.close();
    }

    @Test
    public void shouldThrowKafkaExceptionOnListWithNonAssignorClassType() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        classTypes = Arrays.asList(StickyAssignor.class, String.class);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        assertThrows(KafkaException.class, () -> new KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer()));
    }

}
