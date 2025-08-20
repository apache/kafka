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


import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.getAssignorInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConsumerPartitionAssignorTest {

    @Test
    public void shouldInstantiateAssignor() {
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(
                Collections.singletonList(StickyAssignor.class.getName()),
                Collections.emptyMap()
        );
        assertInstanceOf(StickyAssignor.class, assignors.get(0));
    }

    @Test
    public void shouldInstantiateListOfAssignors() {
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(
                Arrays.asList(StickyAssignor.class.getName(), CooperativeStickyAssignor.class.getName()),
                Collections.emptyMap()
        );
        assertInstanceOf(StickyAssignor.class, assignors.get(0));
        assertInstanceOf(CooperativeStickyAssignor.class, assignors.get(1));
    }

    @Test
    public void shouldThrowKafkaExceptionOnNonAssignor() {
        assertThrows(KafkaException.class, () -> getAssignorInstances(
                Collections.singletonList(String.class.getName()),
                Collections.emptyMap())
        );
    }

    @Test
    public void shouldThrowKafkaExceptionOnAssignorNotFound() {
        assertThrows(KafkaException.class, () -> getAssignorInstances(
                Collections.singletonList("Non-existent assignor"),
                Collections.emptyMap())
        );
    }

    @Test
    public void shouldInstantiateFromClassType() {
        List<String> classTypes =
                initConsumerConfigWithClassTypes(Collections.singletonList(StickyAssignor.class))
                .getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classTypes, Collections.emptyMap());
        assertInstanceOf(StickyAssignor.class, assignors.get(0));
    }

    @Test
    public void shouldInstantiateFromListOfClassTypes() {
        List<String> classTypes = initConsumerConfigWithClassTypes(
                Arrays.asList(StickyAssignor.class, CooperativeStickyAssignor.class)
        ).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);

        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classTypes, Collections.emptyMap());

        assertInstanceOf(StickyAssignor.class, assignors.get(0));
        assertInstanceOf(CooperativeStickyAssignor.class, assignors.get(1));
    }

    @Test
    public void shouldThrowKafkaExceptionOnListWithNonAssignorClassType() {
        List<String> classTypes =
                initConsumerConfigWithClassTypes(Arrays.asList(StickyAssignor.class, String.class))
                .getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);

        assertThrows(KafkaException.class, () -> getAssignorInstances(classTypes, Collections.emptyMap()));
    }

    @Test
    public void shouldThrowKafkaExceptionOnAssignorsWithSameName() {
        assertThrows(KafkaException.class, () -> getAssignorInstances(
            Arrays.asList(RangeAssignor.class.getName(), TestConsumerPartitionAssignor.class.getName()),
            Collections.emptyMap()
        ));
    }

    @Test
    public void shouldBeConfigurable() {
        Map<String, Object> configs = Collections.singletonMap("key", "value");
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(
            Collections.singletonList(TestConsumerPartitionAssignor.class.getName()),
            configs
        );
        assertEquals(1, assignors.size());
        assertInstanceOf(TestConsumerPartitionAssignor.class, assignors.get(0));
        assertEquals(configs, ((TestConsumerPartitionAssignor) assignors.get(0)).configs);
    }


    public static class TestConsumerPartitionAssignor implements ConsumerPartitionAssignor, Configurable {
        private Map<String, ?> configs = null;

        @Override
        public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
            return null;
        }

        @Override
        public String name() {
            // use the RangeAssignor's name to cause naming conflict
            return new RangeAssignor().name();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
        }
    }

    private ConsumerConfig initConsumerConfigWithClassTypes(List<Object> classTypes) {
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        return new ConsumerConfig(props);
    }
}
