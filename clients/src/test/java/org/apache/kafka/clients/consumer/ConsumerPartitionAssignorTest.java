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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.getAssignorInstances;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerPartitionAssignorTest {

    @Test
    public void shouldInstantiateAssignor() {
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(
                Collections.singletonList(StickyAssignor.class.getName()),
                Collections.emptyMap()
        );
        assertTrue(assignors.get(0) instanceof StickyAssignor);
    }

    @Test
    public void shouldInstantiateListOfAssignors() {
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(
                Arrays.asList(StickyAssignor.class.getName(), CooperativeStickyAssignor.class.getName()),
                Collections.emptyMap()
        );
        assertTrue(assignors.get(0) instanceof StickyAssignor);
        assertTrue(assignors.get(1) instanceof CooperativeStickyAssignor);
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
        assertTrue(assignors.get(0) instanceof StickyAssignor);
    }

    @Test
    public void shouldInstantiateFromListOfClassTypes() {
        List<String> classTypes = initConsumerConfigWithClassTypes(
                Arrays.asList(StickyAssignor.class, CooperativeStickyAssignor.class)
        ).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);

        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classTypes, Collections.emptyMap());

        assertTrue(assignors.get(0) instanceof StickyAssignor);
        assertTrue(assignors.get(1) instanceof CooperativeStickyAssignor);
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

    public static class TestConsumerPartitionAssignor implements ConsumerPartitionAssignor {

        @Override
        public ByteBuffer subscriptionUserData(Set<String> topics) {
            return ConsumerPartitionAssignor.super.subscriptionUserData(topics);
        }

        @Override
        public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
            return null;
        }

        @Override
        public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
            ConsumerPartitionAssignor.super.onAssignment(assignment, metadata);
        }

        @Override
        public List<RebalanceProtocol> supportedProtocols() {
            return ConsumerPartitionAssignor.super.supportedProtocols();
        }

        @Override
        public short version() {
            return ConsumerPartitionAssignor.super.version();
        }

        @Override
        public String name() {
            // use the RangeAssignor's name to cause naming conflict
            return new RangeAssignor().name();
        }
    }

    private ConsumerConfig initConsumerConfigWithClassTypes(List<Object> classTypes) {
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        return new ConsumerConfig(props);
    }
}
