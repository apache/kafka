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

import static org.apache.kafka.clients.consumer.internals.PartitionAssignorAdapter.getAssignorInstances;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class PartitionAssignorAdapterTest {

    private List<String> classNames;
    private List<Object> classTypes;

    private Class<StickyAssignor> assignorClass = StickyAssignor.class;
    private Class<PartitionAssignorAdapter> adapterClass = PartitionAssignorAdapter.class;

    @Test
    public void shouldInstantiateNewAssignors() {
        classNames = Arrays.asList(StickyAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classNames, Collections.emptyMap());
        assertTrue(assignorClass.isInstance(assignors.get(0)));
    }

    @Test
    public void shouldAdaptOldAssignors() {
        classNames = Arrays.asList(OldPartitionAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classNames, Collections.emptyMap());
        assertTrue(adapterClass.isInstance(assignors.get(0)));
    }

    @Test
    public void shouldThrowKafkaExceptionOnNonAssignor() {
        classNames = Arrays.asList(NotAnAssignor.class.getName());
        assertThrows(KafkaException.class, () -> getAssignorInstances(classNames, Collections.emptyMap()));
    }

    @Test
    public void shouldThrowKafkaExceptionOnAssignorNotFound() {
        classNames = Arrays.asList("Non-existent assignor");
        assertThrows(KafkaException.class, () -> getAssignorInstances(classNames, Collections.emptyMap()));
    }

    @Test
    public void shouldInstantiateFromListOfOldAndNewClassTypes() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        classTypes = Arrays.asList(StickyAssignor.class, OldPartitionAssignor.class);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
            props, new StringDeserializer(), new StringDeserializer());

        consumer.close();
    }

    @Test
    public void shouldThrowKafkaExceptionOnListWithNonAssignorClassType() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        classTypes = Arrays.asList(StickyAssignor.class, OldPartitionAssignor.class, NotAnAssignor.class);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        assertThrows(KafkaException.class, () -> new KafkaConsumer<>(
            props, new StringDeserializer(), new StringDeserializer()));
    }

    @SuppressWarnings("deprecation")
    public static class OldPartitionAssignor implements PartitionAssignor {

        @Override
        public Subscription subscription(Set<String> topics) {
            return new Subscription(new ArrayList<>(topics), null);
        }

        @Override
        public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
            return new HashMap<>();
        }

        @Override
        public void onAssignment(Assignment assignment) {
        }

        @Override
        public String name() {
            return "old-assignor";
        }
    }

    public static class NotAnAssignor {

        public NotAnAssignor() {
            throw new IllegalStateException("Should not have been instantiated!");
        }
    }

}
