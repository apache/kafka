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
import static org.junit.Assert.assertEquals;
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
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class PartitionAssignorAdapterTest {

    private List<String> classNames;
    private List<Object> classTypes;

    @Test
    public void shouldInstantiateNewAssignors() {
        classNames = Arrays.asList(StickyAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classNames, Collections.emptyMap());
        assertTrue(StickyAssignor.class.isInstance(assignors.get(0)));
    }

    @Test
    public void shouldAdaptOldAssignors() {
        classNames = Arrays.asList(OldPartitionAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = getAssignorInstances(classNames, Collections.emptyMap());
        assertTrue(PartitionAssignorAdapter.class.isInstance(assignors.get(0)));
    }

    @Test
    public void shouldThrowKafkaExceptionOnNonAssignor() {
        classNames = Arrays.asList(String.class.getName());
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

        classTypes = Arrays.asList(StickyAssignor.class, OldPartitionAssignor.class, String.class);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classTypes);
        assertThrows(KafkaException.class, () -> new KafkaConsumer<>(
            props, new StringDeserializer(), new StringDeserializer()));
    }

    @Test
    public void testOnAssignment() {
        OldPartitionAssignor oldAssignor = new OldPartitionAssignor();
        ConsumerPartitionAssignor adaptedAssignor = new PartitionAssignorAdapter(oldAssignor);

        TopicPartition tp1 = new TopicPartition("tp1", 1);
        TopicPartition tp2 = new TopicPartition("tp2", 2);
        List<TopicPartition> partitions = Arrays.asList(tp1, tp2);

        adaptedAssignor.onAssignment(new Assignment(partitions), new ConsumerGroupMetadata(""));

        assertEquals(oldAssignor.partitions, partitions);
    }

    @Test
    public void testAssign() {
        ConsumerPartitionAssignor adaptedAssignor = new PartitionAssignorAdapter(new OldPartitionAssignor());

        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("C1", new Subscription(Arrays.asList("topic1")));
        subscriptions.put("C2", new Subscription(Arrays.asList("topic1", "topic2")));
        subscriptions.put("C3", new Subscription(Arrays.asList("topic2", "topic3")));
        GroupSubscription groupSubscription = new GroupSubscription(subscriptions);

        Map<String, Assignment> assignments = adaptedAssignor.assign(null, groupSubscription).groupAssignment();

        assertEquals(assignments.get("C1").partitions(), Arrays.asList(new TopicPartition("topic1", 1)));
        assertEquals(assignments.get("C2").partitions(), Arrays.asList(new TopicPartition("topic1", 1), new TopicPartition("topic2", 1)));
        assertEquals(assignments.get("C3").partitions(), Arrays.asList(new TopicPartition("topic2", 1), new TopicPartition("topic3", 1)));
    }

    /*
     * Dummy assignor just gives each consumer partition 1 of each topic it's subscribed to
     */
    @SuppressWarnings("deprecation")
    public static class OldPartitionAssignor implements PartitionAssignor {

        List<TopicPartition> partitions = null;

        @Override
        public Subscription subscription(Set<String> topics) {
            return new Subscription(new ArrayList<>(topics), null);
        }

        @Override
        public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
            Map<String, Assignment> assignments = new HashMap<>();
            for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
                List<TopicPartition> partitions = new ArrayList<>();
                for (String topic : entry.getValue().topics()) {
                    partitions.add(new TopicPartition(topic, 1));
                }
                assignments.put(entry.getKey(), new Assignment(partitions, null));
            }
            return assignments;
        }

        @Override
        public void onAssignment(Assignment assignment) {
            partitions = assignment.partitions();
        }

        @Override
        public String name() {
            return "old-assignor";
        }
    }

}
