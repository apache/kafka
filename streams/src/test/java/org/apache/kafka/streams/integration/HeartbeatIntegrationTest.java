package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerHeartbeatDataCallbacks;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

@Category({IntegrationTest.class})
public class HeartbeatIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(1);

    public static class MyDynamicAssignmentLogic {

        private static class MemberState {

            private final String memberId;

            private final PartitionAssignor.Subscription subscription;
            private String data;
            public MemberState(String memberId, PartitionAssignor.Subscription subscription, String data) {
                this.memberId = memberId;
                this.subscription = subscription;
                this.data = data;
            }

        }

        private final Map<String, MemberState> state = new HashMap<>();

        private SortedMap<String, SortedSet<TopicPartition>> assignment;
        private Cluster metadata;
        public Map<String, PartitionAssignor.Assignment> assign(Cluster metadata, Map<String, PartitionAssignor.Subscription> subscriptions) {
            this.metadata = metadata;
            refreshClusterState(subscriptions);
            this.assignment = proposeAssignment();

            final Map<String, PartitionAssignor.Assignment> result = new HashMap<>();
            for (Map.Entry<String, SortedSet<TopicPartition>> entry : assignment.entrySet()) {
                result.put(entry.getKey(), new PartitionAssignor.Assignment(new ArrayList<>(entry.getValue())));
            }
            for (String member : subscriptions.keySet()) {
                if (!result.containsKey(member)) {
                    result.put(member, new PartitionAssignor.Assignment(new LinkedList<>()));
                }
            }

            System.out.println(result);
            System.out.println();
            return result;
        }

        public void updateHealthData(String member, String data) {
            state.get(member).data = data;
        }

        public boolean shouldRebalance() {
            if (assignment.equals(proposeAssignment())) {
                System.out.println("Current state of the cluster is compatible with the prior assignment.");
                return false;
            } else {
                System.out.println("Current state of the cluster is NOT compatible with the prior assignment. Rebalancing...");
                return true;
            }
        }

        private SortedMap<String, SortedSet<TopicPartition>> proposeAssignment() {
            final Map<TopicPartition, List<String>> candidates = new HashMap<>();
            for (Map.Entry<String, MemberState> entry : state.entrySet()) {
                final String memberId = entry.getKey();
                if (state.get(memberId).data.equals("bad")) {
                    System.out.println("Not considering bad instance " + memberId);
                } else {
                    final MemberState memberState = entry.getValue();
                    for (String topic : memberState.subscription.topics()) {
                        for (PartitionInfo partition : this.metadata.availablePartitionsForTopic(topic)) {
                            final TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
                            if (!candidates.containsKey(topicPartition)) {
                                candidates.put(topicPartition, new LinkedList<>());
                            }
                            candidates.get(topicPartition).add(entry.getKey());
                        }
                    }
                }
            }
            SortedMap<String, SortedSet<TopicPartition>> assignment = new TreeMap<>();
            for (Map.Entry<TopicPartition, List<String>> entry : candidates.entrySet()) {
                final TopicPartition topicPartition = entry.getKey();
                final String member = entry.getValue().get(0);
                if (!assignment.containsKey(member)) {
                    assignment.put(member, new TreeSet<>(Comparator.comparing(Object::toString)));
                }
                assignment.get(member).add(topicPartition);
            }
            return assignment;
        }

        private void refreshClusterState(Map<String, PartitionAssignor.Subscription> subscriptions) {
            state.clear();
            for (Map.Entry<String, PartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
                final PartitionAssignor.Subscription subscription = entry.getValue();

                final String memberId = entry.getKey();
                final Optional<String> groupInstanceId = subscription.groupInstanceId();

                final ByteBuffer buffer = subscription.userData();
                final String data = new String(buffer.array(), StandardCharsets.UTF_8);

                final MemberState memberState = new MemberState(memberId, subscription, data);

                state.put(memberId, memberState);
            }
        }

    }

    public static class MyAssignor implements PartitionAssignor, Configurable {

        private Map<String, ?> configs;
        private MyDynamicAssignmentLogic logic;

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            this.logic = (MyDynamicAssignmentLogic) configs.get("assignment.logic");
        }

        @Override
        public Subscription subscription(Set<String> topics) {
            return new Subscription(new ArrayList<>(topics),ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
            System.out.println("I'm apparently the leader. Doing assignment: " + configs.get("group.instance.id"));
            return logic.assign(metadata, subscriptions);
        }

        @Override
        public void onAssignment(Assignment assignment) {
            System.out.println("I [" + configs.get("group.instance.id") + "] got assignment: " + assignment);
        }

        @Override
        public void onAssignment(Assignment assignment, int generation) {
            System.out.println("I [" + configs.get("group.instance.id") + "] got generation[" + generation + "] assignment: " + assignment);
        }

        @Override
        public List<RebalanceProtocol> supportedProtocols() {
            return Collections.singletonList(RebalanceProtocol.EAGER);
        }

        @Override
        public short version() {
            return 0;
        }

        @Override
        public String name() {
            return getClass().getCanonicalName();
        }
    }

    public static class HBData implements ConsumerHeartbeatDataCallbacks, Configurable {
        private final Random random = new Random();

        private Map<String, ?> configs;
        private MyDynamicAssignmentLogic logic;

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            this.logic = (MyDynamicAssignmentLogic) configs.get("assignment.logic");
        }

        @Override
        public ByteBuffer memberUserData() {
            if (random.nextDouble() < 0.05) {
                return ByteBuffer.wrap("bad".getBytes(StandardCharsets.UTF_8));
            } else {
                return ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8));
            }
        }

        @Override
        public GroupHealth leaderAllMemberUserDatas(Map<String, ByteBuffer> userDatas) {
            if (userDatas.isEmpty()) {
                // no data reported, so the cluster health couldn't have changed.
                return GroupHealth.HEALTHY;
            }

            System.out.println("I'm apparently the leader. Re-evaluating assignment: " + configs.get("group.instance.id"));

            for (Map.Entry<String, ByteBuffer> entry : userDatas.entrySet()) {
                final String data = new String(entry.getValue().array(), StandardCharsets.UTF_8);
                logic.updateHealthData(entry.getKey(), data);
                System.out.println(Instant.now() + " member[" + entry.getKey() + "] data[" + new String(entry.getValue().array(), StandardCharsets.UTF_8) + "]");
            }
            System.out.println();
            if (logic.shouldRebalance()) {
                return GroupHealth.UNHEALTHY;
            } else {
                return GroupHealth.HEALTHY;
            }
        }
    }

    @Test
    public void testHeartBeatCommunication() throws InterruptedException {
        CLUSTER.createTopic("topic", 1, 1);

        final KafkaConsumer<Void, Void> consumer1 = new KafkaConsumer<>(
            mkMap(
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, HeartbeatIntegrationTest.class.getName()),
                mkEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "one"),
                mkEntry("assignment.logic", new MyDynamicAssignmentLogic()),
                mkEntry(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, MyAssignor.class.getName()),
                mkEntry(ConsumerConfig.HEARTBEAT_CALLBACK_CONFIG, HBData.class.getName())
            )
        );

        consumer1.subscribe(Pattern.compile("topic"));

        final KafkaConsumer<Void, Void> consumer2 = new KafkaConsumer<>(
            mkMap(
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, HeartbeatIntegrationTest.class.getName()),
                mkEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "two"),
                mkEntry("assignment.logic", new MyDynamicAssignmentLogic()),
                mkEntry(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, MyAssignor.class.getName()),
                mkEntry(ConsumerConfig.HEARTBEAT_CALLBACK_CONFIG, HBData.class.getName())
            )
        );

        consumer2.subscribe(Pattern.compile("topic"));

        while (true) {
            consumer1.poll(Duration.ZERO);
            consumer2.poll(Duration.ZERO);
            Thread.sleep(100);
        }
    }

}
