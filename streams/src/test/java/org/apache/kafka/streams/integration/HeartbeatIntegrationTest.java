package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerHeartbeatDataCallbacks;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

@Category({IntegrationTest.class})
public class HeartbeatIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(1);

    private static class HBData implements ConsumerHeartbeatDataCallbacks{
        @Override
        public ByteBuffer memberUserData() {
            return ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void leaderAllMemberUserDatas(Map<String, ByteBuffer> userDatas) {
            for (Map.Entry<String, ByteBuffer> entry : userDatas.entrySet()) {
                System.out.println(Instant.now() +" member["+entry.getKey()+"] data["+new String(entry.getValue().array(), StandardCharsets.UTF_8)+"]");
            }
        }
    }

    @Test
    public void testHeartBeatCommunication() throws InterruptedException {
        CLUSTER.createTopic("topic",1, 1);

        final KafkaConsumer<Void, Void> consumer1 = new KafkaConsumer<>(
            mkMap(
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, HeartbeatIntegrationTest.class.getName()),
                mkEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "one")
            )
        );

        consumer1.setHeartbeatCallbacks(new HBData());

        consumer1.subscribe(Pattern.compile("topic"));

        final KafkaConsumer<Void, Void> consumer2 = new KafkaConsumer<>(
            mkMap(
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, HeartbeatIntegrationTest.class.getName()),
                mkEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "two")
            )
        );

        consumer2.setHeartbeatCallbacks(new HBData());

        consumer2.subscribe(Pattern.compile("topic"));

        while (true) {
            consumer1.poll(Duration.ZERO);
            consumer2.poll(Duration.ZERO);
            Thread.sleep(100);
        }
    }

}
