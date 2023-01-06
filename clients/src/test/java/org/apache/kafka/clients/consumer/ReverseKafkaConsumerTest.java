package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class ReverseKafkaConsumerTest {

    private static KafkaContainer kafka;
    private static AdminClient adminClient;

    @BeforeAll
    static void beforeAll() throws InterruptedException {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));
        kafka.start();

        adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    @AfterAll
    static void afterAll() {
        if (adminClient != null) {
            adminClient.close();
        }

        kafka.stop();
    }

    @Test
    void shouldConsumeSinglePollInReverse() throws Exception {
        // -- create the topic
        adminClient.createTopics(List.of(new NewTopic("test-topic", 2, (short) 1))).all().get();

        // -- produce some messages
        try (var producer = createProducer()) {
            sendMessages(producer, "test-topic", 10);
        }

        // -- consume the messages
        try (var consumer = createConsumer()) {
            consumer.subscribe(List.of("test-topic"));

            Seq<ConsumerRecord<String, Integer>> records = TestUtils.consumeRecords(consumer, 10, 10000).toList();

            assertThat(records.size()).isEqualTo(10);
        }
    }

//	@Test
//	void shouldConsumeInReverse() {
//		fail("Not yet implemented");
//	}

    private Producer<String, Integer> createProducer() {
        //@formatter:off
        return new KafkaProducer<>(org.apache.kafka.test.TestUtils.producerConfig(
                kafka.getBootstrapServers(), StringSerializer.class, IntegerSerializer.class
        ));
        //@formatter:on
    }

    private KafkaConsumer<String, Integer> createConsumer() {
        //@formatter:off
        return new KafkaConsumer<>(org.apache.kafka.test.TestUtils.consumerConfig(
                kafka.getBootstrapServers(), StringDeserializer.class, IntegerDeserializer.class
        ));
        //@formatter:on
    }

    private void sendMessages(Producer<String, Integer> producer, String topic, int count) throws ExecutionException, InterruptedException {
        for (int i = 0; i < count; i++) {
            producer.send(new ProducerRecord<>(topic, "test-" + i, i)).get();
        }
    }
}