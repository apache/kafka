package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.produceKeyValuesSynchronously;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinRecordsReceived;

public class StatelessIntegrationTest {
    @Test
    public void statelessTopologiesShouldNotCreateDirectories() throws IOException, InterruptedException, ExecutionException {
        final EmbeddedKafkaCluster broker = new EmbeddedKafkaCluster(1);
        broker.start();
        broker.deleteAllTopicsAndWait(30_000L);

        final String applicationId = UUID.randomUUID().toString();

        final String inputTopic = "input" + applicationId;
        final String outputTopic = "output" + applicationId;

        broker.createTopic(inputTopic, 2, 1);
        broker.createTopic(outputTopic, 2, 1);

        final String path = TestUtils.tempDirectory(applicationId).getPath();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, path);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> filter = input.filter((k, s) -> s.length() % 2 == 0);
        final KStream<String, String> map = filter.map((k, v) -> new KeyValue<>(k, k + v));
        map.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        try {
            kafkaStreams.start();

            final Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());
            produceKeyValuesSynchronously(
                inputTopic,
                asList(new KeyValue<>("a", "b"), new KeyValue<>("c", "de")), producerConfig,
                new SystemTime()
            );

            final Properties consumerConfig = new Properties();
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");

            waitUntilMinRecordsReceived(
                consumerConfig,
                outputTopic,
                1
            );

            if (new File(path).exists()) {
                final List<Path> collect =
                    Files.find(Paths.get(path), 999, (p, bfa) -> true).collect(Collectors.toList());
                Assert.fail(path + " should not have existed, but it did: " + collect);
            }

        } finally {
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
    }
}
