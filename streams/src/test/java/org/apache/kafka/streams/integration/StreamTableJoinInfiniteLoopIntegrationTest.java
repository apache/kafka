package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

@Category({IntegrationTest.class})
public class StreamTableJoinInfiniteLoopIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private String tableTopic;
    private String inputTopic;
    private String outputTopic;
    private String applicationId;

    private Properties streamsConfiguration;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws InterruptedException {
        streamsConfiguration = new Properties();

        final String safeTestName = safeUniqueTestName(getClass(), testName);

        tableTopic = "topic-b-" + safeTestName;
        inputTopic = "input-topic-" + safeTestName;
        outputTopic = "output-topic-" + safeTestName;
        applicationId = "app-" + safeTestName;

        CLUSTER.createTopic(inputTopic, 4, 1);
        CLUSTER.createTopic(tableTopic, 2, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    }

    @After
    public void whenShuttingDown() throws IOException {
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldMoveToRunningState() throws InterruptedException {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<Integer, String> stream = streamsBuilder.stream(inputTopic);
        final KTable<Integer, String> table = streamsBuilder.table(tableTopic);

        stream
            .selectKey((key, value) -> key)
            .join(table, (value1, value2) -> value2)
            .to(outputTopic);

        startStreams(streamsBuilder);
    }

    private KafkaStreams startStreams(final StreamsBuilder builder) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);


        kafkaStreams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.REBALANCING == oldState && KafkaStreams.State.RUNNING == newState) {
                latch.countDown();
            }
        });

        kafkaStreams.start();

        latch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

        return kafkaStreams;
    }
}
