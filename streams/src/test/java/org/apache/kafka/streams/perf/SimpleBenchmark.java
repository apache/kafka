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
package org.apache.kafka.streams.perf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Class that provides support for a series of benchmarks. It is usually driven by
 * tests/kafkatest/benchmarks/streams/streams_simple_benchmark_test.py.
 * If ran manually through the main() function below, you must do the following:
 * 1. Have ZK and a Kafka broker set up
 * 2. Run the loading step first: SimpleBenchmark localhost:9092 /tmp/statedir numRecords true "all"
 * 3. Run the stream processing step second: SimpleBenchmark localhost:9092 /tmp/statedir numRecords false "all"
 * Note that what changed is the 4th parameter, from "true" indicating that is a load phase, to "false" indicating
 * that this is a real run.
 *
 * Note that "all" is a convenience option when running this test locally and will not work when running the test
 * at scale (through tests/kafkatest/benchmarks/streams/streams_simple_benchmark_test.py). That is due to exact syncronization
 * needs for each test (e.g., you wouldn't want one instance to run "count" while another
 * is still running "consume"
 */
public class SimpleBenchmark {
    private static final String LOADING_PRODUCER_CLIENT_ID = "simple-benchmark-loading-producer";

    private static final String SOURCE_TOPIC_ONE = "simpleBenchmarkSourceTopic1";
    private static final String SOURCE_TOPIC_TWO = "simpleBenchmarkSourceTopic2";
    private static final String SINK_TOPIC = "simpleBenchmarkSinkTopic";

    private static final String YAHOO_CAMPAIGNS_TOPIC = "yahooCampaigns";
    private static final String YAHOO_EVENTS_TOPIC = "yahooEvents";

    private static final ValueJoiner<byte[], byte[], byte[]> VALUE_JOINER = new ValueJoiner<byte[], byte[], byte[]>() {
        @Override
        public byte[] apply(final byte[] value1, final byte[] value2) {
            // dump joiner in order to have as less join overhead as possible
            if (value1 != null) {
                return value1;
            } else if (value2 != null) {
                return value2;
            } else {
                return new byte[100];
            }
        }
    };

    private static final Serde<byte[]> BYTE_SERDE = Serdes.ByteArray();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();

    long processedBytes = 0L;
    int processedRecords = 0;

    private static final long POLL_MS = 500L;
    private static final long COMMIT_INTERVAL_MS = 30000L;
    private static final int MAX_POLL_RECORDS = 1000;

    /* ----------- benchmark variables that are hard-coded ----------- */

    private static final int KEY_SPACE_SIZE = 10000;

    private static final long STREAM_STREAM_JOIN_WINDOW = 10000L;

    private static final long AGGREGATE_WINDOW_SIZE = 1000L;

    private static final long AGGREGATE_WINDOW_ADVANCE = 500L;

    private static final int SOCKET_SIZE_BYTES = 1024 * 1024;

    // the following numbers are based on empirical results and should only
    // be considered for updates when perf results have significantly changed

    // with at least 10 million records, we run for at most 3 minutes
    private static final int MAX_WAIT_MS = 3 * 60 * 1000;

    /* ----------- benchmark variables that can be specified ----------- */

    final String testName;

    final int numRecords;

    final Properties props;

    private final int valueSize;

    private final double keySkew;

    /* ----------- ----------------------------------------- ----------- */


    private SimpleBenchmark(final Properties props,
                            final String testName,
                            final int numRecords,
                            final double keySkew,
                            final int valueSize) {
        super();
        this.props = props;
        this.testName = testName;
        this.keySkew = keySkew;
        this.valueSize = valueSize;
        this.numRecords = numRecords;
    }

    private void run() {
        switch (testName) {
            // loading phases
            case "load-one":
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                break;
            case "load-two":
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_TWO, numRecords, keySkew, valueSize);
                break;

            // testing phases
            case "consume":
                consume(SOURCE_TOPIC_ONE);
                break;
            case "consumeproduce":
                consumeAndProduce(SOURCE_TOPIC_ONE);
                break;
            case "streamcount":
                countStreamsNonWindowed(SOURCE_TOPIC_ONE);
                break;
            case "streamcountwindowed":
                countStreamsWindowed(SOURCE_TOPIC_ONE);
                break;
            case "streamprocess":
                processStream(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithsink":
                processStreamWithSink(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithstatestore":
                processStreamWithStateStore(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithwindowstore":
                processStreamWithWindowStore(SOURCE_TOPIC_ONE);
                break;
            case "streamtablejoin":
                streamTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "streamstreamjoin":
                streamStreamJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "tabletablejoin":
                tableTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "yahoo":
                yahooBenchmark(YAHOO_CAMPAIGNS_TOPIC, YAHOO_EVENTS_TOPIC);
                break;
            default:
                throw new RuntimeException("Unknown test name " + testName);

        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.err.println("Not enough parameters are provided; expecting propFileName, testName, numRecords, keySkew, valueSize");
            System.exit(1);
        }

        String propFileName = args[0];
        String testName = args[1].toLowerCase(Locale.ROOT);
        int numRecords = Integer.parseInt(args[2]);
        double keySkew = Double.parseDouble(args[3]); // 0d means even distribution
        int valueSize = Integer.parseInt(args[4]);

        final Properties props = Utils.loadProps(propFileName);
        final String kafka = props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(1);
        }

        // Note: this output is needed for automated tests and must not be removed
        System.out.println("StreamsTest instance started");

        System.out.println("testName=" + testName);
        System.out.println("streamsProperties=" + props);
        System.out.println("numRecords=" + numRecords);
        System.out.println("keySkew=" + keySkew);
        System.out.println("valueSize=" + valueSize);

        final SimpleBenchmark benchmark = new SimpleBenchmark(props, testName, numRecords, keySkew, valueSize);

        benchmark.run();
    }

    public void setStreamProperties(final String applicationId) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "simple-benchmark");
        props.put(StreamsConfig.POLL_MS_CONFIG, POLL_MS);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

        // improve producer throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);

        //TODO remove this config or set to smaller value when KIP-91 is merged
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 60000);
    }

    private Properties setProduceConsumeProperties(final String clientId) {
        Properties clientProps = new Properties();
        clientProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        clientProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        clientProps.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        clientProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);
        clientProps.put(ProducerConfig.SEND_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        clientProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        clientProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        clientProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        clientProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        clientProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        clientProps.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        clientProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        return clientProps;
    }

    void resetStats() {
        processedRecords = 0;
        processedBytes = 0L;
    }

    /**
     * Produce values to a topic
     * @param clientId String specifying client ID
     * @param topic Topic to produce to
     * @param numRecords Number of records to produce
     * @param keySkew Key zipf distribution skewness
     * @param valueSize Size of value in bytes
     */
    private void produce(final String clientId,
                         final String topic,
                         final int numRecords,
                         final double keySkew,
                         final int valueSize) {
        final Properties props = setProduceConsumeProperties(clientId);
        final ZipfGenerator keyGen = new ZipfGenerator(KEY_SPACE_SIZE, keySkew);

        try (final KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(props)) {
            final byte[] value = new byte[valueSize];
            // put some random values to increase entropy. Some devices
            // like SSDs do compression and if the array is all zeros
            // the performance will be too good.
            new Random(System.currentTimeMillis()).nextBytes(value);

            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(topic, keyGen.next(), value));
            }
        }
    }

    private void consumeAndProduce(final String topic) {
        final Properties consumerProps = setProduceConsumeProperties("simple-benchmark-consumer");
        final Properties producerProps = setProduceConsumeProperties("simple-benchmark-producer");

        final long startTime = System.currentTimeMillis();
        try (final KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
             final KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps)) {
            final List<TopicPartition> partitions = getAllPartitions(consumer, topic);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            while (true) {
                final ConsumerRecords<Integer, byte[]> records = consumer.poll(POLL_MS);
                if (records.isEmpty()) {
                    if (processedRecords == numRecords) {
                        break;
                    }
                } else {
                    for (final ConsumerRecord<Integer, byte[]> record : records) {
                        producer.send(new ProducerRecord<>(SINK_TOPIC, record.key(), record.value()));
                        processedRecords++;
                        processedBytes += record.value().length + Integer.SIZE;
                        if (processedRecords == numRecords) {
                            break;
                        }
                    }
                }
                if (processedRecords == numRecords) {
                    break;
                }
            }
        }

        final long endTime = System.currentTimeMillis();

        printResults("ConsumerProducer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private void consume(final String topic) {
        final Properties consumerProps = setProduceConsumeProperties("simple-benchmark-consumer");

        final long startTime = System.currentTimeMillis();

        try (final KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            final List<TopicPartition> partitions = getAllPartitions(consumer, topic);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            while (true) {
                final ConsumerRecords<Integer, byte[]> records = consumer.poll(POLL_MS);
                if (records.isEmpty()) {
                    if (processedRecords == numRecords) {
                        break;
                    }
                } else {
                    for (final ConsumerRecord<Integer, byte[]> record : records) {
                        processedRecords++;
                        processedBytes += record.value().length + Integer.SIZE;
                        if (processedRecords == numRecords) {
                            break;
                        }
                    }
                }
                if (processedRecords == numRecords) {
                    break;
                }
            }
        }

        final long endTime = System.currentTimeMillis();

        printResults("Consumer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private void processStream(final String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-source");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(topic, Consumed.with(INTEGER_SERDE, BYTE_SERDE)).peek(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Source Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void processStreamWithSink(final String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-source-sink");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, byte[]> source = builder.stream(topic);
        source.peek(new CountDownAction(latch)).to(SINK_TOPIC);

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams SourceSink Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void processStreamWithStateStore(final String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-with-store");

        final StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<KeyValueStore<Integer, byte[]>> storeBuilder
                = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("store"), INTEGER_SERDE, BYTE_SERDE);
        builder.addStateStore(storeBuilder.withCachingEnabled());

        final KStream<Integer, byte[]> source = builder.stream(topic);

        source.peek(new CountDownAction(latch)).process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {
                    KeyValueStore<Integer, byte[]> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext context) {
                        super.init(context);
                        store = (KeyValueStore<Integer, byte[]>) context.getStateStore("store");
                    }

                    @Override
                    public void process(final Integer key, final byte[] value) {
                        store.get(key);
                        store.put(key, value);
                    }
                };
            }
        }, "store");

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Stateful Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void processStreamWithWindowStore(final String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-with-store");

        final StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<WindowStore<Integer, byte[]>> storeBuilder
                = Stores.windowStoreBuilder(Stores.persistentWindowStore("store",
                AGGREGATE_WINDOW_SIZE * 3,
                3,
                AGGREGATE_WINDOW_SIZE,
                false),
                INTEGER_SERDE, BYTE_SERDE);
        builder.addStateStore(storeBuilder.withCachingEnabled());

        final KStream<Integer, byte[]> source = builder.stream(topic);

        source.peek(new CountDownAction(latch)).process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {
                    WindowStore<Integer, byte[]> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext context) {
                        super.init(context);
                        store = (WindowStore<Integer, byte[]>) context.getStateStore("store");
                    }

                    @Override
                    public void process(final Integer key, final byte[] value) {
                        final long timestamp = context().timestamp();
                        final KeyValueIterator<Windowed<Integer>, byte[]> iter = store.fetch(key - 10, key + 10, timestamp - 1000L, timestamp + 1000L);
                        while (iter.hasNext()) {
                            iter.next();
                        }
                        iter.close();

                        store.put(key, value);
                    }
                };
            }
        }, "store");

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Stateful Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     */
    private void countStreamsNonWindowed(final String sourceTopic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-nonwindowed-count");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, byte[]> input = builder.stream(sourceTopic);

        input.peek(new CountDownAction(latch))
                .groupByKey()
                .count();

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Count Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     */
    private void countStreamsWindowed(final String sourceTopic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-windowed-count");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, byte[]> input = builder.stream(sourceTopic);

        input.peek(new CountDownAction(latch))
                .groupByKey()
                .windowedBy(TimeWindows.of(AGGREGATE_WINDOW_SIZE).advanceBy(AGGREGATE_WINDOW_ADVANCE))
                .count();

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Count Windowed Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KTable left join. The setup is such that each
     * KStream record joins to exactly one element in the KTable
     */
    private void streamTableJoin(final String kStreamTopic, final String kTableTopic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-stream-table-join");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, byte[]> input1 = builder.stream(kStreamTopic);
        final KTable<Integer, byte[]> input2 = builder.table(kTableTopic);

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KStream left join. The setup is such that each
     * KStream record joins to exactly one element in the other KStream
     */
    private void streamStreamJoin(final String kStreamTopic1, final String kStreamTopic2) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-stream-stream-join");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, byte[]> input1 = builder.stream(kStreamTopic1);
        final KStream<Integer, byte[]> input2 = builder.stream(kStreamTopic2);

        input1.leftJoin(input2, VALUE_JOINER, JoinWindows.of(STREAM_STREAM_JOIN_WINDOW)).foreach(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKStream LeftJoin Performance [records/latency/rec-sec/MB-sec  joined]: ", latch);
    }

    /**
     * Measure the performance of a KTable-KTable left join. The setup is such that each
     * KTable record joins to exactly one element in the other KTable
     */
    private void tableTableJoin(String kTableTopic1, String kTableTopic2) {
        final CountDownLatch latch = new CountDownLatch(1);

        // setup join
        setStreamProperties("simple-benchmark-table-table-join");

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, byte[]> input1 = builder.table(kTableTopic1);
        final KTable<Integer, byte[]> input2 = builder.table(kTableTopic2);

        input1.leftJoin(input2, VALUE_JOINER).toStream().foreach(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KTableKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    void printResults(final String nameOfBenchmark, final long latency) {
        System.out.println(nameOfBenchmark +
            processedRecords + "/" +
            latency + "/" +
            recordsPerSec(latency, processedRecords) + "/" +
            megabytesPerSec(latency, processedBytes));
    }

    void runGenericBenchmark(final KafkaStreams streams, final String nameOfBenchmark, final CountDownLatch latch) {
        streams.start();

        final long startTime = System.currentTimeMillis();
        long endTime = startTime;

        while (latch.getCount() > 0 && (endTime - startTime < MAX_WAIT_MS)) {
            try {
                latch.await(1000, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ex) {
                Thread.interrupted();
            }

            endTime = System.currentTimeMillis();
        }
        streams.close();

        printResults(nameOfBenchmark, endTime - startTime);
    }

    private class CountDownAction implements ForeachAction<Integer, byte[]> {
        private final CountDownLatch latch;

        CountDownAction(final CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void apply(final Integer key, final byte[] value) {
            processedRecords++;
            processedBytes += Integer.SIZE + value.length;

            if (processedRecords == numRecords) {
                this.latch.countDown();
            }
        }
    }

    private KafkaStreams createKafkaStreamsWithExceptionHandler(final StreamsBuilder builder, final Properties props) {
        final KafkaStreams streamsClient = new KafkaStreams(builder.build(), props);
        streamsClient.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.out.println("FATAL: An unexpected exception is encountered on thread " + t + ": " + e);

                streamsClient.close(30, TimeUnit.SECONDS);
            }
        });

        return streamsClient;
    }
    
    private double megabytesPerSec(long time, long processedBytes) {
        return  (processedBytes / 1024.0 / 1024.0) / (time / 1000.0);
    }

    private double recordsPerSec(long time, int numRecords) {
        return numRecords / (time / 1000.0);
    }

    private List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer, String... topics) {
        ArrayList<TopicPartition> partitions = new ArrayList<>();

        for (String topic : topics) {
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

    private void yahooBenchmark(final String campaignsTopic, final String eventsTopic) {
        final YahooBenchmark benchmark = new YahooBenchmark(this, campaignsTopic, eventsTopic);

        benchmark.run();
    }

    private class ZipfGenerator {
        final private Random rand = new Random(System.currentTimeMillis());
        final private int size;
        final private double skew;

        private double bottom = 0.0d;

        ZipfGenerator(final int size, final double skew) {
            this.size = size;
            this.skew = skew;

            for (int i = 1; i < size; i++) {
                this.bottom += 1.0d / Math.pow(i, this.skew);
            }
        }

        int next() {
            if (skew == 0.0d) {
                return rand.nextInt(size);
            } else {
                int rank;
                double dice;
                double frequency;

                rank = rand.nextInt(size);
                frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                dice = rand.nextDouble();

                while (!(dice < frequency)) {
                    rank = rand.nextInt(size);
                    frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                    dice = rand.nextDouble();
                }

                return rank;
            }
        }
    }
}
