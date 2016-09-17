/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.Properties;
import java.util.Random;

public class SimpleBenchmark {

    private final String kafka;
    private final String zookeeper;
    private final File stateDir;

    private static final String SOURCE_TOPIC = "simpleBenchmarkSourceTopic";
    private static final String SINK_TOPIC = "simpleBenchmarkSinkTopic";

    private static final String JOIN_TOPIC_1_PREFIX = "joinSourceTopic1";
    private static final String JOIN_TOPIC_2_PREFIX = "joinSourceTopic2";
    private static final ValueJoiner VALUE_JOINER = new ValueJoiner<byte[], byte[], byte[]>() {
        @Override
        public byte[] apply(final byte[] value1, final byte[] value2) {
            if (value1 == null && value2 == null)
                return new byte[VALUE_SIZE];
            if (value1 == null && value2 != null)
                return value2;
            if (value1 != null && value2 == null)
                return value1;

            byte[] tmp = new byte[value1.length + value2.length];
            System.arraycopy(value1, 0, tmp, 0, value1.length);
            System.arraycopy(value2, 0, tmp, value1.length, value2.length);
            return tmp;
        }
    };

    private static int numRecords;
    private static Integer endKey;
    private static final int KEY_SIZE = 8;
    private static final int VALUE_SIZE = 100;
    private static final int RECORD_SIZE = KEY_SIZE + VALUE_SIZE;

    private static final Serde<byte[]> BYTE_SERDE = Serdes.ByteArray();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();

    public SimpleBenchmark(File stateDir, String kafka, String zookeeper) {
        super();
        this.stateDir = stateDir;
        this.kafka = kafka;
        this.zookeeper = zookeeper;
    }

    public static void main(String[] args) throws Exception {
        String kafka = args.length > 0 ? args[0] : "localhost:9092";
        String zookeeper = args.length > 1 ? args[1] : "localhost:2181";
        String stateDirStr = args.length > 2 ? args[2] : "/tmp/kafka-streams-simple-benchmark";
        numRecords = args.length > 3 ? Integer.parseInt(args[3]) : 10000000;
        endKey = numRecords - 1;

        final File stateDir = new File(stateDirStr);
        stateDir.mkdir();
        final File rocksdbDir = new File(stateDir, "rocksdb-test");
        rocksdbDir.mkdir();

        // Note: this output is needed for automated tests and must not be removed
        System.out.println("SimpleBenchmark instance started");
        System.out.println("kafka=" + kafka);
        System.out.println("zookeeper=" + zookeeper);
        System.out.println("stateDir=" + stateDir);
        System.out.println("numRecords=" + numRecords);

        SimpleBenchmark benchmark = new SimpleBenchmark(stateDir, kafka, zookeeper);

        // producer performance
        benchmark.produce(SOURCE_TOPIC, VALUE_SIZE, "simple-benchmark-produce", numRecords, true, numRecords, true);
        // consumer performance
        benchmark.consume(SOURCE_TOPIC);
        // simple stream performance source->process
        benchmark.processStream(SOURCE_TOPIC);
        // simple stream performance source->sink
        benchmark.processStreamWithSink(SOURCE_TOPIC);
        // simple stream performance source->store
        benchmark.processStreamWithStateStore(SOURCE_TOPIC);
        // simple streams performance KSTREAM-KTABLE join
        benchmark.kStreamKTableJoin(JOIN_TOPIC_1_PREFIX + "kStreamKTable", JOIN_TOPIC_2_PREFIX + "kStreamKTable");
        // simple streams performance KSTREAM-KSTREAM join
        benchmark.kStreamKStreamJoin(JOIN_TOPIC_1_PREFIX + "kStreamKStream", JOIN_TOPIC_2_PREFIX + "kStreamKStream");
        // simple streams performance KTABLE-KTABLE join
        benchmark.kTableKTableJoin(JOIN_TOPIC_1_PREFIX + "kTableKTable", JOIN_TOPIC_2_PREFIX + "kTableKTable");
    }

    private Properties setJoinProperties(final String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        return props;
    }

    /**
     * Measure the performance of a KStream-KTable left join. The setup is such that each
     * KStream record joins to exactly one element in the KTable
     */
    public void kStreamKTableJoin(String kStreamTopic, String kTableTopic) throws Exception {
        CountDownLatch latch = new CountDownLatch(numRecords);

        // initialize topics
        System.out.println("Initializing kStreamTopic " + kStreamTopic);
        produce(kStreamTopic, VALUE_SIZE, "simple-benchmark-produce-kstream", numRecords, false, numRecords, false);
        System.out.println("Initializing kTableTopic " + kTableTopic);
        produce(kTableTopic, VALUE_SIZE, "simple-benchmark-produce-ktable", numRecords, true, numRecords, false);

        // setup join
        Properties props = setJoinProperties("simple-benchmark-kstream-ktable-join");
        final KafkaStreams streams = createKafkaStreamsKStreamKTableJoin(props, kStreamTopic, kTableTopic, latch);

        // run benchmark
        runJoinBenchmark(streams, "Streams KStreamKTable LeftJoin Performance [MB/s joined]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KStream left join. The setup is such that each
     * KStream record joins to exactly one element in the other KStream
     */
    public void kStreamKStreamJoin(String kStreamTopic1, String kStreamTopic2) throws Exception {
        CountDownLatch latch = new CountDownLatch(numRecords);

        // initialize topics
        System.out.println("Initializing kStreamTopic " + kStreamTopic1);
        produce(kStreamTopic1, VALUE_SIZE, "simple-benchmark-produce-kstream-topic1", numRecords, true, numRecords, false);
        System.out.println("Initializing kStreamTopic " + kStreamTopic2);
        produce(kStreamTopic2, VALUE_SIZE, "simple-benchmark-produce-kstream-topic2", numRecords, true, numRecords, false);

        // setup join
        Properties props = setJoinProperties("simple-benchmark-kstream-kstream-join");
        final KafkaStreams streams = createKafkaStreamsKStreamKStreamJoin(props, kStreamTopic1, kStreamTopic2, latch);

        // run benchmark
        runJoinBenchmark(streams, "Streams KStreamKStream LeftJoin Performance [MB/s joined]: ", latch);
    }

    /**
     * Measure the performance of a KTable-KTable left join. The setup is such that each
     * KTable record joins to exactly one element in the other KTable
     */
    public void kTableKTableJoin(String kTableTopic1, String kTableTopic2) throws Exception {
        CountDownLatch latch = new CountDownLatch(numRecords);

        // initialize topics
        System.out.println("Initializing kTableTopic " + kTableTopic1);
        produce(kTableTopic1, VALUE_SIZE, "simple-benchmark-produce-ktable-topic1", numRecords, true, numRecords, false);
        System.out.println("Initializing kTableTopic " + kTableTopic2);
        produce(kTableTopic2, VALUE_SIZE, "simple-benchmark-produce-ktable-topic2", numRecords, true, numRecords, false);

        // setup join
        Properties props = setJoinProperties("simple-benchmark-ktable-ktable-join");
        final KafkaStreams streams = createKafkaStreamsKTableKTableJoin(props, kTableTopic1, kTableTopic2, latch);

        // run benchmark
        runJoinBenchmark(streams, "Streams KTableKTable LeftJoin Performance [MB/s joined]: ", latch);
    }

    private void runJoinBenchmark(final KafkaStreams streams, final String nameOfBenchmark, final CountDownLatch latch) {
        streams.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                //ignore
            }
        }
        long endTime = System.currentTimeMillis();


        System.out.println(nameOfBenchmark + megaBytePerSec(endTime - startTime, numRecords, KEY_SIZE + VALUE_SIZE));

        streams.close();
    }



    public void processStream(String topic) {
        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreams(topic, stateDir, kafka, zookeeper, latch);

        Thread thread = new Thread() {
            public void run() {
                streams.start();
            }
        };
        thread.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Streams Performance [MB/sec read]: " + megaBytePerSec(endTime - startTime));

        streams.close();
        try {
            thread.join();
        } catch (Exception ex) {
            // ignore
        }
    }

    public void processStreamWithSink(String topic) {
        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreamsWithSink(topic, stateDir, kafka, zookeeper, latch);

        Thread thread = new Thread() {
            public void run() {
                streams.start();
            }
        };
        thread.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Streams Performance [MB/sec read+write]: " + megaBytePerSec(endTime - startTime));

        streams.close();
        try {
            thread.join();
        } catch (Exception ex) {
            // ignore
        }
    }

    public void processStreamWithStateStore(String topic) {
        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreamsWithStateStore(topic, stateDir, kafka, zookeeper, latch);

        Thread thread = new Thread() {
            public void run() {
                streams.start();
            }
        };
        thread.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Streams Performance [MB/sec read+store]: " + megaBytePerSec(endTime - startTime));

        streams.close();
        try {
            thread.join();
        } catch (Exception ex) {
            // ignore
        }
    }

    /**
     * Produce values to a topic
     * @param topic Topic to produce to
     * @param valueSizeBytes Size of value in bytes
     * @param clientId String specifying client ID
     * @param numRecords Number of records to produce
     * @param sequential if True, then keys are produced sequentially from 0 to upperRange. In this case upperRange must be >= numRecords.
     *                   if False, then keys are produced randomly in range [0, upperRange)
     * @param printStats if True, print stats on how long producing took. If False, don't print stats. False can be used
     *                   when this produce step is part of another benchmark that produces its own stats
     */
    public void produce(String topic, int valueSizeBytes, String clientId, int numRecords, boolean sequential,
                        int upperRange, boolean printStats) throws Exception {

        if (sequential) {
            if (upperRange < numRecords) throw new Exception("UpperRange must be >= numRecords");
        }
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        int key = 0;
        Random rand = new Random();
        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(props);

        byte[] value = new byte[valueSizeBytes];
        long startTime = System.currentTimeMillis();

        if (sequential) key = 0;
        else key = rand.nextInt(upperRange);
        for (int i = 0; i < numRecords; i++) {
            producer.send(new ProducerRecord<>(topic, key, value));
            if (sequential) key++;
            else key = rand.nextInt(upperRange);
        }
        producer.close();

        long endTime = System.currentTimeMillis();

        if (printStats)
            System.out.println("Producer Performance [MB/sec write]: " + megaBytePerSec(endTime - startTime, numRecords, KEY_SIZE + valueSizeBytes));
    }

    public void consume(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple-benchmark-consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(props);

        List<TopicPartition> partitions = getAllPartitions(consumer, topic);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Integer key = null;

        long startTime = System.currentTimeMillis();

        while (true) {
            ConsumerRecords<Integer, byte[]> records = consumer.poll(500);
            if (records.isEmpty()) {
                if (endKey.equals(key))
                    break;
            } else {
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    Integer recKey = record.key();

                    if (key == null || key < recKey)
                        key = recKey;
                }
            }
        }

        long endTime = System.currentTimeMillis();

        consumer.close();
        System.out.println("Consumer Performance [MB/sec read]: " + megaBytePerSec(endTime - startTime));
    }

    private KafkaStreams createKafkaStreams(String topic, File stateDir, String kafka, String zookeeper, final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-benchmark-streams");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Integer, byte[]> source = builder.stream(INTEGER_SERDE, BYTE_SERDE, topic);

        source.process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {

                    @Override
                    public void init(ProcessorContext context) {
                    }

                    @Override
                    public void process(Integer key, byte[] value) {
                        if (endKey.equals(key)) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        });

        return new KafkaStreams(builder, props);
    }

    private KafkaStreams createKafkaStreamsWithSink(String topic, File stateDir, String kafka, String zookeeper, final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-benchmark-streams-with-sink");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Integer, byte[]> source = builder.stream(INTEGER_SERDE, BYTE_SERDE, topic);

        source.to(INTEGER_SERDE, BYTE_SERDE, SINK_TOPIC);
        source.process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {
                    @Override
                    public void init(ProcessorContext context) {
                    }

                    @Override
                    public void process(Integer key, byte[] value) {
                        if (endKey.equals(key)) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        });

        return new KafkaStreams(builder, props);
    }

    private class CountDownAction<K, V> implements ForeachAction<K, V> {
        private CountDownLatch latch;
        CountDownAction(final CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void apply(K key, V value) {
            this.latch.countDown();
        }
    }

    private KafkaStreams createKafkaStreamsKStreamKTableJoin(Properties streamConfig, String kStreamTopic,
                                                             String kTableTopic, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, byte[]> input1 = builder.stream(kStreamTopic);
        final KTable<Long, byte[]> input2 = builder.table(kTableTopic, kTableTopic + "-store");

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        return new KafkaStreams(builder, streamConfig);
    }

    private KafkaStreams createKafkaStreamsKTableKTableJoin(Properties streamConfig, String kTableTopic1,
                                                            String kTableTopic2, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KTable<Long, byte[]> input1 = builder.table(kTableTopic1, kTableTopic1 + "-store");
        final KTable<Long, byte[]> input2 = builder.table(kTableTopic2, kTableTopic2 + "-store");

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        return new KafkaStreams(builder, streamConfig);
    }

    private KafkaStreams createKafkaStreamsKStreamKStreamJoin(Properties streamConfig, String kStreamTopic1,
                                                              String kStreamTopic2, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, byte[]> input1 = builder.stream(kStreamTopic1);
        final KStream<Long, byte[]> input2 = builder.stream(kStreamTopic2);
        final long timeDifferenceMs = 10000L;

        input1.leftJoin(input2, VALUE_JOINER, JoinWindows.of(timeDifferenceMs)).foreach(new CountDownAction(latch));

        return new KafkaStreams(builder, streamConfig);
    }

    private KafkaStreams createKafkaStreamsWithStateStore(String topic, File stateDir, String kafka, String zookeeper,
                                                          final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-benchmark-streams-with-store");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        builder.addStateStore(Stores.create("store").withIntegerKeys().withByteArrayValues().persistent().build());

        KStream<Integer, byte[]> source = builder.stream(INTEGER_SERDE, BYTE_SERDE, topic);

        source.process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {
                    KeyValueStore<Integer, byte[]> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(ProcessorContext context) {
                        store = (KeyValueStore<Integer, byte[]>) context.getStateStore("store");
                    }

                    @Override
                    public void process(Integer key, byte[] value) {
                        store.put(key, value);

                        if (endKey.equals(key)) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        }, "store");

        return new KafkaStreams(builder, props);
    }

    private double megaBytePerSec(long time) {
        return (double) (RECORD_SIZE * numRecords / 1024 / 1024) / ((double) time / 1000);
    }

    private double megaBytePerSec(long time, int numRecords, int recordSizeBytes) {
        return (double) (recordSizeBytes * numRecords / 1024 / 1024) / ((double) time / 1000);
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

}
