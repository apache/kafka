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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
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

public class SimpleBenchmark {

    private final String kafka;
    private final String zookeeper;
    private final File stateDir;

    private static final String SOURCE_TOPIC = "simpleBenchmarkSourceTopic";
    private static final String SINK_TOPIC = "simpleBenchmarkSinkTopic";

    private static final long NUM_RECORDS = 10000000L;
    private static final Long END_KEY = NUM_RECORDS - 1;
    private static final int KEY_SIZE = 8;
    private static final int VALUE_SIZE = 100;
    private static final int RECORD_SIZE = KEY_SIZE + VALUE_SIZE;

    private static final Serde<byte[]> BYTE_SERDE = Serdes.ByteArray();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    public SimpleBenchmark(File stateDir, String kafka, String zookeeper) {
        super();
        this.stateDir = stateDir;
        this.kafka = kafka;
        this.zookeeper = zookeeper;
    }

    public static void main(String[] args) throws Exception {
        final File stateDir = new File("/tmp/kafka-streams-simple-benchmark");
        stateDir.mkdir();

        final File rocksdbDir = new File(stateDir, "rocksdb-test");
        rocksdbDir.mkdir();


        final String kafka = "localhost:9092";
        final String zookeeper = "localhost:2181";

        SimpleBenchmark benchmark = new SimpleBenchmark(stateDir, kafka, zookeeper);

        // producer performance
        benchmark.produce();
        // consumer performance
        benchmark.consume();
        // simple stream performance source->process
        benchmark.processStream();
        // simple stream performance source->sink
        benchmark.processStreamWithSink();
        // simple stream performance source->store
        benchmark.processStreamWithStateStore();
    }

    public void processStream() {
        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreams(stateDir, kafka, zookeeper, latch);

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

    public void processStreamWithSink() {
        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreamsWithSink(stateDir, kafka, zookeeper, latch);

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

    public void processStreamWithStateStore() {
        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreamsWithStateStore(stateDir, kafka, zookeeper, latch);

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

    public void produce() {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "simple-benchmark-produce");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        KafkaProducer<Long, byte[]> producer = new KafkaProducer<>(props);

        byte[] value = new byte[VALUE_SIZE];
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_RECORDS; i++) {
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, (long) i, value));
        }
        producer.close();

        long endTime = System.currentTimeMillis();

        System.out.println("Producer Performance [MB/sec write]: " + megaBytePerSec(endTime - startTime));
    }

    public void consume() {
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple-benchmark-consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(props);

        List<TopicPartition> partitions = getAllPartitions(consumer, SOURCE_TOPIC);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Long key = null;

        long startTime = System.currentTimeMillis();

        while (true) {
            ConsumerRecords<Long, byte[]> records = consumer.poll(500);
            if (records.isEmpty()) {
                if (END_KEY.equals(key))
                    break;
            } else {
                for (ConsumerRecord<Long, byte[]> record : records) {
                    key = record.key();
                }
            }
        }

        long endTime = System.currentTimeMillis();

        consumer.close();
        System.out.println("Consumer Performance [MB/sec read]: " + megaBytePerSec(endTime - startTime));
    }

    private KafkaStreams createKafkaStreams(File stateDir, String kafka, String zookeeper, final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-benchmark-streams");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Long, byte[]> source = builder.stream(LONG_SERDE, BYTE_SERDE, SOURCE_TOPIC);

        source.process(new ProcessorSupplier<Long, byte[]>() {
            @Override
            public Processor<Long, byte[]> get() {
                return new Processor<Long, byte[]>() {

                    @Override
                    public void init(ProcessorContext context) {
                    }

                    @Override
                    public void process(Long key, byte[] value) {
                        if (END_KEY.equals(key)) {
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

    private KafkaStreams createKafkaStreamsWithSink(File stateDir, String kafka, String zookeeper, final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-benchmark-streams-with-sink");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Long, byte[]> source = builder.stream(LONG_SERDE, BYTE_SERDE, SOURCE_TOPIC);

        source.to(LONG_SERDE, BYTE_SERDE, SINK_TOPIC);
        source.process(new ProcessorSupplier<Long, byte[]>() {
            @Override
            public Processor<Long, byte[]> get() {
                return new Processor<Long, byte[]>() {

                    @Override
                    public void init(ProcessorContext context) {
                    }

                    @Override
                    public void process(Long key, byte[] value) {
                        if (END_KEY.equals(key)) {
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


    private KafkaStreams createKafkaStreamsWithStateStore(File stateDir, String kafka, String zookeeper, final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-benchmark-streams-with-store");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        builder.addStateStore(Stores.create("store").withLongKeys().withByteArrayValues().persistent().build());

        KStream<Long, byte[]> source = builder.stream(LONG_SERDE, BYTE_SERDE, SOURCE_TOPIC);

        source.process(new ProcessorSupplier<Long, byte[]>() {
            @Override
            public Processor<Long, byte[]> get() {
                return new Processor<Long, byte[]>() {

                    KeyValueStore<Long, byte[]> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(ProcessorContext context) {
                        store = (KeyValueStore<Long, byte[]>) context.getStateStore("store");
                    }

                    @Override
                    public void process(Long key, byte[] value) {
                        store.put(key, value);

                        if (END_KEY.equals(key)) {
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
        return (double) (RECORD_SIZE * NUM_RECORDS / 1024 / 1024) / ((double) time / 1000);
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
