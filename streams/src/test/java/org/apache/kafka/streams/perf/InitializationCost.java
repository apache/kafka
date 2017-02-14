/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.perf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public class InitializationCost {

    private final static int STORES = 10;

    public static void main(String[] args) throws InterruptedException {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "init-cost");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
//        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 60 * 1000);

        final String inputTopic = "input";


        final KStreamBuilder builder = new KStreamBuilder();
        final CountDownLatch startupLatch = new CountDownLatch(STORES);
        final AtomicBoolean measure = new AtomicBoolean(false);
        final ConcurrentHashMap<Long, AtomicLong> map = new ConcurrentHashMap<>();


        for (int i = 0; i < STORES; i++) {
            final KStream<Long, Long> inputStream = builder.stream(Serdes.Long(), Serdes.Long(), inputTopic + "-" + i);
            final KGroupedStream<Long, Long> groupedStream = inputStream
                    .groupByKey(Serdes.Long(), Serdes.Long());

            groupedStream
                    .count("count-" + i);
        }


        final KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);
        kafkaStreams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
                System.out.println(newState);
            }
        });
        final long start = System.currentTimeMillis();
        kafkaStreams.start();
        startupLatch.await();
        final long took = System.currentTimeMillis() - start;
        System.out.println("It took " + TimeUnit.MILLISECONDS.toSeconds(took) + " seconds to restore " + STORES + " "
                                   + " STORES");
//        measure.set(true);
//        System.out.println("Begin Measurement");
//        final long runLength = TimeUnit.MINUTES.toMillis(5);
//        Thread.sleep(runLength);
//        measure.set(false);
//        System.out.println("End Measurement");
//
////        producerThread.interrupt();
//        kafkaStreams.close();
//
//        long totalMessages = 0;
//        for (final AtomicLong atomicLong : map.values()) {
//            totalMessages += atomicLong.get();
//        }
//
//        final long messagesPerSecond = totalMessages / TimeUnit.MILLISECONDS.toSeconds(runLength);
//        System.out.println("Messages per second = " + messagesPerSecond + " total messages = " + totalMessages);

    }


    private static void startProducerThread(final String inputTopic) {

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(producerProperties, new LongSerializer(), new LongSerializer());
        final Random random = new Random();
        final long start = System.currentTimeMillis();
        long messagesSent = 0;
//                while (!Thread.currentThread().isInterrupted()) {
//                    rateLimiter.acquire();
        for (int i = 0; i < STORES; i++) {
            producer.send(new ProducerRecord<>(inputTopic + "-" + i, (long) random.nextInt(2000000), random.nextLong()));
            messagesSent++;
        }
//                }
        final long stop = System.currentTimeMillis();
        final long took = stop - start;
        final long messagesPerSecond = messagesSent / TimeUnit.MILLISECONDS.toSeconds(took);
        System.out.println("Produced mps " + messagesPerSecond);
        try {
            producer.close();
        } catch (Exception e) {
            //
        }

    }


    private static class Waiter {

        private final int count;
        private final CountDownLatch latch;
        private final Set<Long> threadIds = new HashSet<>();

        Waiter(final int count, final CountDownLatch latch) {
            this.count = count;
            this.latch = latch;
        }

        synchronized void ready(final long threadId) {
            threadIds.add(threadId);
            if (threadIds.size() == count) {
                latch.countDown();
            }
        }
    }
}
