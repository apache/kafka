//package org.apache.kafka.clients.consumer.internals;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaShareConsumer;
//import org.apache.kafka.common.serialization.ByteArrayDeserializer;
//
//import java.io.FileWriter;
//import java.io.PrintWriter;
//import java.time.Duration;
//import java.time.Instant;
//import java.util.Collections;
//import java.util.Properties;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * Multi-consumer ShareConsumer test to measure e2e latency with multiple parallel consumers.
// */
//public class MultiConsumerShareTest {
//
//    // Configuration - easily adjustable
//    private static final int NUM_CONSUMERS = 6; // Configurable number of consumers
//    private static final boolean USE_SAME_GROUP = true; // true = load balancing, false = independent consumption
//
//    // Easy IntelliJ run configuration
//    static {
//        System.setProperty("java.awt.headless", "true"); // Prevents GUI issues in IntelliJ
//    }
//
//    public static void main(String[] args) throws Exception {
//
//        String bootstrapServers = "localhost:9092";
//        String topic = "test-topic-3";
//        String baseGroupId = "test-share-group";
//        String outputFile = System.getProperty("user.home") + "/dev/multi-consumer-perf-test-trunk.txt";
//
//        System.out.println("Starting Multi-Consumer ShareConsumer test...");
//        System.out.println("Bootstrap servers: " + bootstrapServers);
//        System.out.println("Topic: " + topic);
//        System.out.println("Base Group ID: " + baseGroupId);
//        System.out.println("Number of consumers: " + NUM_CONSUMERS);
//        System.out.println("Same group (load balancing): " + USE_SAME_GROUP);
//        System.out.println("Output file: " + outputFile);
//        System.out.println("----------------------------------------");
//
//        PrintWriter writer = null;
//        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);
//        AtomicBoolean shutdown = new AtomicBoolean(false);
//        CountDownLatch startLatch = new CountDownLatch(NUM_CONSUMERS);
//        AtomicLong totalRecords = new AtomicLong(0);
//
//        try {
//            writer = new PrintWriter(new FileWriter(outputFile, false), true); // false = overwrite
//            PrintWriter finalWriter = writer;
//
//            // Write header
//            String header = String.format("%s | Starting Multi-Consumer Test | Consumers: %d | Bootstrap: %s | Topic: %s | BaseGroupId: %s | SameGroup: %s",
//                    Instant.now(), NUM_CONSUMERS, bootstrapServers, topic, baseGroupId, USE_SAME_GROUP);
//            finalWriter.println(header);
//            System.out.println(header);
//
//            // Create and start consumer threads
//            for (int i = 0; i < NUM_CONSUMERS; i++) {
//                final int consumerId = i;
//                final String groupId = USE_SAME_GROUP ? baseGroupId : (baseGroupId + "-" + i);
//
//                executor.submit(() -> {
//                    runConsumer(consumerId, bootstrapServers, topic, groupId, finalWriter, startLatch, shutdown, totalRecords);
//                });
//            }
//
//            // Wait for all consumers to start
//            startLatch.await();
//            System.out.println("All consumers started. Press Ctrl+C to stop...");
//
//            // Add shutdown hook
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                System.out.println("\nShutdown signal received...");
//                shutdown.set(true);
//                executor.shutdown();
//                try {
//                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
//                        executor.shutdownNow();
//                    }
//                } catch (InterruptedException e) {
//                    executor.shutdownNow();
//                }
//            }));
//
//            // Keep main thread alive
//            while (!shutdown.get()) {
//                Thread.sleep(1000);
//
//                // Print periodic status
//                long currentRecords = totalRecords.get();
//                if (currentRecords > 0 && currentRecords % 1000 == 0) {
//                    System.out.printf("Status: %d total records processed across all consumers%n", currentRecords);
//                }
//            }
//
//        } catch (Exception e) {
//            System.err.println("Error: " + e.getMessage());
//            e.printStackTrace();
//        } finally {
//            shutdown.set(true);
//            if (executor != null) {
//                executor.shutdown();
//                try {
//                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
//                        executor.shutdownNow();
//                    }
//                } catch (InterruptedException e) {
//                    executor.shutdownNow();
//                }
//            }
//            if (writer != null) {
//                writer.close();
//                System.out.println("Output file closed: " + outputFile);
//            }
//        }
//    }
//
//    private static void runConsumer(int consumerId, String bootstrapServers, String topic, String groupId,
//                                  PrintWriter writer, CountDownLatch startLatch, AtomicBoolean shutdown, AtomicLong totalRecords) {
//
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
//
//        // Add consumer-specific client ID for better debugging
//        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + consumerId);
//
//        KafkaShareConsumer<byte[], byte[]> consumer = null;
//        long recordCount = 0;
//
//        try {
//            consumer = new KafkaShareConsumer<>(props);
//            consumer.subscribe(Collections.singletonList(topic));
//
//            String startMsg = String.format("%s | Consumer-%d started | GroupId: %s", Instant.now(), consumerId, groupId);
//            synchronized (writer) {
//                writer.println(startMsg);
//            }
//            System.out.println(startMsg);
//
//            // Signal that this consumer is ready
//            startLatch.countDown();
//
//            while (!shutdown.get()) {
//                long pollStartTime = System.currentTimeMillis();
//                long preNetworkTime = System.currentTimeMillis();
//
//                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
//
//                long pollEndTime = System.currentTimeMillis();
//                long pollDuration = pollEndTime - pollStartTime;
//                long networkLatency = pollEndTime - preNetworkTime;
//
//                if (records.isEmpty()) {
//                    // Log empty polls less frequently for multi-consumer
//                    if (recordCount > 0 && recordCount % 50 == 0) {
//                        String emptyPollMsg = String.format("%s | Consumer-%d | EMPTY POLL: duration=%dms, networkLatency=%dms, totalRecords=%d",
//                                Instant.now(), consumerId, pollDuration, networkLatency, recordCount);
//                        synchronized (writer) {
//                            writer.println(emptyPollMsg);
//                        }
//                    }
//                    continue;
//                }
//
//                for (ConsumerRecord<byte[], byte[]> record : records) {
//                    long consumeTime = System.currentTimeMillis();
//                    long recordTimestamp = record.timestamp();
//                    long e2eLatency = consumeTime - recordTimestamp;
//                    long pollToProcessDelay = consumeTime - pollEndTime;
//
//                    recordCount++;
//                    totalRecords.incrementAndGet();
//
//                    // Enhanced logging with consumer ID
//                    String logLine = String.format("%s | consumer=%d | group=%s | partition=%d | offset=%d | rec_ts=%d | consume_ts=%d | " +
//                            "e2e_latency=%dms | poll_duration=%dms | network_latency=%dms | poll_to_process=%dms | record_size=%d",
//                            Instant.now(),
//                            consumerId,
//                            groupId,
//                            record.partition(),
//                            record.offset(),
//                            recordTimestamp,
//                            consumeTime,
//                            e2eLatency,
//                            pollDuration,
//                            networkLatency,
//                            pollToProcessDelay,
//                            (record.serializedValueSize() != -1 ? record.serializedValueSize() : 0) +
//                            (record.serializedKeySize() != -1 ? record.serializedKeySize() : 0));
//
//                    // Synchronized writing to avoid interleaved output
//                    synchronized (writer) {
//                        writer.println(logLine);
//                        writer.flush();
//                    }
//
//                    // Print to console less frequently to avoid spam
//                    if (recordCount % 10 == 0) {
//                        System.out.println(logLine);
//                    }
//                }
//            }
//
//        } catch (Exception e) {
//            System.err.println("Consumer-" + consumerId + " error: " + e.getMessage());
//            e.printStackTrace();
//            synchronized (writer) {
//                writer.println(Instant.now() + " | Consumer-" + consumerId + " | ERROR: " + e.getMessage());
//            }
//        } finally {
//            try {
//                if (consumer != null) {
//                    consumer.close();
//                }
//                String closeMsg = String.format("%s | Consumer-%d closed | Records processed: %d", Instant.now(), consumerId, recordCount);
//                synchronized (writer) {
//                    writer.println(closeMsg);
//                }
//                System.out.println(closeMsg);
//            } catch (Exception e) {
//                System.err.println("Error closing consumer-" + consumerId + ": " + e.getMessage());
//            }
//        }
//    }
//}
