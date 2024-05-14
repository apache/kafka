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
package org.apache.kafka.tools.consumer;

import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import java.util.Collections;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consumer that dumps messages to standard out.
 */
public class ConsoleConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleConsumer.class);
    private static final CountDownLatch SHUTDOWN_LATCH = new CountDownLatch(1);

    static int messageCount = 0;

    public static void main(String[] args) throws Exception {
        ConsoleConsumerOptions opts = new ConsoleConsumerOptions(args);
        try {
            run(opts);
        } catch (AuthenticationException ae) {
            LOG.error("Authentication failed: terminating consumer process", ae);
            Exit.exit(1);
        } catch (Throwable t) {
            LOG.error("Unknown error when running consumer: ", t);
            Exit.exit(1);
        }
    }

    public static void run(ConsoleConsumerOptions opts) {
        messageCount = 0;
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(opts.consumerProps(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        ConsumerWrapper consumerWrapper = new ConsumerWrapper(opts, consumer);

        addShutdownHook(consumerWrapper, opts);

        try {
            process(opts.maxMessages(), opts.formatter(), consumerWrapper, System.out, opts.skipMessageOnError());
        } finally {
            consumerWrapper.cleanup();
            opts.formatter().close();
            reportRecordCount();

            SHUTDOWN_LATCH.countDown();
        }
    }

    static void addShutdownHook(ConsumerWrapper consumer, ConsoleConsumerOptions conf) {
        Exit.addShutdownHook("consumer-shutdown-hook", () -> {
            try {
                consumer.wakeup();
                SHUTDOWN_LATCH.await();
            } catch (Throwable t) {
                LOG.error("Exception while running shutdown hook: ", t);
            }
            if (conf.enableSystestEventsLogging()) {
                System.out.println("shutdown_complete");
            }
        });
    }

    static void process(int maxMessages, MessageFormatter formatter, ConsumerWrapper consumer, PrintStream output, boolean skipMessageOnError) {
        while (messageCount < maxMessages || maxMessages == -1) {
            ConsumerRecord<byte[], byte[]> msg;
            try {
                msg = consumer.receive();
            } catch (WakeupException we) {
                LOG.trace("Caught WakeupException because consumer is shutdown, ignore and terminate.");
                // Consumer will be closed
                return;
            } catch (Throwable t) {
                LOG.error("Error processing message, terminating consumer process: ", t);
                // Consumer will be closed
                return;
            }
            messageCount += 1;
            try {
                formatter.writeTo(new ConsumerRecord<>(msg.topic(), msg.partition(), msg.offset(), msg.timestamp(), msg.timestampType(),
                    0, 0, msg.key(), msg.value(), msg.headers(), Optional.empty()), output);
            } catch (Throwable t) {
                if (skipMessageOnError) {
                    LOG.error("Error processing message, skipping this message: ", t);
                } else {
                    // Consumer will be closed
                    throw t;
                }
            }
            if (checkErr(output)) {
                // Consumer will be closed
                return;
            }
        }
    }

    static void reportRecordCount() {
        System.err.println("Processed a total of " + messageCount + " messages");
    }

    static boolean checkErr(PrintStream output) {
        boolean gotError = output.checkError();
        if (gotError) {
            // This means no one is listening to our output stream anymore, time to shut down
            System.err.println("Unable to write to standard out, closing consumer.");
        }
        return gotError;
    }

    public static class ConsumerWrapper {
        final Time time = Time.SYSTEM;
        final long timeoutMs;
        final Consumer<byte[], byte[]> consumer;

        Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Collections.emptyIterator();

        public ConsumerWrapper(ConsoleConsumerOptions opts, Consumer<byte[], byte[]> consumer) {
            this.consumer = consumer;
            timeoutMs = opts.timeoutMs();
            Optional<String> topic = opts.topicArg();

            if (topic.isPresent()) {
                if (opts.partitionArg().isPresent()) {
                    seek(topic.get(), opts.partitionArg().getAsInt(), opts.offsetArg());
                } else {
                    consumer.subscribe(Collections.singletonList(topic.get()));
                }
            } else {
                opts.includedTopicsArg().ifPresent(topics -> consumer.subscribe(Pattern.compile(topics)));
            }
        }

        private void seek(String topic, int partitionId, long offset) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionId);
            consumer.assign(Collections.singletonList(topicPartition));
            if (offset == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
            } else if (offset == ListOffsetsRequest.LATEST_TIMESTAMP) {
                consumer.seekToEnd(Collections.singletonList(topicPartition));
            } else {
                consumer.seek(topicPartition, offset);
            }
        }

        void resetUnconsumedOffsets() {
            Map<TopicPartition, Long> smallestUnconsumedOffsets = new HashMap<>();
            while (recordIter.hasNext()) {
                ConsumerRecord<byte[], byte[]> record = recordIter.next();
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                // avoid auto-committing offsets which haven't been consumed
                smallestUnconsumedOffsets.putIfAbsent(tp, record.offset());
            }
            smallestUnconsumedOffsets.forEach(consumer::seek);
        }

        ConsumerRecord<byte[], byte[]> receive() {
            long startTimeMs = time.milliseconds();
            while (!recordIter.hasNext()) {
                recordIter = consumer.poll(Duration.ofMillis(timeoutMs)).iterator();
                if (!recordIter.hasNext() && (time.milliseconds() - startTimeMs > timeoutMs)) {
                    throw new TimeoutException();
                }
            }
            return recordIter.next();
        }

        void wakeup() {
            this.consumer.wakeup();
        }

        void cleanup() {
            resetUnconsumedOffsets();
            this.consumer.close();
        }
    }
}
