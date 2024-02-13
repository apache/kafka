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
import java.util.OptionalInt;
import java.util.OptionalLong;
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
        long timeoutMs = opts.timeoutMs() >= 0 ? opts.timeoutMs() : Long.MAX_VALUE;
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(opts.consumerProps(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        ConsumerWrapper consumerWrapper = opts.partitionArg().isPresent()
            ? new ConsumerWrapper(Optional.of(opts.topicArg()), opts.partitionArg(), OptionalLong.of(opts.offsetArg()), Optional.empty(), consumer, timeoutMs)
            : new ConsumerWrapper(Optional.of(opts.topicArg()), OptionalInt.empty(), OptionalLong.empty(), Optional.ofNullable(opts.includedTopicsArg()), consumer, timeoutMs);

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
        final Optional<String> topic;
        final OptionalInt partitionId;
        final OptionalLong offset;
        final Optional<String> includedTopics;
        final Consumer<byte[], byte[]> consumer;
        final long timeoutMs;
        final Time time = Time.SYSTEM;

        Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Collections.emptyIterator();

        public ConsumerWrapper(Optional<String> topic,
                               OptionalInt partitionId,
                               OptionalLong offset,
                               Optional<String> includedTopics,
                               Consumer<byte[], byte[]> consumer,
                               long timeoutMs) {
            this.topic = topic;
            this.partitionId = partitionId;
            this.offset = offset;
            this.includedTopics = includedTopics;
            this.consumer = consumer;
            this.timeoutMs = timeoutMs;

            if (topic.isPresent() && partitionId.isPresent() && offset.isPresent() && !includedTopics.isPresent()) {
                seek(topic.get(), partitionId.getAsInt(), offset.getAsLong());
            } else if (topic.isPresent() && partitionId.isPresent() && !offset.isPresent() && !includedTopics.isPresent()) {
                // default to latest if no offset is provided
                seek(topic.get(), partitionId.getAsInt(), ListOffsetsRequest.LATEST_TIMESTAMP);
            } else if (topic.isPresent() && !partitionId.isPresent() && !offset.isPresent() && !includedTopics.isPresent()) {
                consumer.subscribe(Collections.singletonList(topic.get()));
            } else if (!topic.isPresent() && !partitionId.isPresent() && !offset.isPresent() && includedTopics.isPresent()) {
                consumer.subscribe(Pattern.compile(includedTopics.get()));
            } else {
                throw new IllegalArgumentException("An invalid combination of arguments is provided. " +
                        "Exactly one of 'topic' or 'include' must be provided. " +
                        "If 'topic' is provided, an optional 'partition' may also be provided. " +
                        "If 'partition' is provided, an optional 'offset' may also be provided, otherwise, consumption starts from the end of the partition.");
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
