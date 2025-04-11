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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;


/**
 * Share Consumer that dumps messages to standard out.
 */
public class ConsoleShareConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleShareConsumer.class);
    private static final CountDownLatch SHUTDOWN_LATCH = new CountDownLatch(1);

    static int messageCount = 0;

    public static void main(String[] args) throws Exception {
        ConsoleShareConsumerOptions opts = new ConsoleShareConsumerOptions(args);
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

    public static void run(ConsoleShareConsumerOptions opts) {
        messageCount = 0;
        long timeoutMs = opts.timeoutMs() >= 0 ? opts.timeoutMs() : Long.MAX_VALUE;

        ShareConsumer<byte[], byte[]> consumer = new KafkaShareConsumer<>(opts.consumerProps(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        ConsumerWrapper consumerWrapper = new ConsumerWrapper(opts.topicArg(), consumer, timeoutMs);

        addShutdownHook(consumerWrapper);

        try {
            process(opts.maxMessages(), opts.formatter(), consumerWrapper, System.out, opts.rejectMessageOnError(), opts.acknowledgeType());
        } finally {
            consumerWrapper.cleanup();
            opts.formatter().close();
            reportRecordCount();

            SHUTDOWN_LATCH.countDown();
        }
    }

    private static void addShutdownHook(ConsumerWrapper consumer) {
        Exit.addShutdownHook("consumer-shutdown-hook", () -> {
            try {
                consumer.wakeup();
                SHUTDOWN_LATCH.await();
            } catch (Throwable t) {
                LOG.error("Exception while running shutdown hook: ", t);
            }
        });
    }

    static void process(int maxMessages, MessageFormatter formatter, ConsumerWrapper consumer, PrintStream output,
                        boolean rejectMessageOnError, AcknowledgeType acknowledgeType) {
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
                consumer.acknowledge(msg, acknowledgeType);
            } catch (Throwable t) {
                if (rejectMessageOnError) {
                    LOG.error("Error processing message, rejecting this message: ", t);
                    consumer.acknowledge(msg, AcknowledgeType.REJECT);
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

    private static void reportRecordCount() {
        System.err.println("Processed a total of " + messageCount + " messages");
    }

    private static boolean checkErr(PrintStream output) {
        boolean gotError = output.checkError();
        if (gotError) {
            // This means no one is listening to our output stream anymore, time to shut down
            System.err.println("Unable to write to standard out, closing consumer.");
        }
        return gotError;
    }

    public static class ConsumerWrapper {
        final String topic;
        final ShareConsumer<byte[], byte[]> consumer;
        final long timeoutMs;
        final Time time = Time.SYSTEM;

        Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Collections.emptyIterator();

        public ConsumerWrapper(String topic,
                               ShareConsumer<byte[], byte[]> consumer,
                               long timeoutMs) {
            this.topic = topic;
            this.consumer = consumer;
            this.timeoutMs = timeoutMs;

            consumer.subscribe(Collections.singletonList(topic));
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

        void acknowledge(ConsumerRecord<byte[], byte[]> record, AcknowledgeType acknowledgeType) {
            consumer.acknowledge(record, acknowledgeType);
        }

        void wakeup() {
            this.consumer.wakeup();
        }

        void cleanup() {
            this.consumer.close();
        }
    }
}
