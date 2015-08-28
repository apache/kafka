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

package org.apache.kafka.streaming;

import org.apache.kafka.streaming.processor.TopologyBuilder;
import org.apache.kafka.streaming.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streaming allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * This processing is defined by extending the {@link TopologyBuilder} abstract class to specify the transformation operator build. The
 * {@link KafkaStreaming} instance will be responsible for the lifecycle of these processors. It will instantiate and
 * start one or more of these processors to process the Kafka partitions assigned to this particular instance.
 * <p>
 * This streaming instance will co-ordinate with any other instances (whether in this same process, on other processes
 * on this machine, or on remote machines). These processes will divide up the work so that all partitions are being
 * consumed. If instances are added or die, the corresponding {@link StreamThread} instances will be shutdown or
 * started in the appropriate processes to balance processing load.
 * <p>
 * Internally the {@link KafkaStreaming} instance contains a normal {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}
 * and {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer} instance that is used for reading input and writing output.
 * <p>
 * A simple example might look like this:
 * <pre>
 *    Properties props = new Properties();
 *    props.put("bootstrap.servers", "localhost:4242");
 *    properties config = new properties(props);
 *    config.processor(ExampleStreamProcessor.class);
 *    config.serialization(new StringSerializer(), new StringDeserializer());
 *    KafkaStreaming container = new KafkaStreaming(new MyKStreamTopology(), config);
 *    container.run();
 * </pre>
 *
 */
public class KafkaStreaming {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreaming.class);

    // Container States
    private static final int CREATED = 0;
    private static final int RUNNING = 1;
    private static final int STOPPED = 2;
    private int state = CREATED;

    private final StreamThread[] threads;

    public KafkaStreaming(TopologyBuilder builder, StreamingConfig config) throws Exception {
        if (config.getClass(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG) == null)
            throw new NullPointerException("timestamp extractor is missing");

        this.threads = new StreamThread[config.getInt(StreamingConfig.NUM_STREAM_THREADS_CONFIG)];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new StreamThread(builder, config);
        }
    }

    /**
     * Start the stream process by starting all its threads
     */
    public synchronized void start() {
        log.debug("Starting Kafka Stream process");

        if (state == CREATED) {
            for (StreamThread thread : threads)
                thread.start();
        } else {
            throw new IllegalStateException("This process was already started.");
        }

        state = RUNNING;

        log.info("Started Kafka Stream process");
    }

    /**
     * Shutdown this stream process by signaling the threads to stop,
     * wait for them to join and clean up the process instance.
     */
    public synchronized void close() {
        log.debug("Stopping Kafka Stream process");

        if (state == RUNNING) {
            // signal the threads to stop and wait
            for (StreamThread thread : threads)
                thread.close();

            for (StreamThread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        } else {
            throw new IllegalStateException("This process has not started yet.");
        }

        state = STOPPED;

        log.info("Stopped Kafka Stream process");
    }
}
