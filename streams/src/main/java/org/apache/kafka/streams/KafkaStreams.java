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

package org.apache.kafka.streams;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka Streams allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * The computational logic can be specified either by using the {@link TopologyBuilder} class to define the a DAG topology of
 * {@link org.apache.kafka.streams.processor.Processor}s or by using the {@link org.apache.kafka.streams.kstream.KStreamBuilder}
 * class which provides the high-level {@link org.apache.kafka.streams.kstream.KStream} DSL to define the transformation.
 *
 * The {@link KafkaStreams} class manages the lifecycle of a Kafka Streams instance. One stream instance can contain one or
 * more threads specified in the configs for the processing work.
 * <p>
 * A {@link KafkaStreams} instance can co-ordinate with any other instances with the same application ID (whether in this same process, on other processes
 * on this machine, or on remote machines) as a single (possibly distributed) stream processing client. These instances will divide up the work
 * based on the assignment of the input topic partitions so that all partitions are being
 * consumed. If instances are added or failed, all instances will rebalance the partition assignment among themselves
 * to balance processing load.
 * <p>
 * Internally the {@link KafkaStreams} instance contains a normal {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}
 * and {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer} instance that is used for reading input and writing output.
 * <p>
 *
 * A simple example might look like this:
 * <pre>
 *    Map&lt;String, Object&gt; props = new HashMap&lt;&gt;();
 *    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
 *    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *    props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
 *    props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
 *    props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
 *    props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
 *    StreamsConfig config = new StreamsConfig(props);
 *
 *    KStreamBuilder builder = new KStreamBuilder();
 *    builder.from("my-input-topic").mapValue(value -&gt; value.length().toString()).to("my-output-topic");
 *
 *    KafkaStreams streams = new KafkaStreams(builder, config);
 *    streams.start();
 * </pre>
 *
 */

@InterfaceStability.Unstable
public class KafkaStreams {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreams.class);
    private static final AtomicInteger STREAM_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.streams";

    // container states
    private static final int CREATED = 0;
    private static final int RUNNING = 1;
    private static final int STOPPED = 2;
    private int state = CREATED;

    private final StreamThread[] threads;

    // processId is expected to be unique across JVMs and to be used
    // in userData of the subscription request to allow assignor be aware
    // of the co-location of stream thread's consumers. It is for internal
    // usage only and should not be exposed to users at all.
    private final UUID processId;

    /**
     * Construct the stream instance.
     *
     * @param builder  the processor topology builder specifying the computational logic
     * @param props    properties for the {@link StreamsConfig}
     */
    public KafkaStreams(TopologyBuilder builder, Properties props) {
        this(builder, new StreamsConfig(props));
    }

    /**
     * Construct the stream instance.
     *
     * @param builder  the processor topology builder specifying the computational logic
     * @param config   the stream configs
     */
    public KafkaStreams(TopologyBuilder builder, StreamsConfig config) {
        // create the metrics
        Time time = new SystemTime();

        this.processId = UUID.randomUUID();

        // The application ID is a required config and hence should always have value
        String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);

        String clientId = config.getString(StreamsConfig.CLIENT_ID_CONFIG);
        if (clientId.length() <= 0)
            clientId = applicationId + "-" + STREAM_CLIENT_ID_SEQUENCE.getAndIncrement();

        List<MetricsReporter> reporters = config.getConfiguredInstances(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));

        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                TimeUnit.MILLISECONDS);

        Metrics metrics = new Metrics(metricConfig, reporters, time);

        this.threads = new StreamThread[config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG)];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new StreamThread(builder, config, applicationId, clientId, processId, metrics, time);
        }
    }

    /**
     * Start the stream instance by starting all its threads.
     * @throws IllegalStateException if process was already started
     */
    public synchronized void start() {
        log.debug("Starting Kafka Stream process");

        if (state == CREATED) {
            for (StreamThread thread : threads)
                thread.start();

            state = RUNNING;

            log.info("Started Kafka Stream process");
        } else {
            throw new IllegalStateException("This process was already started.");
        }
    }

    /**
     * Shutdown this stream instance by signaling all the threads to stop,
     * and then wait for them to join.
     * @throws IllegalStateException if process has not started yet
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

            state = STOPPED;

            log.info("Stopped Kafka Stream process");
        } else {
            throw new IllegalStateException("This process has not started yet.");
        }
    }

    /**
     * Sets the handler invoked when a stream thread abruptly terminates due to an uncaught exception.
     *
     * @param eh the object to use as this thread's uncaught exception handler. If null then this thread has no explicit handler.
     */
    public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler eh) {
        for (StreamThread thread : threads)
            thread.setUncaughtExceptionHandler(eh);
    }

}
