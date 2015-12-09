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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka Streaming allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * This processing is defined by using the {@link TopologyBuilder} class or its superclass KStreamBuilder to specify
 * the transformation.
 * The {@link KafkaStreaming} instance will be responsible for the lifecycle of these processors. It will instantiate and
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
 *    Map&lt;String, Object&gt; props = new HashMap&lt;&gt;();
 *    props.put("bootstrap.servers", "localhost:4242");
 *    props.put("key.deserializer", StringDeserializer.class);
 *    props.put("value.deserializer", StringDeserializer.class);
 *    props.put("key.serializer", StringSerializer.class);
 *    props.put("value.serializer", IntegerSerializer.class);
 *    props.put("timestamp.extractor", MyTimestampExtractor.class);
 *    StreamingConfig config = new StreamingConfig(props);
 *
 *    KStreamBuilder builder = new KStreamBuilder();
 *    builder.from("topic1").mapValue(value -&gt; value.length()).to("topic2");
 *
 *    KafkaStreaming streaming = new KafkaStreaming(builder, config);
 *    streaming.start();
 * </pre>
 *
 */
public class KafkaStreaming {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreaming.class);
    private static final AtomicInteger STREAMING_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
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

    public KafkaStreaming(TopologyBuilder builder, StreamingConfig config) throws Exception {
        // create the metrics
        Time time = new SystemTime();

        this.processId = UUID.randomUUID();

        String jobId = config.getString(StreamingConfig.JOB_ID_CONFIG);
        if (jobId.length() <= 0)
            jobId = "kafka-streams";

        String clientId = config.getString(StreamingConfig.CLIENT_ID_CONFIG);
        if (clientId.length() <= 0)
            clientId = jobId + "-" + STREAMING_CLIENT_ID_SEQUENCE.getAndIncrement();

        List<MetricsReporter> reporters = config.getConfiguredInstances(StreamingConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));

        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(StreamingConfig.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(StreamingConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                TimeUnit.MILLISECONDS);

        Metrics metrics = new Metrics(metricConfig, reporters, time);

        this.threads = new StreamThread[config.getInt(StreamingConfig.NUM_STREAM_THREADS_CONFIG)];
        for (int i = 0; i < this.threads.length; i++) {
            this.threads[i] = new StreamThread(builder, config, jobId, clientId, processId, metrics, time);
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

            state = RUNNING;

            log.info("Started Kafka Stream process");
        } else {
            throw new IllegalStateException("This process was already started.");
        }
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

            state = STOPPED;

            log.info("Stopped Kafka Stream process");
        } else {
            throw new IllegalStateException("This process has not started yet.");
        }
    }
}
