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

package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class KafkaStreamsTest {

    @Test
    public void testStartAndClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testStartAndClose");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();

        KStreamBuilder builder = new KStreamBuilder();
        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.start();
        final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int initCountDifference = newInitCount - oldInitCount;
        Assert.assertTrue("some reporters should be initialized by calling start()", initCountDifference > 0);

        streams.close();
        Assert.assertEquals("each reporter initialized should also be closed",
                oldCloseCount + initCountDifference, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCloseIsIdempotent");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        KStreamBuilder builder = new KStreamBuilder();
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        streams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
                closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void testCannotStartOnceClosed() throws Exception {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotStartOnceClosed");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KStreamBuilder builder = new KStreamBuilder();
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.close();

        try {
            streams.start();
        } catch (IllegalStateException e) {
            Assert.assertEquals("Cannot restart after closing.", e.getMessage());
            return;
        }
        Assert.fail("should have caught an exception and returned");
    }

    @Test
    public void testCannotStartTwice() throws Exception {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotStartTwice");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KStreamBuilder builder = new KStreamBuilder();
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        try {
            streams.start();
        } catch (IllegalStateException e) {
            Assert.assertEquals("This process was already started.", e.getMessage());
            return;
        }
        Assert.fail("should have caught an exception and returned");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWhenNotRunning() throws Exception {
        KafkaStreams streams = createKafkaStreams();
        streams.getAllTasks();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() throws Exception {
        KafkaStreams streams = createKafkaStreams();
        streams.getAllTasksWithStore("store");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning() throws Exception {
        KafkaStreams streams = createKafkaStreams();
        streams.getTaskWithKey("store", "key", Serdes.String().serializer());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning() throws Exception {
        KafkaStreams streams = createKafkaStreams();
        streams.getTaskWithKey("store", "key", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String key, final Object value, final int numPartitions) {
                return 0;
            }
        });
    }


    private KafkaStreams createKafkaStreams() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KStreamBuilder builder = new KStreamBuilder();
        return new KafkaStreams(builder, props);
    }
}
