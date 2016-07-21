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

import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class KafkaStreamsTest {

    @Test
    public void testStartAndClose() throws Exception {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testStartAndClose");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

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
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCloseIsIdempotent");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        streams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotStartOnceClosed() throws Exception {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotStartOnceClosed");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.close();

        try {
            streams.start();
        } catch (final IllegalStateException e) {
            Assert.assertEquals("Cannot restart after closing.", e.getMessage());
            throw e;
        } finally {
            streams.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotStartTwice() throws Exception {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotStartTwice");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        try {
            streams.start();
        } catch (final IllegalStateException e) {
            Assert.assertEquals("This process was already started.", e.getMessage());
            throw e;
        } finally {
            streams.close();
        }
    }

    @Test
    public void testCleanup() throws Exception {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotStartTwice");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

        streams.cleanUp();
        streams.start();
        streams.close();
        streams.cleanUp();
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotCleanupWhileRunning() throws Exception {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotCleanupWhileRunning");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

        streams.start();
        try {
            streams.cleanUp();
        } catch (final IllegalStateException e) {
            Assert.assertEquals("Cannot clean up while running.", e.getMessage());
            throw e;
        } finally {
            streams.close();
        }
    }

}
