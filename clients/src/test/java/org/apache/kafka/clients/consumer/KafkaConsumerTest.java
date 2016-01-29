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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTest {

    private final String topic = "test";
    private final TopicPartition tp0 = new TopicPartition("test", 0);

    @Test
    public void testConstructorClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try {
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
                    props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        } catch (KafkaException e) {
            Assert.assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            Assert.assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            Assert.assertEquals("Failed to construct kafka consumer", e.getMessage());
            return;
        }
        Assert.fail("should have caught an exception and returned");
    }

    @Test
    public void testSubscription() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testSubscription");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            props, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        consumer.subscribe(Collections.singletonList(topic));
        Assert.assertEquals(Collections.singleton(topic), consumer.subscription());
        Assert.assertTrue(consumer.assignment().isEmpty());

        consumer.subscribe(Collections.<String>emptyList());
        Assert.assertTrue(consumer.subscription().isEmpty());
        Assert.assertTrue(consumer.assignment().isEmpty());

        consumer.assign(Collections.singletonList(tp0));
        Assert.assertTrue(consumer.subscription().isEmpty());
        Assert.assertEquals(Collections.singleton(tp0), consumer.assignment());

        consumer.unsubscribe();
        Assert.assertTrue(consumer.subscription().isEmpty());
        Assert.assertTrue(consumer.assignment().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeekNegative() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testSeekNegative");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
                props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.assign(Arrays.asList(new TopicPartition("nonExistTopic", 0)));
        consumer.seek(new TopicPartition("nonExistTopic", 0), -1);
    }
}
