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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
import org.apache.kafka.test.MockPartitioner;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareOnlyThisForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class KafkaProducerTest {

    @Test
    public void testConstructorWithSerializers() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer()).close();
    }

    @Test(expected = ConfigException.class)
    public void testNoSerializerProvided() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        new KafkaProducer(producerProps);
    }

    @Test
    public void testConstructorFailureCloseResource() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar.local:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            fail("should have caught an exception and returned");
        } catch (KafkaException e) {
            assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            assertEquals("Failed to construct kafka producer", e.getMessage());
        }
    }

    @Test
    public void testSerializerClose() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
        final int oldInitCount = MockSerializer.INIT_COUNT.get();
        final int oldCloseCount = MockSerializer.CLOSE_COUNT.get();

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
                configs, new MockSerializer(), new MockSerializer());
        assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        assertEquals(oldCloseCount, MockSerializer.CLOSE_COUNT.get());

        producer.close();
        assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        assertEquals(oldCloseCount + 2, MockSerializer.CLOSE_COUNT.get());
    }

    @Test
    public void testInterceptorConstructClose() throws Exception {
        try {
            Properties props = new Properties();
            // test with client ID assigned by KafkaProducer
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName());
            props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something");

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                    props, new StringSerializer(), new StringSerializer());
            assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            assertEquals(0, MockProducerInterceptor.CLOSE_COUNT.get());

            // Cluster metadata will only be updated on calling onSend.
            Assert.assertNull(MockProducerInterceptor.CLUSTER_META.get());

            producer.close();
            assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            assertEquals(1, MockProducerInterceptor.CLOSE_COUNT.get());
        } finally {
            // cleanup since we are using mutable static variables in MockProducerInterceptor
            MockProducerInterceptor.resetCounters();
        }
    }

    @Test
    public void testPartitionerClose() throws Exception {
        try {
            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner.class.getName());

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                    props, new StringSerializer(), new StringSerializer());
            assertEquals(1, MockPartitioner.INIT_COUNT.get());
            assertEquals(0, MockPartitioner.CLOSE_COUNT.get());

            producer.close();
            assertEquals(1, MockPartitioner.INIT_COUNT.get());
            assertEquals(1, MockPartitioner.CLOSE_COUNT.get());
        } finally {
            // cleanup since we are using mutable static variables in MockPartitioner
            MockPartitioner.resetCounters();
        }
    }

    @Test
    public void testOsDefaultSocketBufferSizes() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()).close();
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, -2);
        new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, -2);
        new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @PrepareOnlyThisForTest(Metadata.class)
    @Test
    public void testMetadataFetch() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        Metadata metadata = PowerMock.createNiceMock(Metadata.class);
        MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

        String topic = "topic";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");
        Collection<Node> nodes = Collections.singletonList(new Node(0, "host1", 1000));
        final Cluster emptyCluster = new Cluster(null, nodes,
                Collections.<PartitionInfo>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());
        final Cluster cluster = new Cluster(
                "dummy",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());

        // Expect exactly one fetch for each attempt to refresh while topic metadata is not available
        final int refreshAttempts = 5;
        EasyMock.expect(metadata.fetch()).andReturn(emptyCluster).times(refreshAttempts - 1);
        EasyMock.expect(metadata.fetch()).andReturn(cluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(record);
        PowerMock.verify(metadata);

        // Expect exactly one fetch if topic metadata is available
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(cluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(record, null);
        PowerMock.verify(metadata);

        // Expect exactly one fetch if topic metadata is available
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(cluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.partitionsFor(topic);
        PowerMock.verify(metadata);
    }

    @PrepareOnlyThisForTest(Metadata.class)
    @Test
    public void testMetadataFetchOnStaleMetadata() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        Metadata metadata = PowerMock.createNiceMock(Metadata.class);
        MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

        String topic = "topic";
        ProducerRecord<String, String> initialRecord = new ProducerRecord<>(topic, "value");
        // Create a record with a partition higher than the initial (outdated) partition range
        ProducerRecord<String, String> extendedRecord = new ProducerRecord<>(topic, 2, null, "value");
        Collection<Node> nodes = Collections.singletonList(new Node(0, "host1", 1000));
        final Cluster emptyCluster = new Cluster(null, nodes,
                Collections.<PartitionInfo>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());
        final Cluster initialCluster = new Cluster(
                "dummy",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());
        final Cluster extendedCluster = new Cluster(
                "dummy",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(
                        new PartitionInfo(topic, 0, null, null, null),
                        new PartitionInfo(topic, 1, null, null, null),
                        new PartitionInfo(topic, 2, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());

        // Expect exactly one fetch for each attempt to refresh while topic metadata is not available
        final int refreshAttempts = 5;
        EasyMock.expect(metadata.fetch()).andReturn(emptyCluster).times(refreshAttempts - 1);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(initialRecord);
        PowerMock.verify(metadata);

        // Expect exactly one fetch if topic metadata is available and records are still within range
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(initialRecord, null);
        PowerMock.verify(metadata);

        // Expect exactly two fetches if topic metadata is available but metadata response still returns
        // the same partition size (either because metadata are still stale at the broker too or because
        // there weren't any partitions added in the first place).
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        try {
            producer.send(extendedRecord, null);
            fail("Expected KafkaException to be raised");
        } catch (KafkaException e) {
            // expected
        }
        PowerMock.verify(metadata);

        // Expect exactly two fetches if topic metadata is available but outdated for the given record
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andReturn(extendedCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(extendedRecord, null);
        PowerMock.verify(metadata);
    }

    @Test
    public void testTopicRefreshInMetadata() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        long refreshBackoffMs = 500L;
        long metadataExpireMs = 60000L;
        final Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true,
                true, new ClusterResourceListeners());
        final Time time = new MockTime();
        MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);
        MemberModifier.field(KafkaProducer.class, "time").set(producer, time);
        final String topic = "topic";

        Thread t = new Thread() {
            @Override
            public void run() {
                long startTimeMs = System.currentTimeMillis();
                for (int i = 0; i < 10; i++) {
                    while (!metadata.updateRequested() && System.currentTimeMillis() - startTimeMs < 1000)
                        yield();
                    metadata.update(Cluster.empty(), Collections.singleton(topic), time.milliseconds());
                    time.sleep(60 * 1000L);
                }
            }
        };
        t.start();
        try {
            producer.partitionsFor(topic);
            fail("Expect TimeoutException");
        } catch (TimeoutException e) {
            // skip
        }
        Assert.assertTrue("Topic should still exist in metadata", metadata.containsTopic(topic));
    }

    @PrepareOnlyThisForTest(Metadata.class)
    @Test
    public void testHeaders() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        ExtendedSerializer keySerializer = PowerMock.createNiceMock(ExtendedSerializer.class);
        ExtendedSerializer valueSerializer = PowerMock.createNiceMock(ExtendedSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
        Metadata metadata = PowerMock.createNiceMock(Metadata.class);
        MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

        String topic = "topic";
        final Cluster cluster = new Cluster(
                "dummy",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());


        EasyMock.expect(metadata.fetch()).andReturn(cluster).anyTimes();

        PowerMock.replay(metadata);

        String value = "value";

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        EasyMock.expect(keySerializer.serialize(topic, record.headers(), null)).andReturn(null).once();
        EasyMock.expect(valueSerializer.serialize(topic, record.headers(), value)).andReturn(value.getBytes()).once();

        PowerMock.replay(keySerializer);
        PowerMock.replay(valueSerializer);


        //ensure headers can be mutated pre send.
        record.headers().add(new RecordHeader("test", "header2".getBytes()));

        producer.send(record, null);

        //ensure headers are closed and cannot be mutated post send
        try {
            record.headers().add(new RecordHeader("test", "test".getBytes()));
            fail("Expected IllegalStateException to be raised");
        } catch (IllegalStateException ise) {
            //expected
        }

        //ensure existing headers are not changed, and last header for key is still original value
        assertTrue(Arrays.equals(record.headers().lastHeader("test").value(), "header2".getBytes()));

        PowerMock.verify(valueSerializer);
        PowerMock.verify(keySerializer);

    }

    @Test
    public void closeShouldBeIdempotent() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        Producer producer = new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
        producer.close();
        producer.close();
    }

    @Test
    public void testMetricConfigRecordingLevel() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        try (KafkaProducer producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            assertEquals(Sensor.RecordingLevel.INFO, producer.metrics.config().recordLevel());
        }

        props.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        try (KafkaProducer producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            assertEquals(Sensor.RecordingLevel.DEBUG, producer.metrics.config().recordLevel());
        }
    }

    @PrepareOnlyThisForTest(Metadata.class)
    @Test
    public void testInterceptorPartitionSetOnTooLargeRecord() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1");
        String topic = "topic";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
                new StringSerializer());
        Metadata metadata = PowerMock.createNiceMock(Metadata.class);
        MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);
        final Cluster cluster = new Cluster(
            "dummy",
            Collections.singletonList(new Node(0, "host1", 1000)),
            Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
            Collections.<String>emptySet(),
            Collections.<String>emptySet());
        EasyMock.expect(metadata.fetch()).andReturn(cluster).once();

        // Mock interceptors field
        ProducerInterceptors interceptors = PowerMock.createMock(ProducerInterceptors.class);
        EasyMock.expect(interceptors.onSend(record)).andReturn(record);
        interceptors.onSendError(EasyMock.eq(record), EasyMock.<TopicPartition>notNull(), EasyMock.<Exception>notNull());
        EasyMock.expectLastCall();
        MemberModifier.field(KafkaProducer.class, "interceptors").set(producer, interceptors);

        PowerMock.replay(metadata);
        EasyMock.replay(interceptors);
        producer.send(record);

        EasyMock.verify(interceptors);
    }

    @Test
    public void testPartitionsForWithNullTopic() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            producer.partitionsFor(null);
            fail("Expected NullPointerException to be raised");
        } catch (NullPointerException e) {
            // expected
        }
    }
}
