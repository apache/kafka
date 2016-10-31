/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
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

import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class KafkaProducerTest {

    @Test
    public void testConstructorFailureCloseResource() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try {
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
                    props, new ByteArraySerializer(), new ByteArraySerializer());
        } catch (KafkaException e) {
            Assert.assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            Assert.assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            Assert.assertEquals("Failed to construct kafka producer", e.getMessage());
            return;
        }
        fail("should have caught an exception and returned");
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
        Assert.assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        Assert.assertEquals(oldCloseCount, MockSerializer.CLOSE_COUNT.get());

        producer.close();
        Assert.assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        Assert.assertEquals(oldCloseCount + 2, MockSerializer.CLOSE_COUNT.get());
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
            Assert.assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            Assert.assertEquals(0, MockProducerInterceptor.CLOSE_COUNT.get());

            // Cluster metadata will only be updated on calling onSend.
            Assert.assertNull(MockProducerInterceptor.CLUSTER_META.get());

            producer.close();
            Assert.assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            Assert.assertEquals(1, MockProducerInterceptor.CLOSE_COUNT.get());
        } finally {
            // cleanup since we are using mutable static variables in MockProducerInterceptor
            MockProducerInterceptor.resetCounters();
        }
    }

    @Test
    public void testOsDefaultSocketBufferSizes() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                config, new ByteArraySerializer(), new ByteArraySerializer());
        producer.close();
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

}
