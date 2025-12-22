---
title: Testing a Streams Application
description: 
weight: 7
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


# Testing Kafka Streams




# Importing the test utilities

To test a Kafka Streams application, Kafka provides a test-utils artifact that can be added as regular dependency to your test code base. Example `pom.xml` snippet when using Maven: 
    
    
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams-test-utils</artifactId>
        <version>4.3.0</version>
        <scope>test</scope>
    </dependency>

# Testing a Streams application

The test-utils package provides a `TopologyTestDriver` that can be used pipe data through a `Topology` that is either assembled manually using Processor API or via the DSL using `StreamsBuilder`. The test driver simulates the library runtime that continuously fetches records from input topics and processes them by traversing the topology. You can use the test driver to verify that your specified processor topology computes the correct result with the manually piped in data records. The test driver captures the results records and allows to query its embedded state stores. 
    
    
    // Processor API
    Topology topology = new Topology();
    topology.addSource("sourceProcessor", "input-topic");
    topology.addProcessor("processor", ..., "sourceProcessor");
    topology.addSink("sinkProcessor", "output-topic", "processor");
    // or
    // using DSL
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("input-topic").filter(...).to("output-topic");
    Topology topology = builder.build();
    
    // create test driver
    TopologyTestDriver testDriver = new TopologyTestDriver(topology);

With the test driver you can create `TestInputTopic` giving topic name and the corresponding serializers. `TestInputTopic` provides various methods to pipe new message values, keys and values, or list of KeyValue objects. 
    
    
    TestInputTopic<String, Long> inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
    inputTopic.pipeInput("key", 42L);

To verify the output, you can use `TestOutputTopic` where you configure the topic and the corresponding deserializers during initialization. It offers helper methods to read only certain parts of the result records or the collection of records. For example, you can validate returned `KeyValue` with standard assertions if you only care about the key and value, but not the timestamp of the result record. 
    
    
    TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("output-topic", stringSerde.deserializer(), longSerde.deserializer());
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("key", 42L)));

`TopologyTestDriver` supports punctuations, too. Event-time punctuations are triggered automatically based on the processed records' timestamps. Wall-clock-time punctuations can also be triggered by advancing the test driver's wall-clock-time (the driver mocks wall-clock-time internally to give users control over it). 
    
    
    testDriver.advanceWallClockTime(Duration.ofSeconds(20));

Additionally, you can access state stores via the test driver before or after a test. Accessing stores before a test is useful to pre-populate a store with some initial values. After data was processed, expected updates to the store can be verified. 
    
    
    KeyValueStore store = testDriver.getKeyValueStore("store-name");

Note, that you should always close the test driver at the end to make sure all resources are release properly. 
    
    
    testDriver.close();

## Example

The following example demonstrates how to use the test driver and helper classes. The example creates a topology that computes the maximum value per key using a key-value-store. While processing, no output is generated, but only the store is updated. Output is only sent downstream based on event-time and wall-clock punctuations. 
    
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private KeyValueStore<String, Long> store;
    
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();
    
    @Before
    public void setup() {
        Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("aggStore"),
                Serdes.String(),
                Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
            "aggregator");
        topology.addSink("sinkProcessor", "result-topic", "aggregator");
    
        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);
    
        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());
    
        // pre-populate store
        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }
    
    @After
    public void tearDown() {
        testDriver.close();
    }
    
    @Test
    public void shouldFlushStoreForFirstInput() {
        inputTopic.pipeInput("a", 1L);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
    
    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        inputTopic.pipeInput("a", 1L);
        assertThat(store.get("a"), equalTo(21L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
    
    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        inputTopic.pipeInput("a", 42L);
        assertThat(store.get("a"), equalTo(42L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 42L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
    
    @Test
    public void shouldUpdateStoreForNewKey() {
        inputTopic.pipeInput("b", 21L);
        assertThat(store.get("b"), equalTo(21L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("b", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
    
    @Test
    public void shouldPunctuateIfEvenTimeAdvances() {
        final Instant recordTime = Instant.now();
        inputTopic.pipeInput("a", 1L,  recordTime);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
    
        inputTopic.pipeInput("a", 1L,  recordTime);
        assertThat(outputTopic.isEmpty(), is(true));
    
        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
    
    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
    
    public class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long> {
        @Override
        public Processor<String, Long> get() {
            return new CustomMaxAggregator();
        }
    }
    
    public class CustomMaxAggregator implements Processor<String, Long> {
        ProcessorContext context;
        private KeyValueStore<String, Long> store;
    
        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
            context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore());
            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
        }
    
        @Override
        public void process(String key, Long value) {
            Long oldValue = store.get(key);
            if (oldValue == null || value > oldValue) {
                store.put(key, value);
            }
        }
    
        private void flushStore() {
            KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                KeyValue<String, Long> next = it.next();
                context.forward(next.key, next.value);
            }
        }
    
        @Override
        public void close() {}
    }

# Unit Testing Processors

If you [write a Processor](processor-api.html), you will want to test it. 

Because the `Processor` forwards its results to the context rather than returning them, Unit testing requires a mocked context capable of capturing forwarded data for inspection. For this reason, we provide a `MockProcessorContext` in `test-utils`. 

**Construction**

To begin with, instantiate your processor and initialize it with the mock context: 
    
    
    final Processor processorUnderTest = ...;
    final MockProcessorContext<String, Long> context = new MockProcessorContext<>();
    processorUnderTest.init(context);

If you need to pass configuration to your processor or set the default serdes, you can create the mock with config: 
    
    
    final Properties props = new Properties();
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    props.put("some.other.config", "some config value");
    final MockProcessorContext<String, Long> context = new MockProcessorContext<>(props);

**Captured data**

The mock will capture any values that your processor forwards. You can make assertions on them: 
    
    
    processorUnderTest.process("key", "value");
    
    final Iterator<CapturedForward<? extends String, ? extends Long>> forwarded = context.forwarded().iterator();
    assertEquals(forwarded.next().record(), new Record<>(..., ...));
    assertFalse(forwarded.hasNext());
    
    // you can reset forwards to clear the captured data. This may be helpful in constructing longer scenarios.
    context.resetForwards();
    
    assertEquals(context.forwarded().size(), 0);

If your processor forwards to specific child processors, you can query the context for captured data by child name: 
    
    
    final List<CapturedForward<? extends String, ? extends Long>> captures = context.forwarded("childProcessorName");

The mock also captures whether your processor has called `commit()` on the context: 
    
    
    assertTrue(context.committed());
    
    // commit captures can also be reset.
    context.resetCommit();
    
    assertFalse(context.committed());

**Setting record metadata**

In case your processor logic depends on the record metadata (topic, partition, offset), you can set them on the context: 
    
    
    context.setRecordMetadata("topicName", /*partition*/ 0, /*offset*/ 0L);

Once these are set, the context will continue returning the same values, until you set new ones. 

**State stores**

In case your punctuator is stateful, the mock context allows you to register state stores. You're encouraged to use a simple in-memory store of the appropriate type (KeyValue, Windowed, or Session), since the mock context does _not_ manage changelogs, state directories, etc. 
    
    
    final KeyValueStore<String, Integer> store =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("myStore"),
                Serdes.String(),
                Serdes.Integer()
            )
            .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
            .build();
    store.init(context, store);
    context.register(store, /*deprecated parameter*/ false, /*parameter unused in mock*/ null);

**Verifying punctuators**

Processors can schedule punctuators to handle periodic tasks. The mock context does _not_ automatically execute punctuators, but it does capture them to allow you to unit test them as well: 
    
    
    final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
    final long interval = capturedPunctuator.getIntervalMs();
    final PunctuationType type = capturedPunctuator.getType();
    final boolean cancelled = capturedPunctuator.cancelled();
    final Punctuator punctuator = capturedPunctuator.getPunctuator();
    punctuator.punctuate(/*timestamp*/ 0L);

If you need to write tests involving automatic firing of scheduled punctuators, we recommend creating a simple topology with your processor and using the [`TopologyTestDriver`](testing.html#testing-topologytestdriver). 

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


