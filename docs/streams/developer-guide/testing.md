---
title: Testing a Streams Application
description: Guide to testing Kafka Streams applications.
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
    assertEquals(KeyValue.pair("a", 42L), outputTopic.readKeyValue());

`TopologyTestDriver` supports punctuations, too. Event-time punctuations are triggered automatically based on the processed records' timestamps. Wall-clock-time punctuations can also be triggered by advancing the test driver's wall-clock-time (the driver mocks wall-clock-time internally to give users control over it). 
    
    
    testDriver.advanceWallClockTime(Duration.ofSeconds(20));

Additionally, you can access state stores via the test driver before or after a test. Accessing stores before a test is useful to pre-populate a store with some initial values. After data was processed, expected updates to the store can be verified. 
    
    
    KeyValueStore store = testDriver.getKeyValueStore("store-name");

Note, that you should always close the test driver at the end to make sure all resources are released properly. 
    
    
    testDriver.close();

## Example

The following example demonstrates how to use the test driver and helper classes. The example creates a topology that computes the maximum value per key using a key-value-store. While processing, no output is generated, but only the store is updated. Output is only sent downstream based on event-time and wall-clock punctuations. 
    
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private KeyValueStore<String, Long> store;

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Long> longSerde = Serdes.Long();

    @BeforeEach
    public void setup() {

        var topology = new Topology()
                .addSource("sourceProcessor", "input-topic")
                .addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor")
                .addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("aggStore"),
                                stringSerde,
                                longSerde
                        ),
                        "aggregator")
                .addSink("sinkProcessor", "result-topic", "aggregator");

        // setup test driver
        var props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, longSerde.getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

        // pre-populate store
        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }
    
    @Test
    public void shouldFlushStoreForFirstInput() {
        inputTopic.pipeInput("a", 1L);
        assertEquals(KeyValue.pair("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        inputTopic.pipeInput("a", 1L);
        assertEquals(21L, store.get("a"));
        assertEquals(KeyValue.pair("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldUpdateStoreForLargerValue() {
        inputTopic.pipeInput("a", 42L);
        assertEquals(42L, store.get("a"));
        assertEquals(KeyValue.pair("a", 42L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldUpdateStoreForNewKey() {
        inputTopic.pipeInput("b", 21L);
        assertEquals(21L, store.get("b"));
        assertEquals(KeyValue.pair("a", 21L), outputTopic.readKeyValue());
        assertEquals(KeyValue.pair("b", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldPunctuateIfStreamTimeAdvances() {
        var recordTime = Instant.now();
        inputTopic.pipeInput("a", 1L, recordTime);
        assertEquals(KeyValue.pair("a", 21L), outputTopic.readKeyValue());

        inputTopic.pipeInput("a", 1L, recordTime);
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10));
        assertEquals(KeyValue.pair("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        assertEquals(KeyValue.pair("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }
    
    static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long, String, Long> {
        @Override
        public Processor<String, Long, String, Long> get() {
            return new CustomMaxAggregator();
        }
    }

    static class CustomMaxAggregator extends ContextualProcessor<String, Long, String, Long> {

        private KeyValueStore<String, Long> store;

        @Override
        public void init(ProcessorContext<String, Long> context) {
            super.init(context);
            context.schedule(Duration.ofSeconds(60), WALL_CLOCK_TIME, this::flushStore);
            context.schedule(Duration.ofSeconds(10), STREAM_TIME, this::flushStore);
            store = context.getStateStore("aggStore");
        }

        @Override
        public void process(Record<String, Long> record) {
            var oldValue = store.get(record.key());
            if (oldValue == null || record.value() > oldValue) {
                store.put(record.key(), record.value());
            }
        }

        private void flushStore(long timestamp) {
            try (var it = store.all()) {
                while (it.hasNext()) {
                    var next = it.next();
                    context().forward(new Record<>(next.key, next.value, timestamp));
                }
            }
        }

    }

# Unit Testing Processors

If you [write a Processor](../processor-api), you will want to test it. 

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
    assertEquals(new Record<>(..., ...), forwarded.next().record());
    assertFalse(forwarded.hasNext());
    
    // you can reset forwards to clear the captured data. This may be helpful in constructing longer scenarios.
    context.resetForwards();
    
    assertEquals(0, context.forwarded().size());

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
    
    final KeyValueStore<String, Integer> store = Stores
            .keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("myStore"),
                    Serdes.String(),
                    Serdes.Integer())
            .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
            .build();
    
    context = new MockProcessorContext<>();
    store.init(context.getStateStoreContext(), store);
    context.addStateStore(store);

**Verifying punctuators**

Processors can schedule punctuators to handle periodic tasks. The mock context does _not_ automatically execute punctuators, but it does capture them to allow you to unit test them as well: 
    
    
    final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
    final Duration interval = capturedPunctuator.getInterval();
    final PunctuationType type = capturedPunctuator.getType();
    final boolean cancelled = capturedPunctuator.cancelled();
    final Punctuator punctuator = capturedPunctuator.getPunctuator();

    punctuator.punctuate(/*timestamp*/ 0L);

If you need to write tests involving automatic firing of scheduled punctuators, we recommend creating a simple topology with your processor and using the [`TopologyTestDriver`](.#testing-topologytestdriver). 

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


