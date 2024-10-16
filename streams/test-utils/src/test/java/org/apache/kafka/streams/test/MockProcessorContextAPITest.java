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
package org.apache.kafka.streams.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.MockFixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.api.TestFixedKeyRecordFactory;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class MockProcessorContextAPITest {

    public static Stream<Arguments> shouldCaptureOutputRecords() {
        return Stream.of(
                Arguments.of(
                        new Processor<String, Long, String, Long>() {
                            private ProcessorContext<String, Long> context;

                            @Override
                            public void init(final ProcessorContext<String, Long> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final Record<String, Long> record) {
                                final String key = record.key();
                                final Long value = record.value();
                                context.forward(record.withKey(key + value).withValue(key.length() + value));
                            }
                        },
                        new FixedKeyProcessor<String, Long, Long>() {
                            private FixedKeyProcessorContext<String, Long> context;

                            @Override
                            public void init(final FixedKeyProcessorContext<String, Long> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final FixedKeyRecord<String, Long> record) {
                                final String key = record.key();
                                final Long value = record.value();
                                context.forward(record.withValue(key.length() + value));
                            }
                        }
                )
        );
    }

    public static Stream<Arguments> shouldCaptureRecordsOutputToChildByName() {
        return Stream.of(
                Arguments.of(
                        new Processor<String, Long, String, Long>() {
                            private int count = 0;
                            private ProcessorContext<String, Long> context;

                            @Override
                            public void init(final ProcessorContext<String, Long> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final Record<String, Long> record) {
                                final String key = record.key();
                                final Long value = record.value();
                                if (count == 0) {
                                    context.forward(new Record<>("start", -1L, 0L)); // broadcast
                                }
                                final String toChild = count % 2 == 0 ? "george" : "pete";
                                context.forward(new Record<>(key + value,
                                        key.length() + value, 0L), toChild);
                                count++;
                            }
                        }, new FixedKeyProcessor<String, Long, Long>() {
                            private int count = 0;
                            private FixedKeyProcessorContext<String, Long> context;

                            @Override
                            public void init(final FixedKeyProcessorContext<String, Long> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final FixedKeyRecord<String, Long> record) {
                                final String key = record.key();
                                final Long value = record.value();
                                if (count == 0) {
                                    context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("start", -1L)); // broadcast
                                }
                                final String toChild = count % 2 == 0 ? "george" : "pete";
                                context.forward(
                                        TestFixedKeyRecordFactory.createFixedKeyRecord(key + value,
                                                key.length() + value), toChild);
                                count++;
                            }
                        }
                )
        );
    }

    public static Stream<Arguments> shouldCaptureCommitsAndAllowReset() {
        return Stream.of(
                Arguments.of(
                        new Processor<String, Long, Void, Void>() {
                            private ProcessorContext<Void, Void> context;
                            private int count = 0;

                            @Override
                            public void init(final ProcessorContext<Void, Void> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final Record<String, Long> record) {
                                if (++count > 2) {
                                    context.commit();
                                }
                            }
                        }, new FixedKeyProcessor<String, Long, Void>() {
                            private FixedKeyProcessorContext<String, Void> context;
                            private int count = 0;

                            @Override
                            public void init(final FixedKeyProcessorContext<String, Void> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final FixedKeyRecord<String, Long> record) {
                                if (++count > 2) {
                                    context.commit();
                                }
                            }
                        }
                )
        );
    }

    public static Stream<Arguments> shouldStoreAndReturnStateStores() {
        return Stream.of(
                Arguments.of(
                        new Processor<String, Long, Void, Void>() {
                            private ProcessorContext<Void, Void> context;

                            @Override
                            public void init(final ProcessorContext<Void, Void> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final Record<String, Long> record) {
                                final String key = record.key();
                                final Long value = record.value();
                                final KeyValueStore<String, Long> stateStore = context.getStateStore("my-state");

                                stateStore.put(key, (stateStore.get(key) == null ? 0 : stateStore.get(key)) + value);
                                stateStore.put("all", (stateStore.get("all") == null ? 0 : stateStore.get("all")) + value);
                            }
                        },
                        new FixedKeyProcessor<String, Long, Void>() {
                            private FixedKeyProcessorContext<String, Void> context;

                            @Override
                            public void init(final FixedKeyProcessorContext<String, Void> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final FixedKeyRecord<String, Long> record) {
                                final String key = record.key();
                                final Long value = record.value();
                                final KeyValueStore<String, Long> stateStore = context.getStateStore("my-state");

                                stateStore.put(key, (stateStore.get(key) == null ? 0 : stateStore.get(key)) + value);
                                stateStore.put("all", (stateStore.get("all") == null ? 0 : stateStore.get("all")) + value);

                            }
                        }
                )
        );
    }

    public static Stream<Arguments> shouldCaptureApplicationAndRecordMetadata() {
        return Stream.of(
                Arguments.of(
                        new Processor<String, Object, String, Object>() {
                            private ProcessorContext<String, Object> context;

                            @Override
                            public void init(final ProcessorContext<String, Object> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final Record<String, Object> record) {
                                context.forward(new Record<String, Object>("appId", context.applicationId(), 0L));
                                context.forward(new Record<String, Object>("taskId", context.taskId(), 0L));

                                if (context.recordMetadata().isPresent()) {
                                    final RecordMetadata recordMetadata = context.recordMetadata().get();
                                    context.forward(new Record<String, Object>("topic", recordMetadata.topic(), 0L));
                                    context.forward(new Record<String, Object>("partition", recordMetadata.partition(), 0L));
                                    context.forward(new Record<String, Object>("offset", recordMetadata.offset(), 0L));
                                }

                                context.forward(new Record<String, Object>("record", record, 0L));
                            }
                        }, new FixedKeyProcessor<String, Object, Object>() {
                            private FixedKeyProcessorContext<String, Object> context;

                            @Override
                            public void init(final FixedKeyProcessorContext<String, Object> context) {
                                this.context = context;
                            }

                            @Override
                            public void process(final FixedKeyRecord<String, Object> record) {
                                context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("appId", context.applicationId()));
                                context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("taskId", context.taskId()));

                                if (context.recordMetadata().isPresent()) {
                                    final RecordMetadata recordMetadata = context.recordMetadata().get();
                                    context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("topic", recordMetadata.topic()));
                                    context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("partition", recordMetadata.partition()));
                                    context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("offset", recordMetadata.offset()));
                                }

                                context.forward(TestFixedKeyRecordFactory.createFixedKeyRecord("record", record));
                            }
                        }
                )
        );
    }

    public static Stream<Arguments> shouldCapturePunctuator() {
        return Stream.of(
                Arguments.of(
                        new Processor<String, Long, Void, Void>() {

                            @Override
                            public void init(final ProcessorContext<Void, Void> context) {
                                context.schedule(
                                        Duration.ofSeconds(1L),
                                        PunctuationType.WALL_CLOCK_TIME,
                                        timestamp -> context.commit()
                                );
                            }

                            @Override
                            public void process(final Record<String, Long> record) {

                            }
                        }, new FixedKeyProcessor<String, Long, Void>() {

                            @Override
                            public void init(final FixedKeyProcessorContext<String, Void> context) {
                                context.schedule(
                                        Duration.ofSeconds(1L),
                                        PunctuationType.WALL_CLOCK_TIME,
                                        timestamp -> context.commit()
                                );
                            }

                            @Override
                            public void process(final FixedKeyRecord<String, Long> record) {

                            }
                        }
                )
        );
    }


    @ParameterizedTest
    @MethodSource
    public void shouldCaptureOutputRecords(final Processor<String, Long, String, Long> processor,
                                           final FixedKeyProcessor<String, Long, Long> fixedKeyProcessor) {

        final MockProcessorContext<String, Long> processorContext = new MockProcessorContext<>();
        final MockFixedKeyProcessorContext<String, Long> fixedKeyProcessorContext = new MockFixedKeyProcessorContext<>();

        processor.init(processorContext);
        fixedKeyProcessor.init(fixedKeyProcessorContext);

        final Record<String, Long> record1 = new Record<>("foo", 5L, 0L);
        final Record<String, Long> record2 = new Record<>("barbaz", 50L, 0L);

        processor.process(record1);
        processor.process(record2);

        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record1));
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record2));

        final List<CapturedForward<? extends String, ? extends Long>> forwardedFromProcessor = processorContext.forwarded();
        final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded();

        final List<CapturedForward<String, Long>> expectedFromProcessor = asList(
                new CapturedForward<>(new Record<>("foo5", 8L, 0L)),
                new CapturedForward<>(new Record<>("barbaz50", 56L, 0L))
        );

        final List<MockFixedKeyProcessorContext.CapturedForward<String, Long>> expectedFromFixedKeyProcessor = asList(
                new MockFixedKeyProcessorContext.CapturedForward<>(
                        TestFixedKeyRecordFactory.createFixedKeyRecord("foo", 8L, 0L)),
                new MockFixedKeyProcessorContext.CapturedForward<>(
                        TestFixedKeyRecordFactory.createFixedKeyRecord("barbaz", 56L, 0L))
        );

        assertThat(forwardedFromProcessor, is(expectedFromProcessor));
        assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));

        processorContext.resetForwards();
        fixedKeyProcessorContext.resetForwards();

        assertThat(processorContext.forwarded(), empty());
        assertThat(fixedKeyProcessorContext.forwarded(), empty());
    }


    @ParameterizedTest
    @MethodSource
    public void shouldCaptureRecordsOutputToChildByName(final Processor<String, Long, String, Long> processor,
                                                        final FixedKeyProcessor<String, Long, Long> fixedKeyProcessor) {

        final MockProcessorContext<String, Long> processorContext = new MockProcessorContext<>();
        final MockFixedKeyProcessorContext<String, Long> fixedKeyProcessorContext = new MockFixedKeyProcessorContext<>();

        processor.init(processorContext);
        fixedKeyProcessor.init(fixedKeyProcessorContext);

        final Record<String, Long> record1 = new Record<>("foo", 5L, 0L);
        final Record<String, Long> record2 = new Record<>("barbaz", 50L, 0L);

        processor.process(record1);
        processor.process(record2);

        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record1));
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record2));

        {
            final List<CapturedForward<? extends String, ? extends Long>> forwardedFromProcessor = processorContext.forwarded();
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded();
            final List<CapturedForward<? extends String, ? extends Long>> expected = asList(
                    new CapturedForward<>(new Record<>("start", -1L, 0L), Optional.empty()),
                    new CapturedForward<>(new Record<>("foo5", 8L, 0L), Optional.of("george")),
                    new CapturedForward<>(new Record<>("barbaz50", 56L, 0L), Optional.of("pete"))
            );

            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> expectedFromFixedKeyProcessor = asList(
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "start", -1L, 0L), Optional.empty()),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "foo5", 8L, 0L), Optional.of("george")),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "barbaz50", 56L, 0L), Optional.of("pete"))
            );

            assertThat(forwardedFromProcessor, is(expected));
            assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));
        }
        {
            final List<CapturedForward<? extends String, ? extends Long>> forwardedFromProcessor = processorContext.forwarded("george");
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded("george");
            final List<CapturedForward<? extends String, ? extends Long>> expected = asList(
                    new CapturedForward<>(new Record<>("start", -1L, 0L), Optional.empty()),
                    new CapturedForward<>(new Record<>("foo5", 8L, 0L), Optional.of("george"))
            );

            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> expectedFromFixedKeyProcessor = asList(
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "start", -1L, 0L), Optional.empty()),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "foo5", 8L, 0L), Optional.of("george"))
            );

            assertThat(forwardedFromProcessor, is(expected));
            assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));
        }
        {
            final List<CapturedForward<? extends String, ? extends Long>> forwardedFromProcessor = processorContext.forwarded("pete");
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded("pete");
            final List<CapturedForward<? extends String, ? extends Long>> expected = asList(
                    new CapturedForward<>(new Record<>("start", -1L, 0L), Optional.empty()),
                    new CapturedForward<>(new Record<>("barbaz50", 56L, 0L), Optional.of("pete"))
            );

            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> expectedFromFixedKeyProcessor = asList(
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "start", -1L, 0L), Optional.empty()),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "barbaz50", 56L, 0L), Optional.of("pete"))
            );


            assertThat(forwardedFromProcessor, is(expected));
            assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));
        }
        {
            final List<CapturedForward<? extends String, ? extends Long>> forwardedFromProcessor = processorContext.forwarded("steve");
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded("steve");
            final List<CapturedForward<? extends String, ? extends Long>> expected = singletonList(
                    new CapturedForward<>(new Record<>("start", -1L, 0L))
            );
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ? extends Long>> expectedFromFixedKeyProcessor = singletonList(
                    new MockFixedKeyProcessorContext.CapturedForward<>(
                            TestFixedKeyRecordFactory.createFixedKeyRecord("start", -1L, 0L))
            );

            assertThat(forwardedFromProcessor, is(expected));
            assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));
        }
    }

    @ParameterizedTest
    @MethodSource
    public void shouldCaptureCommitsAndAllowReset(final Processor<String, Long, Void, Void> processor,
                                                  final FixedKeyProcessor<String, Long, Void> fixedKeyProcessor) {

        final MockProcessorContext<Void, Void> processorContext = new MockProcessorContext<>();
        final MockFixedKeyProcessorContext<String, Void> fixedKeyProcessorContext = new MockFixedKeyProcessorContext<>();

        processor.init(processorContext);
        fixedKeyProcessor.init(fixedKeyProcessorContext);

        final Record<String, Long> record1 = new Record<>("foo", 5L, 0L);
        final Record<String, Long> record2 = new Record<>("barbaz", 50L, 0L);
        final Record<String, Long> record3 = new Record<>("foobar", 500L, 0L);

        processor.process(record1);
        processor.process(record2);

        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record1));
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record2));

        assertThat(processorContext.committed(), is(false));
        assertThat(fixedKeyProcessorContext.committed(), is(false));

        processor.process(record3);
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record3));

        assertThat(processorContext.committed(), is(true));
        assertThat(fixedKeyProcessorContext.committed(), is(true));

        processorContext.resetCommit();
        fixedKeyProcessorContext.resetCommit();

        assertThat(processorContext.committed(), is(false));
        assertThat(fixedKeyProcessorContext.committed(), is(false));
    }

    @ParameterizedTest
    @MethodSource
    public void shouldStoreAndReturnStateStores(final Processor<String, Long, Void, Void> processor,
                                                final FixedKeyProcessor<String, Long, Void> fixedKeyProcessor) {

        final MockProcessorContext<Void, Void> processorContext = new MockProcessorContext<>();
        final MockFixedKeyProcessorContext<String, Void> fixedKeyProcessorContext = new MockFixedKeyProcessorContext<>();

        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("my-state"),
                Serdes.String(),
                Serdes.Long()).withLoggingDisabled();

        final KeyValueStore<String, Long> processorStore = storeBuilder.build();
        final KeyValueStore<String, Long> fixedKeyProcessorStore = storeBuilder.build();

        processorStore.init(processorContext.getStateStoreContext(), processorStore);
        fixedKeyProcessorStore.init(fixedKeyProcessorContext.getStateStoreContext(), fixedKeyProcessorStore);

        processor.init(processorContext);
        fixedKeyProcessor.init(fixedKeyProcessorContext);

        final Record<String, Long> record1 = new Record<>("foo", 5L, 0L);
        final Record<String, Long> record2 = new Record<>("bar", 50L, 0L);

        processor.process(record1);
        processor.process(record2);

        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record1));
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record2));

        assertThat(processorStore.get("foo"), is(5L));
        assertThat(processorStore.get("bar"), is(50L));
        assertThat(processorStore.get("all"), is(55L));

        assertThat(fixedKeyProcessorStore.get("foo"), is(5L));
        assertThat(fixedKeyProcessorStore.get("bar"), is(50L));
        assertThat(fixedKeyProcessorStore.get("all"), is(55L));
    }


    @ParameterizedTest
    @MethodSource
    public void shouldCaptureApplicationAndRecordMetadata(final Processor<String, Object, String, Object> processor,
                                                          final FixedKeyProcessor<String, Object, Object> fixedKeyProcessor) {
        final Properties config = mkProperties(
                mkMap(
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "testMetadata"),
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
                )
        );

        final MockProcessorContext<String, Object> processorContext = new MockProcessorContext<>(config);
        final MockFixedKeyProcessorContext<String, Object> fixedKeyProcessorContext = new MockFixedKeyProcessorContext<>(config);

        processor.init(processorContext);
        fixedKeyProcessor.init(fixedKeyProcessorContext);

        final Record<String, Object> record1 = new Record<>("foo", 5L, 0L);

        processor.process(record1);
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record1));
        {
            final List<CapturedForward<? extends String, ?>> forwardedFromProcessor = processorContext.forwarded();
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ?>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded();
            final List<CapturedForward<? extends String, ?>> expectedFromProcessor = asList(
                    new CapturedForward<>(new Record<>("appId", "testMetadata", 0L)),
                    new CapturedForward<>(new Record<>("taskId", new TaskId(0, 0), 0L)),
                    new CapturedForward<>(new Record<>("record", new Record<>("foo", 5L, 0L), 0L))
            );

            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ?>> expectedFromFixedKeyProcessor = asList(
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "appId", "testMetadata", 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "taskId", new TaskId(0, 0), 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "record",
                            TestFixedKeyRecordFactory.createFixedKeyRecord(
                                    "foo", 5L, 0L), 0L))
            );
            assertThat(forwardedFromProcessor, is(expectedFromProcessor));
            assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));
        }
        processorContext.resetForwards();
        fixedKeyProcessorContext.resetForwards();

        processorContext.setRecordMetadata("t1", 0, 0L);
        fixedKeyProcessorContext.setRecordMetadata("t1", 0, 0L);

        processor.process(record1);
        fixedKeyProcessor.process(TestFixedKeyRecordFactory.createFixedKeyRecord(record1));
        {
            final List<CapturedForward<? extends String, ?>> forwardedFromProcessor = processorContext.forwarded();
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ?>> forwardedFromFixedKeyProcessor = fixedKeyProcessorContext.forwarded();

            final List<CapturedForward<? extends String, ?>> expectedFromProcessor = asList(
                    new CapturedForward<>(new Record<>("appId", "testMetadata", 0L)),
                    new CapturedForward<>(new Record<>("taskId", new TaskId(0, 0), 0L)),
                    new CapturedForward<>(new Record<>("topic", "t1", 0L)),
                    new CapturedForward<>(new Record<>("partition", 0, 0L)),
                    new CapturedForward<>(new Record<>("offset", 0L, 0L)),
                    new CapturedForward<>(new Record<>("record", new Record<>("foo", 5L, 0L), 0L))
            );
            final List<MockFixedKeyProcessorContext.CapturedForward<? extends String, ?>> expectedFromFixedKeyProcessor = asList(
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "appId", "testMetadata", 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "taskId", new TaskId(0, 0), 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "topic", "t1", 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "partition", 0, 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "offset", 0L, 0L)),
                    new MockFixedKeyProcessorContext.CapturedForward<>(TestFixedKeyRecordFactory.createFixedKeyRecord(
                            "record", TestFixedKeyRecordFactory.createFixedKeyRecord(
                                    "foo", 5L, 0L), 0L)));
            assertThat(forwardedFromProcessor, is(expectedFromProcessor));
            assertThat(forwardedFromFixedKeyProcessor, is(expectedFromFixedKeyProcessor));
        }
    }

    @ParameterizedTest
    @MethodSource
    public void shouldCapturePunctuator(final Processor<String, Long, Void, Void> processor,
                                        final FixedKeyProcessor<String, Long, Void> fixedKeyProcessor) {

        final MockProcessorContext<Void, Void> processorContext = new MockProcessorContext<>();
        final MockFixedKeyProcessorContext<String, Void> fixedKeyProcessorContext = new MockFixedKeyProcessorContext<>();

        processor.init(processorContext);
        fixedKeyProcessor.init(fixedKeyProcessorContext);

        final MockProcessorContext.CapturedPunctuator capturedProcessorPunctuator =
                processorContext.scheduledPunctuators().get(0);
        assertThat(capturedProcessorPunctuator.getInterval(), is(Duration.ofMillis(1000L)));
        assertThat(capturedProcessorPunctuator.getType(), is(PunctuationType.WALL_CLOCK_TIME));
        assertThat(capturedProcessorPunctuator.cancelled(), is(false));

        final MockFixedKeyProcessorContext.CapturedPunctuator capturedFixedKeyProcessorPunctuator =
                fixedKeyProcessorContext.scheduledPunctuators().get(0);
        assertThat(capturedProcessorPunctuator.getInterval(), is(Duration.ofMillis(1000L)));
        assertThat(capturedProcessorPunctuator.getType(), is(PunctuationType.WALL_CLOCK_TIME));
        assertThat(capturedProcessorPunctuator.cancelled(), is(false));

        final Punctuator punctuator = capturedProcessorPunctuator.getPunctuator();
        assertThat(processorContext.committed(), is(false));
        punctuator.punctuate(1234L);
        assertThat(processorContext.committed(), is(true));

        final Punctuator fixedKeyPunctuator = capturedFixedKeyProcessorPunctuator.getPunctuator();
        assertThat(fixedKeyProcessorContext.committed(), is(false));
        fixedKeyPunctuator.punctuate(1234L);
        assertThat(fixedKeyProcessorContext.committed(), is(true));
    }

    @Test
    public void fullConstructorShouldSetAllExpectedAttributes() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFullConstructor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        final File dummyFile = new File("");
        final MockProcessorContext<Void, Void> processorContext =
                new MockProcessorContext<>(config, new TaskId(1, 1), dummyFile);

        final MockFixedKeyProcessorContext<Void, Void> fixedKeyProcessorContext =
                new MockFixedKeyProcessorContext<>(config, new TaskId(1, 1), dummyFile);

        assertThat(processorContext.applicationId(), is("testFullConstructor"));
        assertThat(processorContext.taskId(), is(new TaskId(1, 1)));
        assertThat(processorContext.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG), is("testFullConstructor"));
        assertThat(processorContext.appConfigsWithPrefix("application.").get("id"), is("testFullConstructor"));
        assertThat(processorContext.keySerde().getClass(), is(Serdes.String().getClass()));
        assertThat(processorContext.valueSerde().getClass(), is(Serdes.Long().getClass()));
        assertThat(processorContext.stateDir(), is(dummyFile));

        assertThat(fixedKeyProcessorContext.applicationId(), is("testFullConstructor"));
        assertThat(fixedKeyProcessorContext.taskId(), is(new TaskId(1, 1)));
        assertThat(fixedKeyProcessorContext.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG), is("testFullConstructor"));
        assertThat(fixedKeyProcessorContext.appConfigsWithPrefix("application.").get("id"), is("testFullConstructor"));
        assertThat(fixedKeyProcessorContext.keySerde().getClass(), is(Serdes.String().getClass()));
        assertThat(fixedKeyProcessorContext.valueSerde().getClass(), is(Serdes.Long().getClass()));
        assertThat(fixedKeyProcessorContext.stateDir(), is(dummyFile));
    }
}
