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
package org.apache.kafka.streams.utils;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorWrapper;
import org.apache.kafka.streams.processor.api.WrappedFixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.WrappedProcessorSupplier;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.utils.TestUtils.RecordingProcessorWrapper.WrapperRecorder;

import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestUtils {

    public static final String PROCESSOR_WRAPPER_COUNTER_CONFIG = "wrapped.counter";

    /**
     * Waits for the given {@link KafkaStreams} instances to all be in a specific {@link KafkaStreams.State}.
     * This method uses polling, which can be more error prone and slightly slower.
     *
     * @param streamsList the list of streams instances to run.
     * @param state the expected state that all the streams to be in within timeout
     * @param timeout the time to wait for the streams to all be in the specific state.
     *
     * @throws InterruptedException if the streams doesn't change to the expected state in time.
     */
    public static void waitForApplicationState(final List<KafkaStreams> streamsList,
                                               final KafkaStreams.State state,
                                               final Duration timeout) throws InterruptedException {
        retryOnExceptionWithTimeout(timeout.toMillis(), () -> {
            final Map<KafkaStreams, KafkaStreams.State> streamsToStates = streamsList
                .stream()
                .collect(Collectors.toMap(stream -> stream, KafkaStreams::state));

            final Map<KafkaStreams, KafkaStreams.State> wrongStateMap = streamsToStates.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != state)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final String reason = String.format(
                "Expected all streams instances in %s to be %s within %d ms, but the following were not: %s",
                streamsList,
                state,
                timeout.toMillis(),
                wrongStateMap
            );
            assertThat(reason, wrongStateMap.isEmpty());
        });
    }

    public static String safeUniqueTestName(final TestInfo testInfo) {
        final String methodName = testInfo.getTestMethod().map(Method::getName).orElse("unknownMethodName");
        return safeUniqueTestName(methodName);
    }

    private static String safeUniqueTestName(final String testName) {
        return sanitize(testName + Uuid.randomUuid().toString());
    }

    private static String sanitize(final String str) {
        return str
            // The `-` is used in Streams' thread name as a separator and some tests rely on this.
            .replace('-', '_')
            .replace(':', '_')
            .replace('.', '_')
            .replace('[', '_')
            .replace(']', '_')
            .replace(' ', '_')
            .replace('=', '_');
    }

    /**
     * Quick method of generating a config map prepopulated with the required
     * StreamsConfig properties
     */
    public static Map<Object, Object> dummyStreamsConfigMap() {
        final Map<Object, Object> baseConfigs = new HashMap<>();
        baseConfigs.put(APPLICATION_ID_CONFIG, "dummy-app-id");
        baseConfigs.put(BOOTSTRAP_SERVERS_CONFIG, "local");
        return baseConfigs;
    }

    public static StoreFactory mockStoreFactory(final String name) {
        final StoreFactory storeFactory = Mockito.mock(StoreFactory.class);
        Mockito.when(storeFactory.storeName()).thenReturn(name);
        return storeFactory;
    }

    /**
     * Simple pass-through processor wrapper that counts the number of processors
     * it wraps.
     * To retrieve the current count, pass an instance of AtomicInteger into the configs
     * alongside the wrapper itself. Use the config key defined with {@link #PROCESSOR_WRAPPER_COUNTER_CONFIG}
     */
    public static class RecordingProcessorWrapper implements ProcessorWrapper {

        private WrapperRecorder recorder;

        @SuppressWarnings("unchecked")
        @Override
        public void configure(final Map<String, ?> configs) {
            if (configs.containsKey(PROCESSOR_WRAPPER_COUNTER_CONFIG)) {
                recorder = (WrapperRecorder) configs.get(PROCESSOR_WRAPPER_COUNTER_CONFIG);
            } else {
                recorder = new WrapperRecorder();
            }
        }

        public static class WrapperRecorder {
            private final Set<String> uniqueStores = new HashSet<>();
            private final Set<String> processorStoresCounted = new HashSet<>();
            private final Set<String> wrappedProcessorNames = Collections.synchronizedSet(new HashSet<>());

            public void wrapProcessorSupplier(final String name) {
                wrappedProcessorNames.add(name);
            }

            public void wrapStateStore(final String processorName, final String storeName) {
                if (!uniqueStores.contains(storeName)) {
                    uniqueStores.add(storeName);
                }

                final String processorStoreKey = processorName + storeName;
                if (!processorStoresCounted.contains(processorStoreKey)) {
                    processorStoresCounted.add(processorStoreKey);
                }
            }

            public int numWrappedProcessors() {
                return wrappedProcessorNames.size();
            }

            // Number of unique state stores in the topology connected to their processors via the
            // ProcessorSupplier#stores method. State stores connected to more than one processor are
            // counted only once
            public int numUniqueStateStores() {
                return uniqueStores.size();
            }

            // Number of stores connected to a processor via the ProcessorSupplier#stores method (ie the size
            // of the set returned by #stores), summed across all processors in the topology.
            // Equal to the number of unique <processorName>-<storeName>
            // pairings. Will be greater than or equal to the value of #numUniqueStateStores, as this method
            // will double count any stores connected to more than one processor
            public int numConnectedStateStores() {
                return processorStoresCounted.size();
            }

            public Set<String> wrappedProcessorNames() {
                return wrappedProcessorNames;
            }

        }

        @Override
        public <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName,
                                                                                                           final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier) {

            return new CountingDelegatingProcessorSupplier<>(recorder, processorName, processorSupplier);
        }

        @Override
        public <KIn, VIn, VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(final String processorName,
                                                                                                               final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier) {
            return new CountingDelegatingFixedKeyProcessorSupplier<>(recorder, processorName, processorSupplier);
        }
    }

    private static class CountingDelegatingProcessorSupplier<KIn, VIn, KOut, VOut>
        implements WrappedProcessorSupplier<KIn, VIn, KOut, VOut> {

        private final WrapperRecorder counter;
        private final String processorName;
        private final ProcessorSupplier<KIn, VIn, KOut, VOut> delegate;

        public CountingDelegatingProcessorSupplier(final WrapperRecorder counter,
                                                   final String processorName,
                                                   final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier) {
            this.counter = counter;
            this.processorName = processorName;
            this.delegate = processorSupplier;

            counter.wrapProcessorSupplier(processorName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            final Set<StoreBuilder<?>> stores = delegate.stores();
            if (stores != null) {
                for (final StoreBuilder<?> store : stores) {
                    counter.wrapStateStore(processorName, store.name());
                }
            }
            return stores;
        }

        @Override
        public Processor<KIn, VIn, KOut, VOut> get() {
            return delegate.get();
        }
    }

    private static class CountingDelegatingFixedKeyProcessorSupplier<KIn, VIn, VOut>
        implements WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> {

        private final WrapperRecorder counter;
        private final String processorName;
        private final FixedKeyProcessorSupplier<KIn, VIn, VOut> delegate;

        public CountingDelegatingFixedKeyProcessorSupplier(final WrapperRecorder counter,
                                                           final String processorName,
                                                           final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier) {
            this.counter = counter;
            this.processorName = processorName;
            this.delegate = processorSupplier;

            counter.wrapProcessorSupplier(processorName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            final Set<StoreBuilder<?>> stores = delegate.stores();
            if (stores != null) {
                for (final StoreBuilder<?> store : stores) {
                    counter.wrapStateStore(processorName, store.name());
                }
            }
            return stores;
        }

        @Override
        public FixedKeyProcessor<KIn, VIn, VOut> get() {
            return delegate.get();
        }
    }
}
