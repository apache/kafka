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

package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.internals.NoOpProcessorWrapper.WrappedFixedKeyProcessorSupplierImpl;
import org.apache.kafka.streams.processor.internals.NoOpProcessorWrapper.WrappedProcessorSupplierImpl;

import java.util.Map;

/**
 * Wrapper class that can be used to inject custom wrappers around the processors of their application topology.
 * The returned instance should wrap the supplied {@code ProcessorSupplier} and the {@code Processor} it supplies
 * to avoid disrupting the regular processing of the application, although this is not required and any processor
 * implementation can be substituted in to replace the original processor entirely (which may be useful for example
 * while testing or debugging an application topology).
 * <p>
 * NOTE: in order to use this feature, you must set the {@link StreamsConfig#PROCESSOR_WRAPPER_CLASS_CONFIG} config and pass it
 * in as a {@link TopologyConfig} when creating the {@link StreamsBuilder} or {@link Topology} by using the
 * appropriate constructor (ie {@link StreamsBuilder#StreamsBuilder(TopologyConfig)} or {@link Topology#Topology(TopologyConfig)})
 * <p>
 * Can be configured, if desired, by implementing the {@link #configure(Map)} method. This will be invoked when
 * the {@code ProcessorWrapper} is instantiated, and will provide it with the TopologyConfigs that were passed in
 * to the {@link StreamsBuilder} or {@link Topology} constructor.
 */
public interface ProcessorWrapper extends Configurable {

    @Override
    default void configure(final Map<String, ?> configs) {
        // do nothing
    }

    /**
     * Wrap or replace the provided {@link ProcessorSupplier} and return a {@link WrappedProcessorSupplier}
     * To convert a {@link ProcessorSupplier} instance into a {@link WrappedProcessorSupplier},
     * use the {@link ProcessorWrapper#asWrapped(ProcessorSupplier)} method
     */
    <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName,
                                                                                                final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier);

    /**
     * Wrap or replace the provided {@link FixedKeyProcessorSupplier} and return a {@link WrappedFixedKeyProcessorSupplier}
     * To convert a {@link FixedKeyProcessorSupplier} instance into a {@link WrappedFixedKeyProcessorSupplier},
     * use the {@link ProcessorWrapper#asWrappedFixedKey(FixedKeyProcessorSupplier)} method
     */
    <KIn, VIn,  VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn,  VOut> wrapFixedKeyProcessorSupplier(final String processorName,
                                                                                                      final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier);

    /**
     * Use to convert a {@link ProcessorSupplier} instance into a {@link WrappedProcessorSupplier}
     */
    static <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> asWrapped(
        final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier
    ) {
        return new WrappedProcessorSupplierImpl<>(processorSupplier);
    }

    /**
     * Use to convert a {@link FixedKeyProcessorSupplier} instance into a {@link WrappedFixedKeyProcessorSupplier}
     */
    static <KIn, VIn,  VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn,  VOut> asWrappedFixedKey(
        final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier
    ) {
        return new WrappedFixedKeyProcessorSupplierImpl<>(processorSupplier);
    }
}