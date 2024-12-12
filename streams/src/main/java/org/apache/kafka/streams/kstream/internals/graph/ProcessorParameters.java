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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

/**
 * Class used to represent a {@link ProcessorSupplier} or {@link FixedKeyProcessorSupplier} and the name
 * used to register it with the {@link org.apache.kafka.streams.processor.internals.InternalTopologyBuilder}
 *
 * Used by the Join nodes as there are several parameters, this abstraction helps
 * keep the number of arguments more reasonable.
 *
 * @see ProcessorSupplier
 * @see FixedKeyProcessorSupplier
 */
public class ProcessorParameters<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier;
    private final FixedKeyProcessorSupplier<KIn, VIn, VOut> fixedKeyProcessorSupplier;
    private final String processorName;

    public ProcessorParameters(final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier,
                               final String processorName) {
        this.processorSupplier = processorSupplier;
        fixedKeyProcessorSupplier = null;
        this.processorName = processorName;
    }

    public ProcessorParameters(final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier,
                               final String processorName) {
        this.processorSupplier = null;
        fixedKeyProcessorSupplier = processorSupplier;
        this.processorName = processorName;
    }

    public ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier() {
        return processorSupplier;
    }

    public FixedKeyProcessorSupplier<KIn, VIn, VOut> fixedKeyProcessorSupplier() {
        return fixedKeyProcessorSupplier;
    }

    public void addProcessorTo(final InternalTopologyBuilder topologyBuilder, final String... parentNodeNames) {
        if (processorSupplier != null) {
            ApiUtils.checkSupplier(processorSupplier);

            final ProcessorSupplier<KIn, VIn, KOut, VOut> wrapped =
                topologyBuilder.wrapProcessorSupplier(processorName, processorSupplier);

            topologyBuilder.addProcessor(processorName, wrapped, parentNodeNames);
            final Set<StoreBuilder<?>> stores = wrapped.stores();
            if (stores != null) {
                for (final StoreBuilder<?> storeBuilder : stores) {
                    topologyBuilder.addStateStore(storeBuilder, processorName);
                }
            }
        }

        if (fixedKeyProcessorSupplier != null) {
            ApiUtils.checkSupplier(fixedKeyProcessorSupplier);

            final FixedKeyProcessorSupplier<KIn, VIn, VOut> wrapped =
                topologyBuilder.wrapFixedKeyProcessorSupplier(processorName, fixedKeyProcessorSupplier);

            topologyBuilder.addProcessor(processorName, wrapped, parentNodeNames);
            final Set<StoreBuilder<?>> stores = wrapped.stores();
            if (stores != null) {
                for (final StoreBuilder<?> storeBuilder : stores) {
                    topologyBuilder.addStateStore(storeBuilder, processorName);
                }
            }
        }
    }

    public String processorName() {
        return processorName;
    }

    @Override
    public String toString() {
        return "ProcessorParameters{" +
            "processor supplier class=" + (processorSupplier != null ? processorSupplier.getClass() : "null") +
            ", fixed key processor supplier class=" + (fixedKeyProcessorSupplier != null ? fixedKeyProcessorSupplier.getClass() : "null") +
            ", processor name='" + processorName + '\'' +
            '}';
    }
}
